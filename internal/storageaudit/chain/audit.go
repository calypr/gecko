package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/sharedwork"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *Service) BuildStorageChainAuditWithOptions(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions) (*model.ChainAuditResponse, error) {
	normalized, err := normalizeStorageChainAuditOptions(options)
	if err != nil {
		return nil, err
	}
	normalized.ForceAuditRefresh = normalized.ForceAuditRefresh || normalized.ForceBucketRefresh || normalized.ForceStorageRevalidation
	normalized.ForceBucketRefresh = normalized.ForceBucketRefresh || normalized.ForceStorageRevalidation
	if service.ChainAuditResponseCache != nil {
		return service.buildStorageChainAuditWithResponseCache(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
	}
	response, _, err := service.buildStorageChainAuditFresh(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized, nil)
	if err != nil {
		return nil, err
	}
	if normalized.ForceAuditRefresh {
		if err := storageChainAuditSnapshotFailure(*response, normalized); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func normalizeStorageChainAuditOptions(options model.StorageChainAuditOptions) (model.StorageChainAuditOptions, error) {
	rawProbeMode := strings.TrimSpace(options.ProbeMode)
	probeMode, ok := model.NormalizeStorageChainProbeMode(options.ProbeMode)
	if !ok {
		return model.StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain probe mode %q", options.ProbeMode)
	}
	bucketMode, ok := model.NormalizeStorageChainBucketInventoryMode(options.BucketInventoryMode)
	if !ok {
		return model.StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain bucket inventory mode %q", options.BucketInventoryMode)
	}
	validationMode := strings.TrimSpace(options.ValidationMode)
	if validationMode == "" {
		if rawProbeMode == "" && bucketMode == model.StorageChainBucketModeValidate {
			validationMode = model.StorageChainValidationModeList
		} else {
			validationMode = model.DefaultStorageChainValidationMode(probeMode, bucketMode)
		}
	}
	validationMode, ok = model.NormalizeStorageChainValidationMode(validationMode)
	if !ok {
		return model.StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain validation mode %q", options.ValidationMode)
	}
	options.ProbeMode = probeMode
	options.ValidationMode = validationMode
	options.BucketInventoryMode = bucketMode
	options.BucketPathPrefix = sarecord.NormalizeRepoSubpath(options.BucketPathPrefix)
	options.FindingKind = strings.TrimSpace(options.FindingKind)
	return options, nil
}

func (service *Service) buildStorageChainAuditWithResponseCache(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions) (*model.ChainAuditResponse, error) {
	cache := service.ChainAuditResponseCache
	cacheKey := storageChainAuditResponseCacheKey(organization, project, ref, gitSubpath, options)
	if !options.ForceAuditRefresh {
		start := time.Now()
		exact, hasExact, err := cache.Get(ctx, cacheKey)
		options.Timings.Record("audit_response_cache_lookup", time.Since(start))
		if err != nil {
			logStorageChainAuditCacheError(options.Timings, cache.Source(), "get", err)
			return nil, fmt.Errorf("load storage chain audit snapshot from %s cache: %w", cache.Source(), err)
		}
		if storageChainAuditRootResponseProjectionAllowed(gitSubpath, options) {
			exactCachedAt := time.Time{}
			if hasExact {
				exactCachedAt = exact.CachedAt
			}
			projectedResponse, projected, err := service.projectStorageChainAuditFromRootResponseCache(ctx, organization, project, ref, gitSubpath, exactCachedAt, options)
			if err != nil {
				return nil, err
			}
			if projected {
				return cachedStorageChainAuditResponse(projectedResponse, true, cache.Source()+":root", options), nil
			}
		}
		if hasExact {
			return cachedStorageChainAuditResponse(exact, true, cache.Source(), options), nil
		}
		options.Timings.Record("audit_response_cache_miss", 0)
	} else {
		options.Timings.Record("audit_response_cache_force_refresh", 0)
	}

	workOptions := options
	if options.Timings != nil {
		workOptions.Timings = &model.StorageChainAuditTimings{Logf: options.Timings.Logf, DebugPrefix: options.Timings.DebugPrefix}
	}
	refreshResult, joinedRefresh, err := service.coalesceStorageChainAuditRefresh(ctx, organization, project, authorizationHeader, cacheKey, options.ForceAuditRefresh, options.ForceBucketRefresh, options.ForceStorageRevalidation, func(workCtx context.Context) (result StorageChainAuditRefreshResult, retErr error) {
		if service.ProjectAuditCoordinator == nil {
			return StorageChainAuditRefreshResult{}, fmt.Errorf("storage chain audit project coordinator is required")
		}
		lease, err := service.ProjectAuditCoordinator.AcquireProject(workCtx, organization, project)
		if err != nil {
			return StorageChainAuditRefreshResult{}, fmt.Errorf("acquire storage chain audit project lease: %w", err)
		}
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), 5*time.Second)
			defer cancel()
			if err := lease.Release(releaseCtx); err != nil {
				log.Printf("storage_chain_audit_project_lease_release_error org=%s project=%s error=%q", organization, project, err.Error())
				if retErr == nil {
					retErr = fmt.Errorf("release storage chain audit project lease: %w", err)
				}
			}
		}()
		workCtx = lease.Context()
		if !workOptions.ForceAuditRefresh {
			cached, ok, err := cache.Get(workCtx, cacheKey)
			if err != nil {
				logStorageChainAuditCacheError(workOptions.Timings, cache.Source(), "get_after_wait", err)
				return StorageChainAuditRefreshResult{}, fmt.Errorf("load storage chain audit snapshot from %s cache: %w", cache.Source(), err)
			}
			if ok {
				return StorageChainAuditRefreshResult{Response: cached, CacheHit: true}, nil
			}
			if storageChainAuditRootResponseProjectionAllowed(gitSubpath, workOptions) {
				rootOptions := workOptions
				rootOptions.FindingKind = ""
				rootOptions.FindingLimit = -1
				projected, ok, err := service.projectStorageChainAuditFromRootResponseCache(workCtx, organization, project, ref, gitSubpath, time.Time{}, rootOptions)
				if err != nil {
					return StorageChainAuditRefreshResult{}, err
				}
				if ok {
					return StorageChainAuditRefreshResult{Response: projected, CacheHit: true, CacheSource: cache.Source() + ":root"}, nil
				}
			}
		}
		buildOptions := workOptions
		buildOptions.FindingKind = ""
		buildOptions.FindingLimit = -1
		refreshStart := time.Now()
		response, inventory, err := service.buildStorageChainAuditFresh(workCtx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, buildOptions, lease)
		if err != nil {
			return StorageChainAuditRefreshResult{}, err
		}
		if buildOptions.ForceAuditRefresh {
			if err := storageChainAuditSnapshotFailure(*response, buildOptions); err != nil {
				return StorageChainAuditRefreshResult{}, err
			}
		}
		value := storecache.CachedResponse{
			CachedAt:              time.Now(),
			RefreshDurationMillis: time.Since(refreshStart).Milliseconds(),
			Response:              *response,
			Inventory:             append([]model.RepoInventoryFile(nil), inventory...),
		}
		if err := storageChainAuditSnapshotFailure(*response, buildOptions); err == nil {
			cacheStart := time.Now()
			if err := lease.PublishResponse(workCtx, cacheKey, value); err != nil {
				logStorageChainAuditCacheError(buildOptions.Timings, cache.Source(), "set", err)
				return StorageChainAuditRefreshResult{}, fmt.Errorf("save storage chain audit snapshot: %w", err)
			}
			buildOptions.Timings.Record("audit_response_cache_store", time.Since(cacheStart))
		}
		return StorageChainAuditRefreshResult{Response: value}, nil
	})
	if err != nil {
		return nil, err
	}
	if joinedRefresh {
		options.Timings.Record("audit_response_refresh_join", 0)
	}
	source := refreshResult.CacheSource
	if source == "" {
		source = cache.Source()
	}
	if joinedRefresh {
		source += ":refresh_join"
	}
	return cachedStorageChainAuditResponse(refreshResult.Response, refreshResult.CacheHit, source, options), nil
}

func storageChainAuditResponseCacheKey(organization string, project string, ref string, gitSubpath string, options model.StorageChainAuditOptions) string {
	return storecache.StorageChainAuditResponseCacheKey(organization, project, ref, gitSubpath, options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, options.BucketPathPrefix)
}

func storageChainAuditRootResponseProjectionAllowed(gitSubpath string, options model.StorageChainAuditOptions) bool {
	return sarecord.NormalizeRepoSubpath(gitSubpath) != "" && sarecord.NormalizeRepoSubpath(options.BucketPathPrefix) == ""
}

func storageChainAuditRequiresBucketInventory(options model.StorageChainAuditOptions) bool {
	if options.BucketInventoryMode == model.StorageChainBucketModeItems {
		return true
	}
	return options.BucketInventoryMode == model.StorageChainBucketModeValidate && options.ValidationMode == model.StorageChainValidationModeList
}

func storageChainAuditSnapshotFailure(response model.ChainAuditResponse, options model.StorageChainAuditOptions) error {
	if (storageChainAuditRequiresBucketInventory(options) || options.ForceBucketRefresh) && (!response.Summary.BucketInventoryAvailable || !response.Summary.BucketInventoryComplete) {
		message := strings.TrimSpace(response.Summary.BucketInventoryWarning)
		if message == "" {
			message = strings.TrimSpace(response.Summary.BucketInventoryError)
		}
		if message == "" {
			message = "Syfon bucket inventory is incomplete"
		}
		return fmt.Errorf("storage chain audit refresh has incomplete bucket inventory: %s", message)
	}
	if count := response.Summary.CountsByKind["probe_error"]; count > 0 {
		return fmt.Errorf("storage chain audit refresh contains %d unverified probe results", count)
	}
	return nil
}

func cachedStorageChainAuditResponse(cached storecache.CachedResponse, hit bool, source string, options model.StorageChainAuditOptions) *model.ChainAuditResponse {
	response := projectStorageChainAuditResponse(cached.Response, options.FindingKind, options.FindingLimit)
	applyStorageChainAuditCacheMetadata(response, hit, cached.CachedAt, cached.RefreshDurationMillis, source, "")
	if hit {
		options.Timings.Record("audit_response_cache_hit", 0)
		options.Timings.RecordMemory(
			"audit_response_cache_hit",
			"total_findings", response.Summary.TotalFindings,
			"returned_findings", response.Summary.ReturnedFindings,
			"finding_limit", options.FindingLimit,
		)
	}
	return response
}

func (service *Service) projectStorageChainAuditFromRootResponseCache(ctx context.Context, organization string, project string, ref string, gitSubpath string, exactCachedAt time.Time, options model.StorageChainAuditOptions) (storecache.CachedResponse, bool, error) {
	cache := service.ChainAuditResponseCache
	cacheKey := storageChainAuditResponseCacheKey(organization, project, ref, "", options)
	start := time.Now()
	cached, ok, err := cache.Get(ctx, cacheKey)
	options.Timings.Record("audit_response_root_cache_lookup", time.Since(start))
	if err != nil {
		logStorageChainAuditCacheError(options.Timings, cache.Source(), "get_root", err)
		return storecache.CachedResponse{}, false, fmt.Errorf("load root storage chain audit snapshot from %s cache: %w", cache.Source(), err)
	}
	if !ok {
		options.Timings.Record("audit_response_root_cache_miss", 0)
		return storecache.CachedResponse{}, false, nil
	}
	if !cached.CachedAt.After(exactCachedAt) {
		return storecache.CachedResponse{}, false, nil
	}
	inventory := filterStorageChainInventory(cached.Inventory, gitSubpath)
	response := storageChainAuditResponseForSubpathView(cached.Response, gitSubpath, inventory)
	options.Timings.Record("audit_response_root_cache_hit", 0)
	options.Timings.RecordMemory(
		"audit_response_root_cache_hit",
		"git_subpath", sarecord.NormalizeRepoSubpath(gitSubpath),
		"git_files", response.Summary.GitTrackedFileCount,
		"total_findings", response.Summary.TotalFindings,
		"returned_findings", response.Summary.ReturnedFindings,
	)
	return storecache.CachedResponse{
		CachedAt:              cached.CachedAt,
		RefreshDurationMillis: cached.RefreshDurationMillis,
		Response:              response,
		Inventory:             inventory,
	}, true, nil
}

func (service *Service) buildStorageChainAuditFresh(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions, lease storecache.ProjectAuditLease) (*model.ChainAuditResponse, []model.RepoInventoryFile, error) {
	bucketMode := options.BucketInventoryMode
	validationMode := options.ValidationMode
	bucketPathPrefix := options.BucketPathPrefix
	start := time.Now()
	options.Timings.StageStart("chain_setup_total")
	inputs, err := service.loadStorageChainInputs(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, bucketMode, validationMode, bucketPathPrefix, options.ForceAuditRefresh, options.ForceBucketRefresh, options.Timings, lease)
	options.Timings.Record("chain_setup_total", time.Since(start))
	if inputs != nil && inputs.recordSet != nil {
		options.Timings.RecordMemory(
			"chain_setup_total",
			"git_files", len(inputs.inventory),
			"syfon_records", sarecord.CountStates(inputs.recordSet.AllProjectRecords),
			"bucket_objects", len(inputs.BucketObjects),
		)
	}
	if err != nil {
		return nil, nil, err
	}
	storageViewStart := time.Now()
	options.Timings.StageStart("storage_view")
	storageView, err := service.buildStorageChainView(ctx, authorizationHeader, organization, project, inputs.recordSet, inputs.inventory, inputs.scopes, inputs.BucketObjects, inputs.BucketObjectsByURL, inputs.bucketInventoryErr, inputs.BucketMetadata, bucketMode, validationMode, bucketPathPrefix, options.ForceStorageRevalidation, options.Timings)
	options.Timings.Record("storage_view", time.Since(storageViewStart))
	if storageView != nil {
		options.Timings.RecordMemory(
			"storage_view",
			"syfon_records", sarecord.CountStates(storageView.AllProjectRecords),
			"bucket_objects", len(storageView.BucketObjectsByURL),
		)
	}
	if err != nil {
		return nil, nil, err
	}
	modelStart := time.Now()
	options.Timings.StageStart("model_build")
	usableBucketInventory := storageView.BucketInventoryAvailable &&
		storageView.BucketInventoryComplete &&
		!storageView.BucketInventoryStale
	includeBucketOrigin := usableBucketInventory &&
		(bucketMode == model.StorageChainBucketModeItems || validationMode == model.StorageChainValidationModeList)
	countCompleteFromSyfon := !storageChainAuditRequiresBucketInventory(options)
	auditModel := buildStorageChainAuditModel(gitSubpath, inputs.inventory, storageView.RecordsByChecksum, storageView.AllProjectRecords, storageView.BucketObjectsByURL, inputs.scopes, organization, project, includeBucketOrigin, countCompleteFromSyfon)
	options.Timings.Record("model_build", time.Since(modelStart))
	options.Timings.RecordMemory(
		"model_build",
		"total_findings", len(auditModel.Findings),
		"syfon_records", sarecord.CountStates(storageView.AllProjectRecords),
		"bucket_objects", len(storageView.BucketObjectsByURL),
		"git_files", len(inputs.inventory),
	)
	auditModel.Summary.BucketInventoryAvailable = storageView.BucketInventoryAvailable
	auditModel.Summary.BucketInventoryComplete = storageView.BucketInventoryComplete
	auditModel.Summary.BucketInventoryError = storageView.BucketInventoryError
	auditModel.Summary.BucketInventoryWarning = storageView.BucketInventoryWarning
	auditModel.Summary.BucketInventoryObserved = storageView.BucketInventoryObserved
	auditModel.Summary.BucketInventorySource = storageView.BucketInventorySource
	auditModel.Summary.ValidationMode = validationMode
	auditModel.Summary.GitRevision = hash.String()
	auditModel.Summary.SyfonRevision = inputs.syfonRevision
	auditModel.Summary.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if inputs.BucketSummary != nil {
		exists := inputs.BucketSummary.Exists
		auditModel.Summary.BucketPathExists = &exists
		auditModel.Summary.BucketPathObjectURL = strings.TrimSpace(inputs.BucketSummary.ObjectURL)
		auditModel.Summary.BucketSummaryMode = strings.TrimSpace(inputs.BucketSummary.Mode)
	}
	filteredFindings := filterStorageChainFindingsByKind(auditModel.Findings, options.FindingKind)
	summary := filterStorageChainSummary(auditModel.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, options.FindingLimit, options.FindingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = options.FindingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	options.Timings.RecordMemory(
		"response_shape",
		"total_findings", summary.TotalFindings,
		"filtered_findings", len(filteredFindings),
		"returned_findings", len(findings),
		"finding_limit", options.FindingLimit,
	)
	return &model.ChainAuditResponse{
		Findings:         findings,
		Groups:           summarizeChainIssueGroups(filteredFindings),
		Summary:          summary,
		PathPrefix:       auditModel.PathPrefix,
		BucketPathPrefix: bucketPathPrefix,
	}, append([]model.RepoInventoryFile(nil), inputs.inventory...), nil
}

func logStorageChainAuditCacheError(timings *model.StorageChainAuditTimings, source string, operation string, err error) {
	if timings == nil || timings.Logf == nil || err == nil {
		return
	}
	timings.Logf("storage_chain_audit_cache_error %s source=%s operation=%s error=%q", strings.TrimSpace(timings.DebugPrefix), strings.TrimSpace(source), strings.TrimSpace(operation), err.Error())
}

func applyStorageChainAuditCacheMetadata(response *model.ChainAuditResponse, hit bool, cachedAt time.Time, refreshDurationMillis int64, source string, cacheError string) {
	if response == nil || cachedAt.IsZero() {
		return
	}
	response.Summary.AuditCacheHit = hit
	response.Summary.AuditCachedAt = cachedAt.UTC().Format(time.RFC3339Nano)
	response.Summary.AuditCacheAgeSeconds = int64(time.Since(cachedAt).Seconds())
	response.Summary.AuditRefreshDurationMs = refreshDurationMillis
	response.Summary.AuditCacheSource = strings.TrimSpace(source)
	response.Summary.AuditCacheError = strings.TrimSpace(cacheError)
}

func (service *Service) coalesceStorageChainAuditRefresh(ctx context.Context, organization string, project string, authorizationHeader string, key string, forceRefresh bool, forceBucketRefresh bool, forceStorageRevalidation bool, refresh func(context.Context) (StorageChainAuditRefreshResult, error)) (StorageChainAuditRefreshResult, bool, error) {
	return service.auditWork.Do(ctx, projectChainWorkKey(organization, project), sharedwork.Demand{
		AuthorizationScope: sharedwork.AuthorizationScope(authorizationHeader),
		SemanticKey:        key,
		ForceAudit:         forceRefresh,
		ForceBuckets:       forceBucketRefresh,
		ForceStorage:       forceStorageRevalidation,
	}, refresh)
}

func projectChainWorkKey(organization string, project string) string {
	return strings.TrimSpace(organization) + "\x00" + strings.TrimSpace(project)
}

func cloneStorageChainAuditResponse(in model.ChainAuditResponse) model.ChainAuditResponse {
	data, err := json.Marshal(in)
	if err != nil {
		return in
	}
	var out model.ChainAuditResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return in
	}
	return out
}
