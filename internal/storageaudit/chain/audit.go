package chain

import (
	"context"
	"encoding/json"
	"fmt"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *Service) BuildStorageChainAuditWithOptions(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions) (*model.ChainAuditResponse, error) {
	normalized, err := normalizeStorageChainAuditOptions(options)
	if err != nil {
		return nil, err
	}
	if service.ChainAuditResponseCache != nil && storageChainAuditResponseCacheAllowed(gitSubpath, normalized) {
		return service.buildStorageChainAuditWithResponseCache(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
	}
	if service.ChainAuditResponseCache != nil && storageChainAuditRootResponseProjectionAllowed(gitSubpath, normalized) {
		response, ok, err := service.projectStorageChainAuditFromRootResponseCache(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
		if err != nil {
			return nil, err
		}
		if ok {
			return response, nil
		}
	}
	if service.ChainAuditResponseCache != nil && normalized.Timings != nil {
		normalized.Timings.Record("audit_response_cache_bypass_non_root", 0)
	}
	return service.buildStorageChainAuditFresh(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
}

func storageChainAuditResponseCacheAllowed(gitSubpath string, options model.StorageChainAuditOptions) bool {
	return sarecord.NormalizeRepoSubpath(gitSubpath) == "" && sarecord.NormalizeRepoSubpath(options.BucketPathPrefix) == ""
}

func storageChainAuditRootResponseProjectionAllowed(gitSubpath string, options model.StorageChainAuditOptions) bool {
	return sarecord.NormalizeRepoSubpath(gitSubpath) != "" && sarecord.NormalizeRepoSubpath(options.BucketPathPrefix) == "" && !options.ForceAuditRefresh
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
	syfonRevision, err := service.store.LoadStorageAuditSyfonRevision(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	cacheKey := storecache.StorageChainAuditResponseCacheKey(organization, project, ref, gitSubpath, options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, options.BucketPathPrefix, hash.String(), syfonRevision)
	cache := service.ChainAuditResponseCache
	if !options.ForceAuditRefresh {
		start := time.Now()
		cached, ok, err := cache.Get(ctx, cacheKey)
		options.Timings.Record("audit_response_cache_lookup", time.Since(start))
		if err != nil {
			logStorageChainAuditCacheError(options.Timings, cache.Source(), "get", err)
		}
		if ok {
			response := projectStorageChainAuditResponse(cached.Response, options.FindingKind, options.FindingLimit)
			applyStorageChainAuditCacheMetadata(response, true, cached.CachedAt, cached.RefreshDurationMillis, cache.Source(), "")
			options.Timings.Record("audit_response_cache_hit", 0)
			options.Timings.RecordMemory(
				"audit_response_cache_hit",
				"total_findings", response.Summary.TotalFindings,
				"returned_findings", response.Summary.ReturnedFindings,
				"finding_limit", options.FindingLimit,
			)
			return response, nil
		}
		options.Timings.Record("audit_response_cache_miss", 0)
	} else {
		options.Timings.Record("audit_response_cache_force_refresh", 0)
	}

	cached, joinedRefresh, err := service.coalesceStorageChainAuditRefresh(ctx, cacheKey, options.ForceBucketRefresh, func() (storecache.CachedResponse, error) {
		buildOptions := options
		buildOptions.FindingKind = ""
		buildOptions.FindingLimit = -1
		refreshStart := time.Now()
		response, err := service.buildStorageChainAuditFresh(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, buildOptions)
		if err != nil {
			return storecache.CachedResponse{}, err
		}
		response.Summary.SyfonRevision = syfonRevision
		value := storecache.CachedResponse{
			CachedAt:              time.Now(),
			RefreshDurationMillis: time.Since(refreshStart).Milliseconds(),
			Response:              *response,
		}
		cacheStart := time.Now()
		if err := cache.Set(ctx, cacheKey, value, storecache.StorageChainAuditCacheTTL()); err != nil {
			logStorageChainAuditCacheError(options.Timings, cache.Source(), "set", err)
		}
		options.Timings.Record("audit_response_cache_store", time.Since(cacheStart))
		return value, nil
	})
	if err != nil {
		return nil, err
	}
	if joinedRefresh {
		options.Timings.Record("audit_response_refresh_join", 0)
	}
	projected := projectStorageChainAuditResponse(cached.Response, options.FindingKind, options.FindingLimit)
	source := cache.Source()
	if joinedRefresh {
		source += ":refresh_join"
	}
	applyStorageChainAuditCacheMetadata(projected, false, cached.CachedAt, cached.RefreshDurationMillis, source, "")
	return projected, nil
}

func (service *Service) projectStorageChainAuditFromRootResponseCache(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions) (*model.ChainAuditResponse, bool, error) {
	cache := service.ChainAuditResponseCache
	syfonRevision, err := service.store.LoadStorageAuditSyfonRevision(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, false, err
	}
	cacheKey := storecache.StorageChainAuditResponseCacheKey(organization, project, ref, "", options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, "", hash.String(), syfonRevision)
	start := time.Now()
	cached, ok, err := cache.Get(ctx, cacheKey)
	options.Timings.Record("audit_response_root_cache_lookup", time.Since(start))
	if err != nil {
		logStorageChainAuditCacheError(options.Timings, cache.Source(), "get_root", err)
		return nil, false, nil
	}
	if !ok {
		options.Timings.Record("audit_response_root_cache_miss", 0)
		return nil, false, nil
	}
	inventoryStart := time.Now()
	inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
	options.Timings.Record("audit_response_root_cache_project_inventory", time.Since(inventoryStart))
	if err != nil {
		return nil, false, err
	}
	response := projectStorageChainAuditResponseForSubpath(cached.Response, gitSubpath, inventory, options.FindingKind, options.FindingLimit)
	applyStorageChainAuditCacheMetadata(response, true, cached.CachedAt, cached.RefreshDurationMillis, cache.Source()+":root", "")
	options.Timings.Record("audit_response_root_cache_hit", 0)
	options.Timings.RecordMemory(
		"audit_response_root_cache_hit",
		"git_subpath", sarecord.NormalizeRepoSubpath(gitSubpath),
		"git_files", response.Summary.GitTrackedFileCount,
		"total_findings", response.Summary.TotalFindings,
		"returned_findings", response.Summary.ReturnedFindings,
	)
	return response, true, nil
}

func (service *Service) buildStorageChainAuditFresh(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options model.StorageChainAuditOptions) (*model.ChainAuditResponse, error) {
	bucketMode := options.BucketInventoryMode
	validationMode := options.ValidationMode
	bucketPathPrefix := options.BucketPathPrefix
	start := time.Now()
	options.Timings.StageStart("chain_setup_total")
	inputs, err := service.loadStorageChainInputs(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, bucketMode, validationMode, bucketPathPrefix, options.ForceBucketRefresh, options.Timings)
	options.Timings.Record("chain_setup_total", time.Since(start))
	if inputs != nil && inputs.recordSet != nil {
		options.Timings.RecordMemory(
			"chain_setup_total",
			"git_files", len(inputs.inventory),
			"syfon_records", countRecordStates(inputs.recordSet.AllProjectRecords),
			"bucket_objects", len(inputs.BucketObjects),
		)
	}
	if err != nil {
		return nil, err
	}
	storageViewStart := time.Now()
	options.Timings.StageStart("storage_view")
	storageView, err := service.buildStorageChainView(ctx, authorizationHeader, organization, project, inputs.recordSet, inputs.inventory, inputs.scopes, inputs.BucketObjects, inputs.BucketObjectsByURL, inputs.bucketInventoryErr, inputs.BucketMetadata, bucketMode, validationMode, options.Timings)
	options.Timings.Record("storage_view", time.Since(storageViewStart))
	if storageView != nil {
		options.Timings.RecordMemory(
			"storage_view",
			"syfon_records", countRecordStates(storageView.AllProjectRecords),
			"bucket_objects", len(storageView.BucketObjectsByURL),
		)
	}
	if err != nil {
		return nil, err
	}
	modelStart := time.Now()
	options.Timings.StageStart("model_build")
	includeBucketOrigin := storageView.BucketInventoryAvailable &&
		!storageView.BucketInventoryStale &&
		(bucketMode == model.StorageChainBucketModeItems || validationMode == model.StorageChainValidationModeList)
	auditModel := buildStorageChainAuditModel(gitSubpath, inputs.inventory, storageView.RecordsByChecksum, storageView.AllProjectRecords, storageView.BucketObjectsByURL, inputs.scopes, organization, project, includeBucketOrigin)
	options.Timings.Record("model_build", time.Since(modelStart))
	options.Timings.RecordMemory(
		"model_build",
		"total_findings", len(auditModel.Findings),
		"syfon_records", countRecordStates(storageView.AllProjectRecords),
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
	}, nil
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

func (service *Service) coalesceStorageChainAuditRefresh(ctx context.Context, key string, forceBucketRefresh bool, refresh func() (storecache.CachedResponse, error)) (storecache.CachedResponse, bool, error) {
	var work *InflightResponseRefresh
	for {
		service.ChainAuditRefreshMu.Lock()
		work = service.ChainAuditRefreshWork[key]
		if work == nil {
			work = &InflightResponseRefresh{done: make(chan struct{}), forceBucketRefresh: forceBucketRefresh}
			service.ChainAuditRefreshWork[key] = work
			service.ChainAuditRefreshMu.Unlock()
			break
		}
		service.ChainAuditRefreshMu.Unlock()
		select {
		case <-ctx.Done():
			return storecache.CachedResponse{}, true, ctx.Err()
		case <-work.done:
			if !forceBucketRefresh || work.forceBucketRefresh {
				return storecache.CloneCachedResponse(work.value), true, work.err
			}
		}
	}

	value, err := refresh()
	service.ChainAuditRefreshMu.Lock()
	work.value = storecache.CloneCachedResponse(value)
	work.err = err
	delete(service.ChainAuditRefreshWork, key)
	close(work.done)
	service.ChainAuditRefreshMu.Unlock()
	return value, false, err
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
