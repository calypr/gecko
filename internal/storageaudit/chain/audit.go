package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
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

func projectStorageChainAuditResponseForSubpath(base model.ChainAuditResponse, gitSubpath string, inventory []model.RepoInventoryFile, findingKind string, findingLimit int) *model.ChainAuditResponse {
	pathPrefix := sarecord.NormalizeRepoSubpath(gitSubpath)
	filteredFindings := make([]model.ChainFinding, 0, len(base.Findings))
	for _, finding := range base.Findings {
		if storageChainFindingMatchesSubpath(finding, pathPrefix) {
			filteredFindings = append(filteredFindings, finding)
		}
	}

	summary := base.Summary
	countsByKind := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		countsByKind[kind] = 0
	}
	issueGitPaths := make(map[string]struct{})
	objectIDs := make(map[string]struct{})
	bucketObjects := make(map[string]struct{})
	for _, finding := range filteredFindings {
		countsByKind[finding.Kind]++
		if storageChainFindingHasGitPath(finding.Kind) {
			for _, sourcePath := range finding.SourcePaths {
				if storagePathMatchesRepoSubpath(sourcePath, pathPrefix) && !strings.Contains(sourcePath, "://") {
					issueGitPaths[sarecord.NormalizeRepoSubpath(sourcePath)] = struct{}{}
				}
			}
			if len(finding.SourcePaths) == 0 && storagePathMatchesRepoSubpath(finding.NormalizedPath, pathPrefix) && !strings.Contains(finding.NormalizedPath, "://") {
				issueGitPaths[sarecord.NormalizeRepoSubpath(finding.NormalizedPath)] = struct{}{}
			}
		}
		for _, objectID := range finding.ObjectIDs {
			if trimmed := strings.TrimSpace(objectID); trimmed != "" {
				objectIDs[trimmed] = struct{}{}
			}
		}
		if trimmed := strings.TrimSpace(finding.BucketObjectURL); trimmed != "" {
			bucketObjects[trimmed] = struct{}{}
		}
		for _, accessURL := range finding.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				bucketObjects[trimmed] = struct{}{}
			}
		}
	}
	gitTrackedFileCount := len(inventory)
	completeCount := gitTrackedFileCount - len(issueGitPaths)
	if completeCount < 0 {
		completeCount = 0
	}
	countsByKind["bucket_syfon_git_complete"] = completeCount
	summary.CountsByKind = countsByKind
	summary.TotalFindings = len(filteredFindings)
	summary.GitTrackedFileCount = gitTrackedFileCount
	summary.SyfonRecordCount = completeCount + len(objectIDs)
	summary.BucketObjectCount = completeCount + len(bucketObjects)

	projected := base
	projected.Findings = filteredFindings
	projected.Groups = summarizeChainIssueGroups(filteredFindings)
	projected.Summary = summary
	projected.PathPrefix = pathPrefix
	projected.BucketPathPrefix = ""
	return projectStorageChainAuditResponse(projected, findingKind, findingLimit)
}

func storageChainFindingHasGitPath(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "git_only_no_syfon", "syfon_git_no_bucket", "git_syfon_metadata_mismatch", "probe_error":
		return true
	}
	return false
}

func projectStorageChainAuditResponse(base model.ChainAuditResponse, findingKind string, findingLimit int) *model.ChainAuditResponse {
	response := cloneStorageChainAuditResponse(base)
	filteredFindings := filterStorageChainFindingsByKind(response.Findings, findingKind)
	summary := filterStorageChainSummary(response.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, findingLimit, findingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = findingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	response.Findings = findings
	response.Groups = summarizeChainIssueGroups(filteredFindings)
	response.Summary = summary
	return &response
}

func storageChainFindingMatchesSubpath(finding model.ChainFinding, pathPrefix string) bool {
	if pathPrefix == "" {
		return true
	}
	candidates := []string{finding.NormalizedPath, finding.BucketObjectURL, finding.ResolvedKey}
	candidates = append(candidates, finding.SourcePaths...)
	candidates = append(candidates, finding.AccessURLs...)
	for _, record := range finding.Records {
		candidates = append(candidates, record.NormalizedPath)
		candidates = append(candidates, record.AccessURLs...)
		for _, accessMethod := range record.AccessMethods {
			candidates = append(candidates, accessMethod.URL)
		}
		for _, probe := range record.AccessProbes {
			candidates = append(candidates, probe.URL, probe.Path, probe.Key)
		}
	}
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.SourcePaths...)
		candidates = append(candidates, finding.Evidence.AccessURLs...)
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.Keys...)
	}
	for _, candidate := range candidates {
		if storagePathMatchesRepoSubpath(candidate, pathPrefix) {
			return true
		}
	}
	return false
}

func storagePathMatchesRepoSubpath(raw string, pathPrefix string) bool {
	pathPrefix = sarecord.NormalizeRepoSubpath(pathPrefix)
	if pathPrefix == "" {
		return true
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return false
	}
	if _, key, ok := sarecord.ParseStorageURL(value); ok {
		key = sarecord.NormalizeRepoSubpath(key)
		return key == pathPrefix || strings.HasPrefix(key, pathPrefix+"/") || strings.Contains("/"+key, "/"+pathPrefix+"/")
	}
	normalized := sarecord.NormalizeRepoSubpath(value)
	return normalized == pathPrefix || strings.HasPrefix(normalized, pathPrefix+"/") || strings.Contains("/"+normalized, "/"+pathPrefix+"/")
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

func filterStorageChainFindingsByKind(findings []model.ChainFinding, kind string) []model.ChainFinding {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return append([]model.ChainFinding(nil), findings...)
	}
	filtered := make([]model.ChainFinding, 0, len(findings))
	for _, finding := range findings {
		if finding.Kind == kind {
			filtered = append(filtered, finding)
		}
	}
	return filtered
}

func filterStorageChainSummary(summary model.ChainSummary, findings []model.ChainFinding) model.ChainSummary {
	filteredCounts := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		filteredCounts[kind] = 0
	}
	filteredCounts["bucket_syfon_git_complete"] = summary.CountsByKind["bucket_syfon_git_complete"]
	for _, finding := range findings {
		filteredCounts[finding.Kind]++
	}
	summary.CountsByKind = filteredCounts
	summary.TotalFindings = len(findings)
	return summary
}

func limitStorageChainFindings(findings []model.ChainFinding, limit int, kind string) []model.ChainFinding {
	if limit <= 0 || limit >= len(findings) {
		return append([]model.ChainFinding(nil), findings...)
	}
	if strings.TrimSpace(kind) == "" {
		limited := make([]model.ChainFinding, 0, len(findings))
		countsByKind := make(map[string]int)
		for _, finding := range findings {
			if countsByKind[finding.Kind] >= limit {
				continue
			}
			limited = append(limited, finding)
			countsByKind[finding.Kind]++
		}
		return limited
	}
	return append([]model.ChainFinding(nil), findings[:limit]...)
}

func (service *Service) loadCachedProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	start := time.Now()
	cacheKey := service.projectChainInputCacheKey(organization, project)
	summary, err := service.store.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("get syfon project metrics summary: %w", err)
	}
	if summary != nil {
		validator := projectAuditRecordValidatorFromSummary(*summary)
		service.chainInputMu.RLock()
		cached, ok := service.projectAuditCache[cacheKey]
		service.chainInputMu.RUnlock()
		if ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			log.Printf("syfon_project_record_cache_hit org=%s project=%s record_count=%d age_ms=%d duration_ms=%d", organization, project, len(cached.records), time.Since(cached.cachedAt).Milliseconds(), time.Since(start).Milliseconds())
			return cached.records, nil
		}

		workKey := projectAuditRecordWorkKey(cacheKey, validator)
		service.chainInputMu.Lock()
		if cached, ok := service.projectAuditCache[cacheKey]; ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			service.chainInputMu.Unlock()
			log.Printf("syfon_project_record_cache_hit org=%s project=%s record_count=%d age_ms=%d duration_ms=%d", organization, project, len(cached.records), time.Since(cached.cachedAt).Milliseconds(), time.Since(start).Milliseconds())
			return cached.records, nil
		}
		if inflight, ok := service.projectAuditWork[workKey]; ok {
			service.chainInputMu.Unlock()
			<-inflight.done
			if inflight.err != nil {
				return nil, inflight.err
			}
			log.Printf("syfon_project_record_cache_wait org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(inflight.records), time.Since(start).Milliseconds())
			return inflight.records, nil
		}
		inflight := &inflightProjectAuditRecordState{
			done:      make(chan struct{}),
			validator: validator,
		}
		service.projectAuditWork[workKey] = inflight
		service.chainInputMu.Unlock()
		log.Printf("syfon_project_record_cache_refresh org=%s project=%s validator_count=%d validator_latest=%q validator_revision=%q", organization, project, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
		defer func() {
			service.chainInputMu.Lock()
			delete(service.projectAuditWork, workKey)
			close(inflight.done)
			service.chainInputMu.Unlock()
		}()

		ProjectRecords, err := service.store.ListProjectAuditRecords(context.WithoutCancel(ctx), authorizationHeader, organization, project)
		if err != nil {
			inflight.err = fmt.Errorf("list syfon project audit records: %w", err)
			return nil, inflight.err
		}
		copiedRecords := append([]gintegrationsyfon.ProjectRecord(nil), ProjectRecords...)
		service.chainInputMu.Lock()
		service.projectAuditCache[cacheKey] = cachedProjectAuditRecordState{
			records:   copiedRecords,
			validator: validator,
			cachedAt:  time.Now(),
		}
		service.chainInputMu.Unlock()
		inflight.records = copiedRecords
		log.Printf("syfon_project_record_cache_refreshed org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(copiedRecords), time.Since(start).Milliseconds())
		return copiedRecords, nil
	}
	ProjectRecords, err := service.store.ListProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	log.Printf("syfon_project_record_cache_uncached org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(ProjectRecords), time.Since(start).Milliseconds())
	return ProjectRecords, nil
}

func projectAuditRecordWorkKey(cacheKey string, validator projectAuditRecordValidator) string {
	return fmt.Sprintf("%s::records::%d::%s::%s", cacheKey, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
}

func buildProjectAuditRecordSet(ProjectRecords []gintegrationsyfon.ProjectRecord) *sarecord.RecordSet {
	recordsByChecksum := make(map[string][]sarecord.State)
	for _, record := range ProjectRecords {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], sarecord.State{
			ProjectRecord: record,
		})
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: recordsByChecksum,
	}
}

func projectAuditRecordValidatorFromSummary(summary gintegrationsyfon.ProjectMetricsSummary) projectAuditRecordValidator {
	return projectAuditRecordValidator{
		RecordCount:             summary.RecordCount,
		RecordLatestUpdatedTime: strings.TrimSpace(summary.RecordLatestUpdatedTime),
		RecordRevision:          strings.TrimSpace(summary.RecordRevision),
	}
}

func (service *Service) loadProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	inventory, err := service.store.ListProjectBucketObjects(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, sarecord.BucketInventoryMetadata{}, fmt.Errorf("list syfon project bucket objects: %w", err)
	}
	objects, lookup := buildBucketObjectLookup(inventory.Objects)
	metadata := sarecord.BucketInventoryMetadata{Complete: inventory.Complete, Warning: strings.TrimSpace(inventory.Warning), Observed: strings.TrimSpace(inventory.ObservedAt), Source: "live"}
	return objects, lookup, metadata, nil
}

func (service *Service) loadProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	inventory, err := service.store.ListProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, sarecord.BucketInventoryMetadata{}, fmt.Errorf("list syfon project bucket inventory: %w", err)
	}
	objects, lookup := buildBucketObjectLookup(inventory.Objects)
	metadata := sarecord.BucketInventoryMetadata{
		Complete: inventory.Complete,
		Warning:  strings.TrimSpace(inventory.Warning),
		Observed: strings.TrimSpace(inventory.ObservedAt),
		Source:   "live",
	}
	return objects, lookup, metadata, nil
}

func (service *Service) loadCachedProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project)
	service.chainInputMu.RLock()
	cached, ok := service.ChainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if ok && time.Now().Before(cached.ExpiresAt) && cached.ProjectScopes != nil {
		return append([]domain.StorageBucketScope(nil), cached.ProjectScopes...), nil
	}
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	service.updateChainInputCache(cacheKey, func(state *ChainInputCacheEntry) {
		state.ProjectScopes = append([]domain.StorageBucketScope(nil), scopes...)
	})
	return scopes, nil
}

func (service *Service) loadCachedProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project) + "::bucket-items::" + sarecord.NormalizeRepoSubpath(bucketPathPrefix)
	service.chainInputMu.RLock()
	cached, ok := service.ChainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if !forceRefresh && ok && time.Now().Before(cached.ExpiresAt) && cached.BucketObjects != nil && cached.BucketObjectsByURL != nil {
		objects, lookup := cloneBucketInventory(cached.BucketObjects, cached.BucketObjectsByURL)
		metadata := cached.BucketMetadata
		metadata.Source = "memory"
		return objects, lookup, metadata, nil
	}
	bucketObjects, bucketObjectsByURL, metadata, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, sarecord.BucketInventoryMetadata{}, err
	}
	service.updateChainInputCache(cacheKey, func(state *ChainInputCacheEntry) {
		state.BucketObjects, state.BucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
		state.BucketMetadata = metadata
	})
	objects, lookup := cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	return objects, lookup, metadata, nil
}

func (service *Service) loadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project) + "::bucket-validation-inventory::" + sarecord.NormalizeRepoSubpath(bucketPathPrefix)
	var stale storecache.CachedBucketInventory
	var hasStale bool
	if cache := service.ProjectBucketCache; cache != nil {
		redisKey := storecache.ProjectBucketInventoryCacheKey(organization, project, bucketPathPrefix)
		cached, ok, err := cache.Get(ctx, redisKey)
		if err != nil {
			log.Printf("syfon_project_bucket_inventory_cache_error org=%s project=%s path_prefix=%q source=%s operation=get error=%q", organization, project, bucketPathPrefix, cache.Source(), err.Error())
		} else if ok && !forceRefresh && time.Since(cached.CachedAt) < storecache.ProjectBucketInventoryRefreshInterval {
			objects, lookup := buildBucketObjectLookup(cached.Objects)
			log.Printf("syfon_project_bucket_inventory_cache_hit org=%s project=%s path_prefix=%q source=%s object_count=%d age_seconds=%d", organization, project, bucketPathPrefix, cache.Source(), len(objects), int64(time.Since(cached.CachedAt).Seconds()))
			return objects, lookup, sarecord.BucketInventoryMetadata{Complete: cached.Complete, Warning: cached.Warning, Observed: cached.Observed, Source: cache.Source()}, nil
		} else if ok {
			stale = cached
			hasStale = true
		}
	}
	service.chainInputMu.RLock()
	cached, ok := service.ChainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if !forceRefresh && ok && time.Now().Before(cached.ExpiresAt) && cached.BucketObjects != nil && cached.BucketObjectsByURL != nil {
		objects, lookup := cloneBucketInventory(cached.BucketObjects, cached.BucketObjectsByURL)
		metadata := cached.BucketMetadata
		metadata.Source = "memory"
		return objects, lookup, metadata, nil
	}
	bucketObjects, bucketObjectsByURL, metadata, err := service.loadProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		if hasStale {
			objects, lookup := buildBucketObjectLookup(stale.Objects)
			log.Printf("syfon_project_bucket_inventory_cache_stale_fallback org=%s project=%s path_prefix=%q source=%s object_count=%d age_seconds=%d refresh_error=%q", organization, project, bucketPathPrefix, service.ProjectBucketCache.Source(), len(objects), int64(time.Since(stale.CachedAt).Seconds()), err.Error())
			warning := strings.TrimSpace(stale.Warning)
			if warning != "" {
				warning += "; "
			}
			warning += err.Error()
			return objects, lookup, sarecord.BucketInventoryMetadata{Complete: false, Warning: warning, Observed: stale.Observed, Source: service.ProjectBucketCache.Source(), Stale: true}, nil
		}
		return nil, nil, sarecord.BucketInventoryMetadata{Complete: false, Warning: err.Error(), Source: "live"}, err
	}
	if cache := service.ProjectBucketCache; cache != nil && len(bucketObjects) > 0 {
		redisKey := storecache.ProjectBucketInventoryCacheKey(organization, project, bucketPathPrefix)
		value := storecache.CachedBucketInventory{CachedAt: time.Now(), Objects: append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...), Complete: metadata.Complete, Warning: metadata.Warning, Observed: metadata.Observed}
		if err := cache.Set(ctx, redisKey, value, storecache.ProjectBucketInventoryStaleTTL); err != nil {
			log.Printf("syfon_project_bucket_inventory_cache_error org=%s project=%s path_prefix=%q source=%s operation=set error=%q", organization, project, bucketPathPrefix, cache.Source(), err.Error())
		} else {
			log.Printf("syfon_project_bucket_inventory_cache_store org=%s project=%s path_prefix=%q source=%s object_count=%d refresh_interval_seconds=%d stale_ttl_seconds=%d", organization, project, bucketPathPrefix, cache.Source(), len(bucketObjects), int64(storecache.ProjectBucketInventoryRefreshInterval.Seconds()), int64(storecache.ProjectBucketInventoryStaleTTL.Seconds()))
		}
	}
	service.updateChainInputCache(cacheKey, func(state *ChainInputCacheEntry) {
		state.BucketObjects, state.BucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
		state.BucketMetadata = metadata
	})
	objects, lookup := cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	return objects, lookup, metadata, nil
}

func (service *Service) projectChainInputCacheKey(organization string, project string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project)
}

func (service *Service) updateChainInputCache(cacheKey string, update func(*ChainInputCacheEntry)) {
	service.chainInputMu.Lock()
	defer service.chainInputMu.Unlock()
	state := service.ChainInputCache[cacheKey]
	if time.Now().After(state.ExpiresAt) {
		state = ChainInputCacheEntry{}
	}
	state.ExpiresAt = time.Now().Add(chainInputCacheTTL)
	update(&state)
	service.ChainInputCache[cacheKey] = state
}

func (service *Service) evictProjectChainInputCache(organization string, project string) {
	baseKey := service.projectChainInputCacheKey(organization, project)
	prefix := baseKey + "::"
	service.chainInputMu.Lock()
	defer service.chainInputMu.Unlock()
	for key := range service.ChainInputCache {
		if key == baseKey || strings.HasPrefix(key, prefix) {
			delete(service.ChainInputCache, key)
		}
	}
}

func cloneBucketInventory(bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	objects := append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...)
	lookup := make(map[string]gintegrationsyfon.ProjectBucketObject, len(bucketObjectsByURL))
	for objectURL, item := range bucketObjectsByURL {
		lookup[objectURL] = item
	}
	return objects, lookup
}

func buildBucketObjectLookup(bucketObjects []gintegrationsyfon.ProjectBucketObject) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	bucketObjectsByURL := make(map[string]gintegrationsyfon.ProjectBucketObject, len(bucketObjects))
	for _, item := range bucketObjects {
		if objectURL := sarecord.CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL); objectURL != "" {
			bucketObjectsByURL[objectURL] = item
		}
	}
	return append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...), bucketObjectsByURL
}

func synthesizeBucketInventoryFromProbes(allProjectRecords map[string][]sarecord.State) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	if len(allProjectRecords) == 0 {
		return []gintegrationsyfon.ProjectBucketObject{}, map[string]gintegrationsyfon.ProjectBucketObject{}
	}
	bucketObjectsByURL := make(map[string]gintegrationsyfon.ProjectBucketObject)
	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, probe := range record.AccessProbes {
				if !strings.EqualFold(strings.TrimSpace(probe.Status), "present") {
					continue
				}
				objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL)
				if objectURL == "" {
					continue
				}
				if _, ok := bucketObjectsByURL[objectURL]; ok {
					continue
				}
				bucketObjectsByURL[objectURL] = gintegrationsyfon.ProjectBucketObject{
					ObjectURL:    objectURL,
					Provider:     strings.TrimSpace(probe.Provider),
					Bucket:       strings.TrimSpace(probe.Bucket),
					Key:          strings.TrimSpace(probe.Key),
					Path:         strings.TrimSpace(probe.Path),
					SizeBytes:    derefInt64(probe.SizeBytes),
					MetaSHA256:   strings.TrimSpace(probe.MetaSHA256),
					ETag:         strings.TrimSpace(probe.ETag),
					LastModified: strings.TrimSpace(probe.LastModified),
				}
			}
		}
	}
	bucketObjects := make([]gintegrationsyfon.ProjectBucketObject, 0, len(bucketObjectsByURL))
	for _, item := range bucketObjectsByURL {
		bucketObjects = append(bucketObjects, item)
	}
	sort.Slice(bucketObjects, func(i, j int) bool {
		return bucketObjects[i].ObjectURL < bucketObjects[j].ObjectURL
	})
	return bucketObjects, bucketObjectsByURL
}

func shouldDegradeBucketInventory(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "project-bucket") &&
		(strings.Contains(message, "status 403") ||
			strings.Contains(message, "status 409") ||
			strings.Contains(message, "permission denied") ||
			strings.Contains(message, "provider denied list access") ||
			strings.Contains(message, "bucket inventory request") ||
			strings.Contains(message, "bucket target may be missing or inaccessible"))
}

func buildStorageChainIndex(inventory []model.RepoInventoryFile, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string) sarecord.ChainIndex {
	repoPathsByChecksum := make(map[string][]string, len(inventory))
	repoPathsByBucketURL := make(map[string][]string, len(inventory))
	repoChecksumsByPath := make(map[string]string, len(inventory))
	for _, item := range inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		repoPath := sarecord.NormalizeRepoSubpath(item.RepoPath)
		if repoPath == "" {
			continue
		}
		repoPathsByChecksum[checksum] = append(repoPathsByChecksum[checksum], repoPath)
		repoChecksumsByPath[repoPath] = checksum
		for _, bucketURL := range sarecord.ProjectScopeRepoPathObjectURLs([]string{repoPath}, scopes, organization, project) {
			repoPathsByBucketURL[bucketURL] = append(repoPathsByBucketURL[bucketURL], repoPath)
		}
	}
	allRecords := sarecord.FlattenRecordStates(allProjectRecords)
	recordsByBucketURL := make(map[string][]sarecord.State)
	for _, record := range allRecords {
		for _, bucketURL := range sarecord.RecordBucketURLs(record) {
			recordsByBucketURL[bucketURL] = append(recordsByBucketURL[bucketURL], record)
		}
	}
	return sarecord.ChainIndex{
		Inventory:            inventory,
		AllRecords:           allRecords,
		BucketObjectsByURL:   bucketObjectsByURL,
		RepoPathsByChecksum:  repoPathsByChecksum,
		RepoPathsByBucketURL: repoPathsByBucketURL,
		RepoChecksumsByPath:  repoChecksumsByPath,
		RecordsByBucketURL:   recordsByBucketURL,
		EquivalentRecordKeys: buildEquivalentRecordKeyIndex(allRecords),
	}
}

func buildStorageChainAuditModel(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string, includeBucketOrigin bool) *chainAuditModel {
	index := buildStorageChainIndex(inventory, allProjectRecords, bucketObjectsByURL, scopes, organization, project)
	acc := chainAuditAccumulator{
		findings: make([]model.ChainFinding, 0),
		summary:  newChainSummary(len(bucketObjectsByURL), len(index.AllRecords), len(inventory)),
	}
	if includeBucketOrigin {
		buildBucketOriginChainFindings(index, &acc)
	}
	buildSyfonOriginChainFindings(index, &acc, !includeBucketOrigin)
	buildGitOriginChainFindings(index, recordsByChecksum, allProjectRecords, scopes, organization, project, &acc)
	return finalizeChainFindings(gitSubpath, acc)
}

func buildBucketOriginChainFindings(index sarecord.ChainIndex, acc *chainAuditAccumulator) {
	bucketURLs := make([]string, 0, len(index.BucketObjectsByURL))
	for bucketURL := range index.BucketObjectsByURL {
		bucketURLs = append(bucketURLs, bucketURL)
	}
	sort.Strings(bucketURLs)
	for _, bucketURL := range bucketURLs {
		item := index.BucketObjectsByURL[bucketURL]
		if sarecord.BucketObjectHasCompleteChain(index.RecordsByBucketURL[bucketURL], index.RepoPathsByChecksum, index.BucketObjectsByURL) {
			acc.addCount("bucket_syfon_git_complete", 1)
			continue
		}
		if len(index.RecordsByBucketURL[bucketURL]) > 0 {
			// An exact Syfon-to-bucket URL match is not a bucket orphan, even
			// when the Git and Syfon checksums disagree. The Syfon-origin pass
			// reports that conflict without offering the bucket for deletion.
			continue
		}
		if bucketObjectHasEquivalentSyfonRecord(item, index.EquivalentRecordKeys) {
			continue
		}
		acc.add("bucket_only_object", buildChainBucketOnlyFinding(item))
	}
}

func buildEquivalentRecordKeyIndex(records []sarecord.State) sarecord.EquivalentRecordKeyIndex {
	index := sarecord.EquivalentRecordKeyIndex{
		ByName:            make(map[string]struct{}),
		ByNameSize:        make(map[string]map[int64]struct{}),
		UnknownSizeByName: make(map[string]struct{}),
	}
	for _, record := range records {
		for _, accessURL := range sarecord.RecordStorageCandidateURLs(record) {
			_, key, ok := sarecord.ParseStorageURL(accessURL)
			if !ok {
				continue
			}
			name := strings.TrimSpace(path.Base(key))
			if name == "" {
				continue
			}
			index.ByName[name] = struct{}{}
			if record.Size <= 0 {
				index.UnknownSizeByName[name] = struct{}{}
				continue
			}
			if index.ByNameSize[name] == nil {
				index.ByNameSize[name] = make(map[int64]struct{})
			}
			index.ByNameSize[name][record.Size] = struct{}{}
		}
	}
	return index
}

func bucketObjectHasEquivalentSyfonRecord(item gintegrationsyfon.ProjectBucketObject, index sarecord.EquivalentRecordKeyIndex) bool {
	itemName := strings.TrimSpace(path.Base(strings.TrimSpace(item.Key)))
	if itemName == "" {
		return false
	}
	if _, ok := index.ByName[itemName]; !ok {
		return false
	}
	if item.SizeBytes <= 0 {
		return true
	}
	if _, ok := index.UnknownSizeByName[itemName]; ok {
		return true
	}
	if sizes := index.ByNameSize[itemName]; sizes != nil {
		_, ok := sizes[item.SizeBytes]
		return ok
	}
	return false
}

func recordHasResolvedStorage(record sarecord.State, bucketMatches []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(bucketMatches) > 0 {
		return true
	}
	return sarecord.ResolveRecordStorage(record, bucketObjectsByURL).HasPresentProbe
}

func logMissingBucketCandidateDebug(record sarecord.State, gitPaths []string, bucketMatches []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, acc *chainAuditAccumulator) {
	if acc == nil || acc.missingBucketDebugLogCount >= storageChainValidationDebugLimit {
		return
	}
	resolution := sarecord.ResolveRecordStorage(record, bucketObjectsByURL)
	log.Printf(
		"INFO: storage_chain_missing_bucket_candidates object_id=%s checksum=%s name=%q git_paths=%q raw_access_urls=%q canonical_access_urls=%q candidate_status=%q bucket_matches=%q access_probe_status=%q",
		strings.TrimSpace(record.ObjectID),
		strings.TrimSpace(record.Checksum),
		strings.TrimSpace(record.Name),
		strings.Join(sarecord.UniqueStrings(gitPaths), ","),
		strings.Join(sarecord.RawAccessURLsForRecord(record), ","),
		strings.Join(sarecord.UniqueStrings(record.CanonicalAccessURLs), ","),
		strings.Join(storageCandidateStatuses(resolution.CandidateURLs, bucketObjectsByURL), ","),
		strings.Join(sarecord.UniqueStrings(bucketMatches), ","),
		strings.Join(storageProbeDebugStatuses(record.AccessProbes), ","),
	)
	acc.missingBucketDebugLogCount++
}

func storageCandidateStatuses(candidateURLs []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	out := make([]string, 0, len(candidateURLs))
	for _, candidate := range sarecord.UniqueStrings(candidateURLs) {
		status := "missing"
		if _, ok := bucketObjectsByURL[candidate]; ok {
			status = "found"
		}
		out = append(out, candidate+"="+status)
	}
	return out
}

func storageProbeDebugStatuses(probes []gintegrationsyfon.BulkStorageProbeResult) []string {
	out := make([]string, 0, len(probes))
	for _, probe := range probes {
		objectURL := sarecord.SyfonProbeObjectURL(probe)
		if objectURL == "" {
			objectURL = strings.TrimSpace(probe.ObjectURL)
		}
		status := strings.TrimSpace(probe.Status)
		if status == "" {
			status = "unknown"
		}
		validation := strings.TrimSpace(probe.ValidationStatus)
		if validation != "" {
			status += "/" + validation
		}
		if errKind := strings.TrimSpace(probe.ErrorKind); errKind != "" {
			status += "/" + errKind
		}
		out = append(out, objectURL+"="+status)
	}
	return out
}

func buildSyfonOriginChainFindings(index sarecord.ChainIndex, acc *chainAuditAccumulator, countCompleteFromSyfon bool) {
	for _, record := range index.AllRecords {
		gitPaths := sarecord.UniqueStrings(index.RepoPathsByChecksum[sarecord.NormalizeAnalyticsChecksum(record.Checksum)])
		bucketMatches := sarecord.MatchedBucketObjectURLs(record, index.BucketObjectsByURL)
		classification := sarecord.ClassifyStorageFinding(record, index.BucketObjectsByURL)
		if len(gitPaths) == 0 && len(bucketMatches) > 0 && (classification == sarecord.FindingNone || classification == sarecord.FindingValidationMismatch) {
			if samePathGitPaths := repoPathsForBucketURLs(index.RepoPathsByBucketURL, bucketMatches); len(samePathGitPaths) > 0 {
				findings := buildSamePathChecksumConflictFindings(index, record, samePathGitPaths, bucketMatches)
				acc.add("git_syfon_metadata_mismatch", findings...)
				continue
			}
		}
		switch classification {
		case sarecord.FindingBrokenBucketMap:
			findingRecord := sarecord.RepairableBrokenAccessRecord(record)
			if len(findingRecord.AccessProbes) == 0 {
				findingRecord = record
			}
			findings := buildChainRecordFindingsWithOptions("syfon_broken_bucket_mapping", findingRecord, gitPaths, bucketMatches, "Syfon access URL did not resolve through a configured bucket mapping.", true)
			acc.findings = append(acc.findings, findings...)
			acc.addCount("syfon_broken_bucket_mapping", len(findings))
		case sarecord.FindingObjectMissing:
			if len(gitPaths) > 0 {
				logMissingBucketCandidateDebug(record, gitPaths, bucketMatches, index.BucketObjectsByURL, acc)
				findings := buildChainRecordFindings("syfon_git_no_bucket", record, gitPaths, bucketMatches, "Git and Syfon matched, but the mapped bucket object does not exist.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("syfon_git_no_bucket", len(gitPaths))
				continue
			}
			acc.add("syfon_missing_bucket_object", buildChainRecordFindings("syfon_missing_bucket_object", record, nil, bucketMatches, "Syfon record points to a mapped bucket location, but the object does not exist.")...)
		case sarecord.FindingValidationMismatch:
			if len(gitPaths) > 0 {
				findings := buildChainRecordFindings("git_syfon_metadata_mismatch", record, gitPaths, bucketMatches, "Mapped bucket object exists, but object evidence disagrees with the Syfon/Git record.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("git_syfon_metadata_mismatch", len(gitPaths))
				continue
			}
			acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
		case sarecord.FindingBrokenAccessURL, sarecord.FindingProbeError:
			findings := buildChainRecordFindings("probe_error", record, gitPaths, bucketMatches, "Storage probes could not confirm this mapped object. Gecko does not treat it as missing; after review, you may delete the Syfon record.")
			acc.findings = append(acc.findings, findings...)
			acc.addCount("probe_error", sarecord.ChainPathCount(gitPaths))
		case sarecord.FindingNone:
			if countCompleteFromSyfon && len(gitPaths) > 0 && recordHasResolvedStorage(record, bucketMatches, index.BucketObjectsByURL) {
				acc.addCount("bucket_syfon_git_complete", len(gitPaths))
				continue
			}
			if len(gitPaths) == 0 && len(bucketMatches) > 0 {
				acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
			}
		}
	}
}

func repoPathsForBucketURLs(repoPathsByBucketURL map[string][]string, bucketURLs []string) []string {
	paths := make([]string, 0, len(bucketURLs))
	for _, bucketURL := range sarecord.UniqueStrings(bucketURLs) {
		paths = append(paths, repoPathsByBucketURL[bucketURL]...)
	}
	return sarecord.UniqueStrings(paths)
}

func hasSyfonRecordForBucketURLs(recordsByBucketURL map[string][]sarecord.State, bucketURLs []string) bool {
	for _, bucketURL := range sarecord.UniqueStrings(bucketURLs) {
		if len(recordsByBucketURL[bucketURL]) > 0 {
			return true
		}
	}
	return false
}

func buildSamePathChecksumConflictFindings(index sarecord.ChainIndex, record sarecord.State, gitPaths []string, bucketMatches []string) []model.ChainFinding {
	findings := buildChainRecordFindings(
		"git_syfon_metadata_mismatch",
		record,
		gitPaths,
		bucketMatches,
		"Git and Syfon map to the same bucket object path, but their checksums disagree. Do not delete this object automatically.",
	)
	for findingIndex := range findings {
		findings[findingIndex].Actionability = sarecord.ActionabilityInspectOnly
		findings[findingIndex].AvailableActions = []string{sarecord.ActionInspectEvidence}
		findings[findingIndex].DefaultAction = sarecord.ActionInspectEvidence
		findings[findingIndex].SupportsDryRun = false
		gitChecksum := index.RepoChecksumsByPath[sarecord.NormalizeRepoSubpath(findings[findingIndex].NormalizedPath)]
		findings[findingIndex].Error = "Git LFS and Syfon map to the same bucket object path but have different SHA-256 values."
		findings[findingIndex].SuggestedFix = fmt.Sprintf(
			"Git LFS pointer SHA-256 is %s; Syfon record SHA-256 is %s. Recompute the bucket object's SHA-256 and reconcile the stale side manually before changing Git, Syfon, or the bucket object.",
			gitChecksum,
			strings.TrimSpace(record.Checksum),
		)
	}
	return findings
}

func buildGitOriginChainFindings(index sarecord.ChainIndex, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, scopes []domain.StorageBucketScope, organization string, project string, acc *chainAuditAccumulator) {
	for _, item := range index.Inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if len(allProjectRecords[checksum]) > 0 || len(recordsByChecksum[checksum]) > 0 {
			continue
		}
		bucketURLs := sarecord.ProjectScopeRepoPathObjectURLs([]string{item.RepoPath}, scopes, organization, project)
		if hasSyfonRecordForBucketURLs(index.RecordsByBucketURL, bucketURLs) {
			// The Syfon-origin pass reports an exact-path checksum conflict as a
			// metadata mismatch. Do not also offer this Git pointer for Syfon
			// creation just because its checksum differs.
			continue
		}
		bucketObjectURL := ""
		bucketSizeBytes := int64(0)
		bucketEvaluation := "not_evaluated"
		if len(bucketURLs) == 1 {
			bucketObjectURL = bucketURLs[0]
			if bucketObject, ok := index.BucketObjectsByURL[bucketObjectURL]; ok {
				bucketSizeBytes = bucketObject.SizeBytes
				bucketEvaluation = "present"
			} else {
				bucketEvaluation = "not_found"
			}
		} else if len(bucketURLs) > 1 {
			bucketEvaluation = "ambiguous_mapping"
		}
		actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("git_only_no_syfon", "verified")
		recommendedAction := "Git checksum has no matching Syfon record. Bucket presence is not claimed by this finding."
		if bucketEvaluation == "present" && bucketSizeBytes == item.Size {
			recommendedAction = "Git checksum has no matching Syfon record. The mapped bucket object exists and its byte size matches the Git LFS pointer; create a Syfon record after reviewing that RGW did not provide a SHA-256 for verification."
		} else if bucketEvaluation == "present" {
			recommendedAction = "Git checksum has no matching Syfon record. The mapped bucket object exists but its byte size differs from the Git LFS pointer, so it is not safe to create a Syfon record."
		}
		acc.add("git_only_no_syfon", model.ChainFinding{
			Kind:              "git_only_no_syfon",
			EvidenceStatus:    "verified",
			NormalizedPath:    item.RepoPath,
			Checksum:          checksum,
			SourcePaths:       []string{item.RepoPath},
			ObjectIDs:         []string{},
			RecordCount:       0,
			SizeBytes:         item.Size,
			BucketObjectURL:   bucketObjectURL,
			BucketSizeBytes:   bucketSizeBytes,
			RecommendedAction: recommendedAction,
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence: &model.Evidence{
				Checksum:         checksum,
				SourcePaths:      []string{item.RepoPath},
				ObjectIDs:        []string{},
				BucketObjectURLs: bucketURLs,
				BucketEvaluation: bucketEvaluation,
			},
		})
	}
}

type storageAuditBaseInputs struct {
	index             *repoindex.Index
	inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

func (service *Service) loadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]model.RepoInventoryFile, error) {
	index, err := service.store.LoadRepoIndex(ctx, ref, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	return repoindex.FilterRepoInventoryFiles(index, gitSubpath)
}

type storageChainInputs struct {
	inventory          []model.RepoInventoryFile
	recordSet          *sarecord.RecordSet
	scopes             []domain.StorageBucketScope
	BucketSummary      *gintegrationsyfon.ProjectBucketSummary
	BucketObjects      []gintegrationsyfon.ProjectBucketObject
	BucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
	bucketInventoryErr error
	BucketMetadata     sarecord.BucketInventoryMetadata
}

func (service *Service) loadStorageChainInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, bucketMode string, validationMode string, bucketPathPrefix string, forceBucketRefresh bool, timings *model.StorageChainAuditTimings) (*storageChainInputs, error) {
	type inventoryResult struct {
		inventory []model.RepoInventoryFile
		err       error
	}
	type recordResult struct {
		recordSet *sarecord.RecordSet
		err       error
	}
	type scopeResult struct {
		scopes []domain.StorageBucketScope
		err    error
	}
	type bucketResult struct {
		BucketSummary      *gintegrationsyfon.ProjectBucketSummary
		BucketObjects      []gintegrationsyfon.ProjectBucketObject
		BucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
		err                error
		metadata           sarecord.BucketInventoryMetadata
	}

	inventoryCh := make(chan inventoryResult, 1)
	recordCh := make(chan recordResult, 1)
	scopeCh := make(chan scopeResult, 1)
	bucketCh := make(chan bucketResult, 1)

	go func() {
		start := time.Now()
		timings.StageStart("repo_index")
		inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
		timings.Record("repo_index", time.Since(start))
		timings.RecordMemory("repo_index", "git_files", len(inventory))
		logStorageChainInputResult("repo_index", len(inventory), err)
		inventoryCh <- inventoryResult{inventory: inventory, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_records")
		recordSet, err := service.loadCachedProjectAuditRecordSet(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_records", time.Since(start))
		recordCount := 0
		if recordSet != nil {
			recordCount = countRecordStates(recordSet.AllProjectRecords)
		}
		timings.RecordMemory("syfon_project_records", "syfon_records", recordCount)
		logStorageChainInputResult("syfon_project_records", recordCount, err)
		recordCh <- recordResult{recordSet: recordSet, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_scopes")
		scopes, err := service.loadCachedProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_scopes", time.Since(start))
		logStorageChainInputResult("syfon_project_scopes", len(scopes), err)
		scopeCh <- scopeResult{scopes: scopes, err: err}
	}()
	go func() {
		if bucketMode == model.StorageChainBucketModeValidate && validationMode != model.StorageChainValidationModeList {
			timings.Record("syfon_bucket_inventory_skipped", 0)
			bucketCh <- bucketResult{
				BucketObjects:      []gintegrationsyfon.ProjectBucketObject{},
				BucketObjectsByURL: map[string]gintegrationsyfon.ProjectBucketObject{},
				metadata:           sarecord.BucketInventoryMetadata{Complete: false, Source: "not_requested"},
			}
			return
		}
		start := time.Now()
		timings.StageStart("syfon_bucket_inventory")
		var bucketObjects []gintegrationsyfon.ProjectBucketObject
		var bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
		var err error
		metadata := sarecord.BucketInventoryMetadata{Complete: true, Source: "live"}
		if bucketMode == model.StorageChainBucketModeValidate {
			// Full bucket LIST calls are expensive on some providers. Reuse the
			// project inventory during the cooldown unless the caller explicitly
			// requests a new bucket traversal.
			bucketObjects, bucketObjectsByURL, metadata, err = service.loadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, "", forceBucketRefresh)
		} else {
			bucketObjects, bucketObjectsByURL, metadata, err = service.loadCachedProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceBucketRefresh)
		}
		timings.Record("syfon_bucket_inventory", time.Since(start))
		timings.RecordMemory("syfon_bucket_inventory", "bucket_objects", len(bucketObjects), "bucket_lookup", len(bucketObjectsByURL))
		logStorageChainInputResult("syfon_bucket_items", len(bucketObjects), err)
		bucketCh <- bucketResult{BucketObjects: bucketObjects, BucketObjectsByURL: bucketObjectsByURL, err: err, metadata: metadata}
	}()

	inventory := <-inventoryCh
	recordSet := <-recordCh
	scopes := <-scopeCh
	bucketResultValue := <-bucketCh
	if inventory.err != nil {
		return nil, inventory.err
	}
	if recordSet.err != nil {
		return nil, recordSet.err
	}
	if scopes.err != nil {
		return nil, scopes.err
	}
	return &storageChainInputs{
		inventory:          inventory.inventory,
		recordSet:          recordSet.recordSet,
		scopes:             scopes.scopes,
		BucketSummary:      bucketResultValue.BucketSummary,
		BucketObjects:      bucketResultValue.BucketObjects,
		BucketObjectsByURL: bucketResultValue.BucketObjectsByURL,
		bucketInventoryErr: bucketResultValue.err,
		BucketMetadata:     bucketResultValue.metadata,
	}, nil
}

func logStorageChainInputResult(stage string, count int, err error) {
	if err != nil {
		log.Printf("INFO: storage_chain_input_done stage=%s count=%d error=%q", strings.TrimSpace(stage), count, err.Error())
		return
	}
	log.Printf("INFO: storage_chain_input_done stage=%s count=%d", strings.TrimSpace(stage), count)
}

type chainAuditAccumulator struct {
	findings                   []model.ChainFinding
	summary                    model.ChainSummary
	missingBucketDebugLogCount int
}

func newChainSummary(bucketObjectCount, syfonRecordCount, gitTrackedFileCount int) model.ChainSummary {
	return model.ChainSummary{
		CountsByKind: map[string]int{
			"bucket_only_object":          0,
			"bucket_syfon_no_git":         0,
			"bucket_syfon_git_complete":   0,
			"syfon_broken_bucket_mapping": 0,
			"syfon_missing_bucket_object": 0,
			"syfon_git_no_bucket":         0,
			"git_only_no_syfon":           0,
			"git_syfon_metadata_mismatch": 0,
			"probe_error":                 0,
		},
		BucketObjectCount:        bucketObjectCount,
		SyfonRecordCount:         syfonRecordCount,
		GitTrackedFileCount:      gitTrackedFileCount,
		BucketInventoryAvailable: true,
	}
}

func (acc *chainAuditAccumulator) add(kind string, findings ...model.ChainFinding) {
	acc.findings = append(acc.findings, findings...)
	acc.summary.CountsByKind[kind] += len(findings)
}

func (acc *chainAuditAccumulator) addCount(kind string, count int) {
	acc.summary.CountsByKind[kind] += count
}

func finalizeChainFindings(gitSubpath string, acc chainAuditAccumulator) *chainAuditModel {
	sort.Slice(acc.findings, func(i, j int) bool {
		if acc.findings[i].Kind == acc.findings[j].Kind {
			return acc.findings[i].NormalizedPath < acc.findings[j].NormalizedPath
		}
		return acc.findings[i].Kind < acc.findings[j].Kind
	})
	acc.summary.TotalFindings = len(acc.findings)
	return &chainAuditModel{
		Findings:   acc.findings,
		Summary:    acc.summary,
		PathPrefix: sarecord.NormalizeRepoSubpath(gitSubpath),
	}
}

func summarizeChainIssueGroups(findings []model.ChainFinding) []model.ChainIssueGroup {
	groups := make(map[string]*model.ChainIssueGroup)
	groupPaths := make(map[string]map[string]struct{})
	groupObjects := make(map[string]map[string]struct{})
	for _, finding := range findings {
		group := groups[finding.Kind]
		if group == nil {
			group = &model.ChainIssueGroup{Kind: finding.Kind}
			groups[finding.Kind] = group
			groupPaths[finding.Kind] = map[string]struct{}{}
			groupObjects[finding.Kind] = map[string]struct{}{}
		}
		group.FindingCount++
		group.RecordCount += finding.RecordCount
		group.TotalBytes += finding.SizeBytes
		groupPaths[finding.Kind][finding.NormalizedPath] = struct{}{}
		for _, objectID := range finding.ObjectIDs {
			groupObjects[finding.Kind][objectID] = struct{}{}
		}
	}
	kinds := make([]string, 0, len(groups))
	for kind := range groups {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	out := make([]model.ChainIssueGroup, 0, len(kinds))
	for _, kind := range kinds {
		group := *groups[kind]
		group.PathCount = len(groupPaths[kind])
		group.ObjectCount = len(groupObjects[kind])
		out = append(out, group)
	}
	return out
}

func (service *Service) loadStorageAuditBaseInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*storageAuditBaseInputs, error) {
	state, err := service.store.LoadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
	if err != nil {
		return nil, err
	}
	return &storageAuditBaseInputs{
		index:             state.Index,
		inventory:         state.Inventory,
		RecordsByChecksum: state.RecordsByChecksum,
		usageByObjectID:   state.UsageByObjectID,
	}, nil
}

func (service *Service) loadScopedProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string, base *storageAuditBaseInputs) (*sarecord.RecordSet, error) {
	allProjectRecords, err := service.store.ListProjectRecordStates(ctx, authorizationHeader, organization, project, base.usageByObjectID)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: base.RecordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func (service *Service) loadProjectScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return service.store.LoadProjectStorageScopes(ctx, authorizationHeader, organization, project)
}

func (service *Service) loadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	scopes, err := service.store.ListProjectScopes(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project scopes: %w", err)
	}
	if len(scopes) == 0 {
		scopes, err = service.store.LoadProjectStorageScopes(ctx, authorizationHeader, organization, project)
		if err != nil {
			return nil, err
		}
	}
	sort.SliceStable(scopes, func(i, j int) bool {
		iProject := strings.TrimSpace(scopes[i].ProjectID)
		jProject := strings.TrimSpace(scopes[j].ProjectID)
		if iProject == "" && jProject != "" {
			return true
		}
		if iProject != "" && jProject == "" {
			return false
		}
		if scopes[i].Bucket != scopes[j].Bucket {
			return scopes[i].Bucket < scopes[j].Bucket
		}
		return scopes[i].Path < scopes[j].Path
	})
	return scopes, nil
}

func applyScopeCanonicalization(recordSet *sarecord.RecordSet, scopes []domain.StorageBucketScope, organization string, project string) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	recordsByChecksum, allProjectRecords := applyScopedStorageMappings(recordSet.RecordsByChecksum, recordSet.AllProjectRecords, scopes, organization, project)
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}
}

func (service *Service) loadCachedProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string) (*sarecord.RecordSet, error) {
	ProjectRecords, err := service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	return buildProjectAuditRecordSet(ProjectRecords), nil
}

func (service *Service) attachProjectStorageProbes(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.store.AttachStorageProbes(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func (service *Service) attachProjectStorageListValidations(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.store.AttachStorageListValidations(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func attachProjectStorageInventoryValidations(recordSet *sarecord.RecordSet, inventory []model.RepoInventoryFile, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string) (*sarecord.RecordSet, error) {
	if recordSet == nil {
		return nil, nil
	}
	inventoryBuckets := projectInventoryBuckets(bucketObjectsByURL)
	repoPathsByChecksum := repoPathsByChecksumForInventory(inventory)
	attach := func(input map[string][]sarecord.State) (map[string][]sarecord.State, error) {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				probes, err := inventoryValidationProbesForRecord(record, repoPathsByChecksum[sarecord.NormalizeAnalyticsChecksum(record.Checksum)], bucketObjectsByURL, inventoryBuckets, scopes, organization, project)
				if err != nil {
					return nil, err
				}
				clone.AccessProbes = probes
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out, nil
	}
	recordsByChecksum, err := attach(recordSet.RecordsByChecksum)
	if err != nil {
		return nil, err
	}
	allProjectRecords, err := attach(recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func repoPathsByChecksumForInventory(inventory []model.RepoInventoryFile) map[string][]string {
	out := make(map[string][]string)
	for _, item := range inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		out[checksum] = append(out[checksum], item.RepoPath)
	}
	for checksum, paths := range out {
		out[checksum] = sarecord.UniqueStrings(paths)
	}
	return out
}

func inventoryValidationProbesForRecord(record sarecord.State, repoPaths []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, inventoryBuckets map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0)
	seen := make(map[string]struct{})
	accessURLs, err := recordAccessURLsForProjectInventory(record.AccessURLs, inventoryBuckets, scopes, organization, project)
	if err != nil {
		return nil, err
	}
	accessURLs = append(accessURLs, sarecord.ProjectScopeRepoPathObjectURLs(repoPaths, scopes, organization, project)...)
	accessURLs = sarecord.UniqueStrings(accessURLs)
	for _, accessURL := range accessURLs {
		objectURL := sarecord.CanonicalStorageURL("", "", accessURL)
		if objectURL == "" {
			continue
		}
		bucket, _, _ := sarecord.ParseStorageURL(objectURL)
		if len(inventoryBuckets) > 0 {
			if _, ok := inventoryBuckets[bucket]; !ok {
				return nil, fmt.Errorf("storage access URL mapped to %q, but bucket %q is not present in project bucket inventory for project %s/%s", objectURL, bucket, strings.TrimSpace(organization), strings.TrimSpace(project))
			}
		}
		if _, ok := seen[objectURL]; ok {
			continue
		}
		seen[objectURL] = struct{}{}
		expectedName := sarecord.ExpectedStorageObjectNameForListValidation(objectURL, record.Name)
		if item, ok := bucketObjectsByURL[objectURL]; ok {
			probes = append(probes, inventoryPresentProbe(record, objectURL, expectedName, item))
			continue
		}
		probes = append(probes, inventoryMissingProbe(record, objectURL, expectedName))
	}
	return probes, nil
}

func recordAccessURLsForProjectInventory(accessURLs []string, inventoryBuckets map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) ([]string, error) {
	out := make([]string, 0, len(accessURLs))
	for _, accessURL := range accessURLs {
		mapped := false
		if objectURL := sarecord.CanonicalizeScopedStorageURL(accessURL, scopes, organization, project); objectURL != "" {
			out = append(out, objectURL)
			mapped = true
		}
		objectURL := sarecord.CanonicalStorageURL("", "", accessURL)
		if objectURL == "" {
			continue
		}
		bucket, _, ok := sarecord.ParseStorageURL(objectURL)
		if !ok {
			continue
		}
		if len(inventoryBuckets) > 0 {
			if _, ok := inventoryBuckets[bucket]; ok {
				out = append(out, objectURL)
				mapped = true
			}
		}
		if !mapped {
			return nil, fmt.Errorf("storage access URL %q could not be mapped into bucket scopes for project %s/%s", strings.TrimSpace(accessURL), strings.TrimSpace(organization), strings.TrimSpace(project))
		}
	}
	return sarecord.UniqueStrings(out), nil
}

func projectInventoryBuckets(bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) map[string]struct{} {
	out := make(map[string]struct{})
	for objectURL, item := range bucketObjectsByURL {
		if bucket := strings.TrimSpace(item.Bucket); bucket != "" {
			out[bucket] = struct{}{}
			continue
		}
		bucket, _, ok := sarecord.ParseStorageURL(objectURL)
		if ok {
			out[bucket] = struct{}{}
		}
	}
	return out
}

func inventoryPresentProbe(record sarecord.State, objectURL string, expectedName string, item gintegrationsyfon.ProjectBucketObject) gintegrationsyfon.BulkStorageProbeResult {
	size := item.SizeBytes
	sizeMatch := sarecord.StorageSizesMatchForAudit(record.Size, item.SizeBytes)
	nameMatch := true
	mismatches := make([]string, 0, 1)
	if !sizeMatch {
		mismatches = append(mismatches, "size_mismatch")
	}
	validationStatus := "matched"
	if len(mismatches) > 0 {
		validationStatus = "mismatched"
	}
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   sarecord.StorageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            model.StorageChainValidationModeInventory,
		ObjectURL:            objectURL,
		Provider:             strings.TrimSpace(item.Provider),
		Bucket:               strings.TrimSpace(item.Bucket),
		Key:                  strings.Trim(strings.TrimSpace(item.Key), "/"),
		Path:                 strings.TrimSpace(item.Path),
		Exists:               true,
		Status:               "present",
		SizeBytes:            &size,
		MetaSHA256:           strings.TrimSpace(item.MetaSHA256),
		ETag:                 strings.TrimSpace(item.ETag),
		LastModified:         strings.TrimSpace(item.LastModified),
		ValidationStatus:     validationStatus,
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: mismatches,
	}
}

func inventoryMissingProbe(record sarecord.State, objectURL string, expectedName string) gintegrationsyfon.BulkStorageProbeResult {
	bucket, key, _ := sarecord.ParseStorageURL(objectURL)
	sizeMatch := false
	nameMatch := false
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   sarecord.StorageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            model.StorageChainValidationModeInventory,
		ObjectURL:            objectURL,
		Provider:             "s3",
		Bucket:               bucket,
		Key:                  key,
		Path:                 path.Base(key),
		Exists:               false,
		Status:               "unknown",
		Error:                fmt.Sprintf("object %q was absent from project bucket inventory and requires exact verification", objectURL),
		ErrorKind:            "inventory_miss",
		ValidationStatus:     "unverifiable",
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: []string{},
	}
}

func (service *Service) buildStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *sarecord.RecordSet, inventory []model.RepoInventoryFile, scopes []domain.StorageBucketScope, bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, bucketInventoryErr error, BucketMetadata sarecord.BucketInventoryMetadata, bucketMode string, validationMode string, timings *model.StorageChainAuditTimings) (*sarecord.StorageView, error) {
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	view := &sarecord.StorageView{
		Scopes:                   append([]domain.StorageBucketScope(nil), scopes...),
		RecordsByChecksum:        recordSet.RecordsByChecksum,
		AllProjectRecords:        recordSet.AllProjectRecords,
		BucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		BucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		BucketInventoryAvailable: true,
		BucketInventoryComplete:  BucketMetadata.Complete,
		BucketInventoryWarning:   BucketMetadata.Warning,
		BucketInventoryObserved:  BucketMetadata.Observed,
		BucketInventorySource:    BucketMetadata.Source,
		BucketInventoryStale:     BucketMetadata.Stale,
	}
	if bucketMode == model.StorageChainBucketModeValidate {
		if bucketInventoryErr != nil {
			view.BucketInventoryAvailable = false
			view.BucketInventoryError = strings.TrimSpace(bucketInventoryErr.Error())
		}
		if validationMode == model.StorageChainValidationModeInventory {
			return view, nil
		}
		if validationMode == model.StorageChainValidationModeList {
			if bucketInventoryErr != nil {
				probeStart := time.Now()
				probedRecordSet, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
				timings.Record("inventory_error_head_fallback", time.Since(probeStart))
				if probeErr != nil {
					return nil, probeErr
				}
				view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
				view.AllProjectRecords = probedRecordSet.AllProjectRecords
				view.BucketObjects, view.BucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.AllProjectRecords)
				return view, nil
			}
			view.BucketObjects = bucketObjects
			view.BucketObjectsByURL = bucketObjectsByURL
			validateStart := time.Now()
			probedRecordSet, err := attachProjectStorageInventoryValidations(recordSet, inventory, view.BucketObjectsByURL, scopes, organization, project)
			if err != nil {
				return nil, err
			}
			timings.Record("inventory_list_validation", time.Since(validateStart))

			candidateStart := time.Now()
			candidates := selectInventoryMissRecordSet(probedRecordSet)
			if BucketMetadata.Stale {
				candidates = recordSet
			}
			timings.Record("exact_list_candidate_selection", time.Since(candidateStart))
			if candidates != nil {
				probeStart := time.Now()
				exactRecordSet, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, candidates)
				timings.Record("targeted_head_validation", time.Since(probeStart))
				if probeErr != nil {
					return nil, probeErr
				}
				probedRecordSet = mergeRecordSetProbes(probedRecordSet, exactRecordSet)
			}
			timings.RecordMemory("inventory_list_validation", "syfon_records", countRecordStates(probedRecordSet.AllProjectRecords), "bucket_objects", len(view.BucketObjectsByURL), "exact_probe_records", countRecordSet(candidates))
			view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
			view.AllProjectRecords = probedRecordSet.AllProjectRecords
			return view, nil
		}
		probeStart := time.Now()
		var (
			probedRecordSet *sarecord.RecordSet
			probeErr        error
			stage           string
		)
		switch validationMode {
		case model.StorageChainValidationModeMetadata:
			probedRecordSet, probeErr = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
			stage = "bulk_metadata_validation"
		default:
			probedRecordSet, probeErr = service.attachProjectStorageListValidations(ctx, authorizationHeader, recordSet)
			stage = "bulk_list_validation"
		}
		timings.Record(stage, time.Since(probeStart))
		if probedRecordSet != nil {
			timings.RecordMemory(stage, "syfon_records", countRecordStates(probedRecordSet.AllProjectRecords))
		}
		if probeErr != nil {
			return nil, probeErr
		}
		view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
		view.AllProjectRecords = probedRecordSet.AllProjectRecords
		view.BucketObjects, view.BucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.AllProjectRecords)
		return view, nil
	}
	if bucketInventoryErr != nil {
		return nil, bucketInventoryErr
	}
	view.BucketObjects, view.BucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	if validationMode == model.StorageChainValidationModeInventory || validationMode == model.StorageChainValidationModeList {
		return view, nil
	}

	probeSelectStart := time.Now()
	probeCandidates := selectTargetedProbeRecordSet(recordSet, view.BucketObjectsByURL)
	timings.Record("targeted_probe_selection", time.Since(probeSelectStart))
	if probeCandidates == nil {
		return view, nil
	}
	probeStart := time.Now()
	probedSubset, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, probeCandidates)
	timings.Record("bulk_probe", time.Since(probeStart))
	if probedSubset != nil {
		timings.RecordMemory("bulk_probe", "probe_records", countRecordStates(probedSubset.AllProjectRecords))
	}
	if probeErr != nil {
		return nil, probeErr
	}
	merged := mergeRecordSetProbes(recordSet, probedSubset)
	view.RecordsByChecksum = merged.RecordsByChecksum
	view.AllProjectRecords = merged.AllProjectRecords
	return view, nil
}

func (service *Service) loadStorageAuditStorageView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *sarecord.RecordSet, includeBucketInventory bool, includeProbes bool) (*sarecord.StorageView, error) {
	scopes, err := service.loadProjectScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	if includeProbes {
		recordSet, err = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
		if err != nil {
			return nil, err
		}
	}
	view := &sarecord.StorageView{
		Scopes:                   scopes,
		RecordsByChecksum:        recordSet.RecordsByChecksum,
		AllProjectRecords:        recordSet.AllProjectRecords,
		BucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		BucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		BucketInventoryAvailable: includeBucketInventory,
	}
	if includeBucketInventory {
		bucketObjects, bucketObjectsByURL, metadata, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, "")
		if err != nil {
			if !includeProbes || !shouldDegradeBucketInventory(err) {
				return nil, err
			}
			view.BucketInventoryAvailable = false
			view.BucketInventoryError = strings.TrimSpace(err.Error())
			bucketObjects, bucketObjectsByURL = synthesizeBucketInventoryFromProbes(recordSet.AllProjectRecords)
		}
		view.BucketObjects = bucketObjects
		view.BucketObjectsByURL = bucketObjectsByURL
		view.BucketInventoryComplete = metadata.Complete
		view.BucketInventoryWarning = metadata.Warning
		view.BucketInventoryObserved = metadata.Observed
		view.BucketInventorySource = metadata.Source
	}
	return view, nil
}

func cloneRecordStateMap(input map[string][]sarecord.State) map[string][]sarecord.State {
	out := make(map[string][]sarecord.State, len(input))
	for checksum, group := range input {
		states := make([]sarecord.State, 0, len(group))
		for _, record := range group {
			clone := record
			clone.CanonicalAccessURLs = append([]string(nil), record.CanonicalAccessURLs...)
			clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
			states = append(states, clone)
		}
		out[checksum] = states
	}
	return out
}

func selectTargetedProbeRecordSet(recordSet *sarecord.RecordSet, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			if !recordNeedsTargetedProbe(record, bucketObjectsByURL) {
				continue
			}
			selected[checksum] = append(selected[checksum], record)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func selectInventoryMissRecordSet(recordSet *sarecord.RecordSet) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			accessURLs := make(map[string]struct{})
			for _, accessURL := range sarecord.AccessURLsForStorage(record) {
				if objectURL := sarecord.CanonicalStorageURL("", "", accessURL); objectURL != "" {
					accessURLs[objectURL] = struct{}{}
				}
			}
			missingURLs := make([]string, 0)
			for _, probe := range record.AccessProbes {
				if strings.TrimSpace(probe.Operation) != model.StorageChainValidationModeInventory || strings.TrimSpace(probe.ErrorKind) != "inventory_miss" {
					continue
				}
				if objectURL := sarecord.SyfonProbeObjectURL(probe); objectURL != "" {
					if _, ok := accessURLs[objectURL]; !ok {
						continue
					}
					missingURLs = append(missingURLs, objectURL)
				}
			}
			missingURLs = sarecord.UniqueStrings(missingURLs)
			if len(missingURLs) == 0 {
				continue
			}
			clone := record
			clone.CanonicalAccessURLs = missingURLs
			selected[checksum] = append(selected[checksum], clone)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func countRecordSet(recordSet *sarecord.RecordSet) int {
	if recordSet == nil {
		return 0
	}
	return countRecordStates(recordSet.AllProjectRecords)
}

func recordNeedsTargetedProbe(record sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	resolution := sarecord.ResolveRecordStorage(record, bucketObjectsByURL)
	if len(resolution.MatchedBucketObjectURLs) == 0 {
		return true
	}
	if !sameStringSet(sarecord.RawAccessURLsForRecord(record), sarecord.AccessURLsForStorage(record)) {
		return true
	}
	checksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
	if checksum == "" {
		return false
	}
	for _, objectURL := range resolution.MatchedBucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if sarecord.NormalizeAnalyticsChecksum(item.MetaSHA256) != "" {
			return false
		}
	}
	return true
}

func sameStringSet(left []string, right []string) bool {
	a := sarecord.UniqueStrings(left)
	b := sarecord.UniqueStrings(right)
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, value := range a {
		set[strings.TrimSpace(value)] = struct{}{}
	}
	for _, value := range b {
		if _, ok := set[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func mergeRecordSetProbes(base *sarecord.RecordSet, probed *sarecord.RecordSet) *sarecord.RecordSet {
	if base == nil {
		return probed
	}
	if probed == nil {
		return base
	}
	probesByObjectID := make(map[string][]gintegrationsyfon.BulkStorageProbeResult)
	for _, group := range probed.AllProjectRecords {
		for _, record := range group {
			probesByObjectID[strings.TrimSpace(record.ObjectID)] = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
		}
	}
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				if probes, ok := probesByObjectID[strings.TrimSpace(record.ObjectID)]; ok {
					clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), probes...)
				}
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: attach(base.RecordsByChecksum),
		AllProjectRecords: attach(base.AllProjectRecords),
	}
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

type InflightResponseRefresh struct {
	done               chan struct{}
	forceBucketRefresh bool
	value              storecache.CachedResponse
	err                error
}

// coalesceStorageChainAuditRefresh ensures a root audit has one authoritative
// refresh result per cache key. The UI can issue overlapping forced refreshes;
// without this guard, a partial bucket LIST from the later request can replace
// the completed response from the earlier request.
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

func buildChainBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) model.ChainFinding {
	objectURL := sarecord.CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("bucket_only_object", "verified")
	return model.ChainFinding{
		Kind:              "bucket_only_object",
		EvidenceStatus:    "verified",
		NormalizedPath:    objectURL,
		ObjectIDs:         []string{},
		AccessURLs:        []string{objectURL},
		BucketObjectURL:   objectURL,
		ResolvedBucket:    strings.TrimSpace(item.Bucket),
		ResolvedKey:       strings.TrimSpace(item.Key),
		ProbeStatus:       "enumerated",
		RecordCount:       0,
		SizeBytes:         item.SizeBytes,
		RecommendedAction: "Bucket object exists, but no Syfon record matched it.",
		Actionability:     actionability,
		AvailableActions:  availableActions,
		DefaultAction:     defaultAction,
		SupportsDryRun:    supportsDryRun,
		Evidence: &model.Evidence{
			AccessURLs:        []string{objectURL},
			BucketObjectURLs:  []string{objectURL},
			Buckets:           sarecord.UniqueStrings([]string{item.Bucket}),
			Keys:              sarecord.UniqueStrings([]string{item.Key}),
			StorageOperations: []string{model.StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}

func storageChainActionSupportForEvidence(kind string, evidenceStatus string) (string, []string, string, bool) {
	if strings.TrimSpace(kind) == "syfon_broken_bucket_mapping" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionRemoveBrokenAccessURLs, sarecord.ActionInspectEvidence},
			sarecord.ActionInspectEvidence,
			true
	}
	if strings.TrimSpace(kind) == "git_only_no_syfon" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionCreateSyfonRecord, sarecord.ActionInspectEvidence},
			sarecord.ActionCreateSyfonRecord,
			false
	}
	if strings.TrimSpace(kind) == "git_syfon_metadata_mismatch" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionDeleteSyfonRecord, sarecord.ActionDeleteBucketObject, sarecord.ActionDeleteBoth, sarecord.ActionInspectEvidence},
			"",
			true
	}
	repair := sarecord.RepairPolicyForKind(kind)
	if repair.Actionability == sarecord.ActionabilityAutoRepair && !sarecord.AllowsAutomaticRepair(kind, sarecord.EvidenceStatus(evidenceStatus)) {
		repair = sarecord.InspectOnlyRepairPolicy()
	}
	return repair.Actionability, append([]string(nil), repair.Actions...), repair.DefaultAction, repair.SupportsDryRun
}

func buildChainRecordFindings(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string) []model.ChainFinding {
	return buildChainRecordFindingsWithOptions(kind, record, gitPaths, bucketObjectURLs, action, false)
}

func buildChainRecordFindingsWithOptions(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string, preferSyfonRecordPath bool) []model.ChainFinding {
	paths := sarecord.UniqueStrings(gitPaths)
	if len(paths) == 0 {
		displayPath := sarecord.OrphanDisplayPath(strings.TrimSpace(record.Checksum), append(bucketObjectURLs, record.AccessURLs...))
		if preferSyfonRecordPath {
			displayPath = sarecord.SyfonRecordDisplayPath(record)
		}
		if displayPath == "" {
			displayPath = strings.TrimSpace(record.Checksum)
		}
		paths = []string{displayPath}
	}
	objectIDs := sarecord.UniqueStrings([]string{record.ObjectID})
	accessURLs := sarecord.UniqueStrings(record.AccessURLs)
	evidence := sarecord.BuildFindingEvidence(strings.TrimSpace(record.Checksum), gitPaths, []sarecord.State{record}, "enumerated_and_probed")
	if evidence != nil {
		evidence.BucketObjectURLs = sarecord.UniqueStrings(append(evidence.BucketObjectURLs, bucketObjectURLs...))
	}
	evidenceStatus := storageEvidenceStatus(record, len(bucketObjectURLs) > 0)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(kind, evidenceStatus)
	primaryProbe := selectChainProbe(record, bucketObjectURLs)
	suggestedFix := suggestedFixForChainFinding(kind, record)
	suggestedAction := suggestedActionForChainFinding(kind, record)
	if suggestedAction != "" && !stringSliceContains(availableActions, suggestedAction) {
		availableActions = append([]string(nil), availableActions...)
		availableActions = append(availableActions, suggestedAction)
	}
	findings := make([]model.ChainFinding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, model.ChainFinding{
			Kind:              kind,
			EvidenceStatus:    evidenceStatus,
			NormalizedPath:    path,
			Checksum:          strings.TrimSpace(record.Checksum),
			SourcePaths:       sarecord.UniqueStrings(gitPaths),
			ObjectIDs:         objectIDs,
			Records:           []model.CleanupRecordAudit{sarecord.CleanupRecordAuditForRecord(path, "access_url", record)},
			AccessURLs:        accessURLs,
			BucketObjectURL:   primaryProbe.bucketObjectURL,
			ResolvedBucket:    primaryProbe.probe.Bucket,
			ResolvedKey:       primaryProbe.probe.Key,
			ProbeStatus:       primaryProbe.probe.Status,
			ErrorKind:         primaryProbe.probe.ErrorKind,
			Error:             chainFindingError(kind, record, primaryProbe.probe),
			RecordCount:       1,
			SizeBytes:         record.Size,
			RecommendedAction: action,
			SuggestedFix:      suggestedFix,
			SuggestedAction:   suggestedAction,
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence:          evidence,
		})
	}
	return findings
}

func suggestedActionForChainFinding(kind string, record sarecord.State) string {
	return ""
}

func suggestedFixForChainFinding(kind string, record sarecord.State) string {
	normalizedKind := strings.TrimSpace(kind)
	switch normalizedKind {
	case "git_syfon_metadata_mismatch":
	case "syfon_broken_bucket_mapping":
		return ""
	default:
		return ""
	}
	mismatches := make([]string, 0, 2)
	if probe, ok := firstAccessProbeMismatch(record, "size_mismatch"); ok {
		bucketSize := int64(0)
		if probe.SizeBytes != nil {
			bucketSize = *probe.SizeBytes
		}
		mismatches = append(mismatches, fmt.Sprintf("Syfon record size is %s, bucket inventory reports %s", formatAuditSize(record.Size), formatAuditSize(bucketSize)))
	}
	if len(mismatches) == 0 {
		return "Bucket object exists, but its storage evidence does not match the Syfon/Git record. Review the record and bucket object before applying a destructive fix."
	}
	return strings.Join(mismatches, ". ") + ". Because the byte lengths differ, the bucket object cannot have the current Git/Syfon SHA-256. Recompute the bucket object's SHA-256 before manually reconciling Git and Syfon."
}

func firstAccessProbeMismatch(record sarecord.State, mismatch string) (model.CleanupAccessProbe, bool) {
	for _, probe := range sarecord.AccessProbesForRecord(record) {
		if accessProbeHasMismatch(probe, mismatch) {
			return probe, true
		}
	}
	return model.CleanupAccessProbe{}, false
}

func accessProbeHasMismatch(probe model.CleanupAccessProbe, mismatch string) bool {
	for _, item := range probe.ValidationMismatches {
		if strings.TrimSpace(item) == mismatch {
			return true
		}
	}
	return false
}

func formatAuditSize(size int64) string {
	if size <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%d B", size)
}

type chainProbeSelection struct {
	bucketObjectURL string
	probe           model.CleanupAccessProbe
}

func selectChainProbe(record sarecord.State, bucketObjectURLs []string) chainProbeSelection {
	probes := sarecord.AccessProbesForRecord(record)
	if len(bucketObjectURLs) > 0 {
		targets := map[string]struct{}{}
		for _, bucketURL := range bucketObjectURLs {
			targets[strings.TrimSpace(bucketURL)] = struct{}{}
		}
		for _, probe := range probes {
			objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if _, ok := targets[objectURL]; ok {
				return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
			}
		}
	}
	for _, probe := range probes {
		operation := strings.TrimSpace(probe.Operation)
		if operation != model.StorageChainValidationModeMetadata && operation != model.StorageChainValidationModeList {
			continue
		}
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	for _, probe := range probes {
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	return chainProbeSelection{}
}

func chainFindingError(kind string, record sarecord.State, probe model.CleanupAccessProbe) string {
	if trimmed := strings.TrimSpace(probe.Error); trimmed != "" {
		return trimmed
	}
	switch kind {
	case "syfon_broken_bucket_mapping":
		return "Syfon access URL did not resolve through a configured bucket mapping"
	case "syfon_missing_bucket_object", "syfon_git_no_bucket":
		return "mapped bucket object was not found"
	case "git_syfon_metadata_mismatch":
		return "mapped bucket object exists, but object evidence disagrees with the Syfon/Git record"
	case "probe_error":
		return sarecord.AccessErrorForRecord(record)
	default:
		return ""
	}
}

func storageEvidenceStatus(record sarecord.State, bucketObserved bool) string {
	return string(sarecord.AssessStorageRecordEvidence(record, bucketObserved).Status)
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func applyScopedStorageMappings(recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, scopes []domain.StorageBucketScope, organization string, project string) (map[string][]sarecord.State, map[string][]sarecord.State) {
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, state := range group {
				clone := state
				clone.CanonicalAccessURLs = sarecord.CanonicalizeRecordAccessURLs(state.AccessURLs, scopes, organization, project)
				clone.CanonicalAccessURLByRaw = sarecord.CanonicalizeRecordAccessURLMappings(state.AccessURLs, scopes, organization, project)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return attach(recordsByChecksum), attach(allProjectRecords)
}

func FilterStorageChainSummary(summary model.ChainSummary, findings []model.ChainFinding) model.ChainSummary {
	return filterStorageChainSummary(summary, findings)
}

func ActionSupportForEvidence(kind string, evidenceStatus string) (string, []string, string, bool) {
	return storageChainActionSupportForEvidence(kind, evidenceStatus)
}
