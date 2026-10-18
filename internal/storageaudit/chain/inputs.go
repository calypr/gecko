package chain

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

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

func buildProjectAuditRecordSet(projectRecords []gintegrationsyfon.ProjectRecord) *sarecord.RecordSet {
	recordsByChecksum := make(map[string][]sarecord.State)
	for _, record := range projectRecords {
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
		switch {
		case err != nil:
			log.Printf("syfon_project_bucket_inventory_cache_error org=%s project=%s path_prefix=%q source=%s operation=get error=%q", organization, project, bucketPathPrefix, cache.Source(), err.Error())
		case ok && !forceRefresh && time.Since(cached.CachedAt) < storecache.ProjectBucketInventoryRefreshInterval:
			objects, lookup := buildBucketObjectLookup(cached.Objects)
			log.Printf("syfon_project_bucket_inventory_cache_hit org=%s project=%s path_prefix=%q source=%s object_count=%d age_seconds=%d", organization, project, bucketPathPrefix, cache.Source(), len(objects), int64(time.Since(cached.CachedAt).Seconds()))
			return objects, lookup, sarecord.BucketInventoryMetadata{Complete: cached.Complete, Warning: cached.Warning, Observed: cached.Observed, Source: cache.Source()}, nil
		case ok:
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
		var metadata sarecord.BucketInventoryMetadata
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

func (service *Service) loadCachedProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string) (*sarecord.RecordSet, error) {
	ProjectRecords, err := service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	return buildProjectAuditRecordSet(ProjectRecords), nil
}
