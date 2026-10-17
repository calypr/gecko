package storageaudit

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) loadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*repoindex.Index, []RepoInventoryFile, map[string][]sarecord.State, map[string]gintegrationsyfon.FileUsage, error) {
	index, err := repoindex.LoadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	inventory, err := repoindex.FilterRepoInventoryFiles(index, gitSubpath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	recordsByChecksum, usageByObjectID, err := service.loadProjectJoinCache(ctx, authorizationHeader, organization, project, hash, index.Sidecar.Files, forceRefresh)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return index, inventory, recordsByChecksum, usageByObjectID, nil
}

func (service *StorageAnalyticsService) loadProjectJoinCache(ctx context.Context, authorizationHeader string, organization string, project string, hash plumbing.Hash, inventory []RepoInventoryFile, forceRefresh bool) (map[string][]sarecord.State, map[string]gintegrationsyfon.FileUsage, error) {
	cacheKey := service.projectJoinCacheKey(organization, project, hash.String())
	redisKey := storecache.StorageExactProjectJoinCacheKey(organization, project, hash.String())
	service.projectJoinMu.Lock()
	if !forceRefresh {
		if cached, ok := service.projectJoinCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
			service.projectJoinMu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=memory records_by_checksum=%d usage_rows=%d", organization, project, len(cached.RecordsByChecksum), len(cached.usageByObjectID))
			return cloneRecordStateMap(cached.RecordsByChecksum), storecache.CloneFileUsageMap(cached.usageByObjectID), nil
		}
	}
	if inflight, ok := service.projectJoinWork[cacheKey]; ok {
		service.projectJoinMu.Unlock()
		<-inflight.done
		return cloneRecordStateMap(inflight.RecordsByChecksum), storecache.CloneFileUsageMap(inflight.usageByObjectID), inflight.err
	}
	inflight := &inflightProjectJoinState{done: make(chan struct{})}
	service.projectJoinWork[cacheKey] = inflight
	service.projectJoinMu.Unlock()
	defer func() {
		service.projectJoinMu.Lock()
		delete(service.projectJoinWork, cacheKey)
		close(inflight.done)
		service.projectJoinMu.Unlock()
	}()

	if !forceRefresh && service.exactProjectJoinCache != nil {
		redisStart := time.Now()
		cached, ok, err := service.exactProjectJoinCache.Get(ctx, redisKey)
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=get error=%q", organization, project, service.exactProjectJoinCache.Source(), err.Error())
		}
		if ok {
			state := cachedProjectJoinState{
				expiresAt:         time.Now().Add(projectJoinCacheTTL),
				RecordsByChecksum: cloneRecordStateMap(cached.RecordsByChecksum),
				usageByObjectID:   storecache.CloneFileUsageMap(cached.UsageByObjectID),
			}
			service.projectJoinMu.Lock()
			service.projectJoinCache[cacheKey] = state
			service.projectJoinMu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=%s age_seconds=%d records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), int64(time.Since(cached.CachedAt).Seconds()), len(cached.RecordsByChecksum), len(cached.UsageByObjectID), time.Since(redisStart).Milliseconds())
			inflight.RecordsByChecksum = cloneRecordStateMap(cached.RecordsByChecksum)
			inflight.usageByObjectID = storecache.CloneFileUsageMap(cached.UsageByObjectID)
			return cloneRecordStateMap(cached.RecordsByChecksum), storecache.CloneFileUsageMap(cached.UsageByObjectID), nil
		}
		log.Printf("storage_folder_exact_join_cache_miss org=%s project=%s source=%s duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), time.Since(redisStart).Milliseconds())
	}

	recordStart := time.Now()
	projectRecords, err := service.chainSvc.LoadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		inflight.err = err
		return nil, nil, inflight.err
	}
	checksums := inventoryChecksumSet(inventory)
	matchedRecords := filterProjectRecordsByChecksum(projectRecords, checksums)
	log.Printf("storage_folder_exact_syfon_local_join org=%s project=%s git_checksums=%d project_records=%d matched_records=%d duration_ms=%d", organization, project, len(checksums), len(projectRecords), len(matchedRecords), time.Since(recordStart).Milliseconds())

	usageStart := time.Now()
	objectIDs := projectRecordObjectIDs(matchedRecords)
	usageByObjectID, err := service.listProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs, cleanupInactiveDays)
	if err != nil {
		inflight.err = fmt.Errorf("list syfon project file usage: %w", err)
		return nil, nil, inflight.err
	}
	log.Printf("storage_folder_exact_bulk_usage_lookup org=%s project=%s object_ids=%d usage_rows=%d duration_ms=%d", organization, project, len(objectIDs), len(usageByObjectID), time.Since(usageStart).Milliseconds())

	recordsByChecksum := buildProjectJoinRecordsByChecksum(matchedRecords, usageByObjectID)
	service.projectJoinMu.Lock()
	service.projectJoinCache[cacheKey] = cachedProjectJoinState{
		expiresAt:         time.Now().Add(projectJoinCacheTTL),
		RecordsByChecksum: cloneRecordStateMap(recordsByChecksum),
		usageByObjectID:   storecache.CloneFileUsageMap(usageByObjectID),
	}
	service.projectJoinMu.Unlock()
	if service.exactProjectJoinCache != nil {
		setStart := time.Now()
		err := service.exactProjectJoinCache.Set(ctx, redisKey, storecache.CachedExactJoinState{
			CachedAt:          time.Now(),
			RecordsByChecksum: recordsByChecksum,
			UsageByObjectID:   usageByObjectID,
		}, storecache.StorageChainAuditCacheTTL())
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=set error=%q", organization, project, service.exactProjectJoinCache.Source(), err.Error())
		} else {
			log.Printf("storage_folder_exact_join_cache_store org=%s project=%s source=%s records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), len(recordsByChecksum), len(usageByObjectID), time.Since(setStart).Milliseconds())
		}
	}
	inflight.RecordsByChecksum = cloneRecordStateMap(recordsByChecksum)
	inflight.usageByObjectID = storecache.CloneFileUsageMap(usageByObjectID)
	return recordsByChecksum, usageByObjectID, nil
}

func inventoryChecksumSet(inventory []RepoInventoryFile) map[string]struct{} {
	checksums := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		checksums[checksum] = struct{}{}
	}
	return checksums
}

func filterProjectRecordsByChecksum(records []gintegrationsyfon.ProjectRecord, checksums map[string]struct{}) []gintegrationsyfon.ProjectRecord {
	out := make([]gintegrationsyfon.ProjectRecord, 0)
	for _, record := range records {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		if _, ok := checksums[normalizedChecksum]; !ok {
			continue
		}
		record.Checksum = normalizedChecksum
		out = append(out, record)
	}
	return out
}

func projectRecordObjectIDs(records []gintegrationsyfon.ProjectRecord) []string {
	objectIDs := make([]string, 0, len(records))
	for _, record := range records {
		objectIDs = append(objectIDs, record.ObjectID)
	}
	return sarecord.UniqueStrings(objectIDs)
}

func buildProjectJoinRecordsByChecksum(records []gintegrationsyfon.ProjectRecord, usageByObjectID map[string]gintegrationsyfon.FileUsage) map[string][]sarecord.State {
	recordsByChecksum := make(map[string][]sarecord.State)
	for _, record := range records {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], sarecord.State{
			ProjectRecord: record,
			Usage:         usageByObjectID[record.ObjectID],
		})
	}
	return recordsByChecksum
}

func (service *StorageAnalyticsService) listProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	usageByObjectID := make(map[string]gintegrationsyfon.FileUsage)
	if len(objectIDs) == 0 {
		return usageByObjectID, nil
	}
	for start := 0; start < len(objectIDs); start += projectFileUsageBulkChunkSize {
		end := start + projectFileUsageBulkChunkSize
		if end > len(objectIDs) {
			end = len(objectIDs)
		}
		chunkUsage, err := service.storage.ListProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs[start:end], inactiveDays)
		if err != nil {
			if gintegrationsyfon.IsHTTPStatus(err, http.StatusNotFound, http.StatusMethodNotAllowed) {
				return service.storage.ListProjectFileUsage(ctx, authorizationHeader, organization, project, inactiveDays)
			}
			return nil, err
		}
		for objectID, usage := range chunkUsage {
			usageByObjectID[objectID] = usage
		}
	}
	return usageByObjectID, nil
}

func (service *StorageAnalyticsService) projectJoinCacheKey(organization string, project string, commitHash string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::" + strings.TrimSpace(commitHash)
}

func (service *StorageAnalyticsService) evictProjectJoinCache(organization string, project string) {
	prefix := strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::"
	service.projectJoinMu.Lock()
	defer service.projectJoinMu.Unlock()
	for key := range service.projectJoinCache {
		if strings.HasPrefix(key, prefix) {
			delete(service.projectJoinCache, key)
		}
	}
}

func (service *StorageAnalyticsService) evictProjectAuditRecordCache(organization string, project string) {
	cacheKey := service.chainSvc.ProjectChainInputCacheKey(organization, project)
	service.chainSvc.EvictProjectAuditRecords(cacheKey)
}

func (service *StorageAnalyticsService) listProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]sarecord.State, error) {
	projectRecords, err := service.storage.ListProjectRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project records: %w", err)
	}
	out := make(map[string][]sarecord.State)
	for _, record := range projectRecords {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		out[normalizedChecksum] = append(out[normalizedChecksum], sarecord.State{
			ProjectRecord: record,
			Usage:         usageByObjectID[record.ObjectID],
		})
	}
	return out, nil
}

func (service *StorageAnalyticsService) loadProjectStorageScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	buckets, err := service.storage.ListBuckets(ctx, authorizationHeader)
	if err != nil {
		return nil, fmt.Errorf("list syfon buckets: %w", err)
	}
	bucketNames := make([]string, 0, len(buckets))
	for bucket := range buckets {
		bucketNames = append(bucketNames, bucket)
	}
	sort.Strings(bucketNames)
	scopes := make([]domain.StorageBucketScope, 0)
	for _, bucket := range bucketNames {
		items, err := service.storage.ListBucketScopes(ctx, authorizationHeader, bucket)
		if err != nil {
			return nil, fmt.Errorf("list syfon bucket scopes for %s: %w", bucket, err)
		}
		for _, scope := range items {
			if !strings.EqualFold(strings.TrimSpace(scope.Organization), organization) {
				continue
			}
			scopeProject := strings.TrimSpace(scope.ProjectID)
			if scopeProject != "" && !strings.EqualFold(scopeProject, project) {
				continue
			}
			scopes = append(scopes, scope)
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

func (service *StorageAnalyticsService) attachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, false)
}

func (service *StorageAnalyticsService) attachStorageListValidations(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, true)
}

func (service *StorageAnalyticsService) attachStorageValidationResults(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, useListValidation bool) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	started := time.Now()
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemKeys := map[string]string{}
	recordProbeKeysByObjectID := map[string][]string{}
	accessURLCount := 0

	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, accessURL := range sarecord.ProbeAccessURLsForRecord(record) {
				accessURLCount++
				normalizedURL := strings.TrimSpace(accessURL)
				if normalizedURL == "" {
					continue
				}
				key := sarecord.StorageProbeRequestKey(normalizedURL, record.Size, record.Checksum)
				expectedName := ""
				if useListValidation {
					expectedName = sarecord.ExpectedStorageObjectNameForListValidation(normalizedURL, record.Name)
					key = sarecord.StorageListValidationRequestKey(normalizedURL, record.Size, expectedName)
				}
				recordProbeKeysByObjectID[record.ObjectID] = append(recordProbeKeysByObjectID[record.ObjectID], key)
				if _, ok := itemKeys[key]; ok {
					continue
				}
				itemKeys[key] = key
				expectedSize := record.Size
				item := gintegrationsyfon.BulkStorageProbeItem{
					ID:                key,
					ObjectURL:         normalizedURL,
					ExpectedSizeBytes: &expectedSize,
					ExpectedSHA256:    strings.TrimSpace(record.Checksum),
				}
				if useListValidation {
					item.ExpectedSHA256 = ""
					item.ExpectedName = expectedName
				}
				items = append(items, item)
				if useListValidation && len(items) <= 20 {
					log.Printf(
						"INFO: storage_chain_bulk_list_item object_id=%s name=%q checksum=%s submitted_url=%s raw_access_urls=%q canonical_access_urls=%q expected_size=%d expected_name=%q",
						strings.TrimSpace(record.ObjectID),
						strings.TrimSpace(record.Name),
						strings.TrimSpace(record.Checksum),
						normalizedURL,
						strings.Join(sarecord.RawAccessURLsForRecord(record), ","),
						strings.Join(sarecord.AccessURLsForStorage(record), ","),
						record.Size,
						item.ExpectedName,
					)
				}
			}
		}
	}
	mode := "head"
	operation := StorageChainValidationModeMetadata
	if useListValidation {
		mode = "list"
		operation = StorageChainValidationModeList
	}
	log.Printf(
		"INFO: storage_chain_validation_request_built mode=%s record_count=%d access_url_count=%d unique_request_count=%d duplicate_request_count=%d build_ms=%d",
		mode,
		countRecordStates(allProjectRecords),
		accessURLCount,
		len(items),
		accessURLCount-len(items),
		time.Since(started).Milliseconds(),
	)

	resultsByKey := map[string]gintegrationsyfon.BulkStorageProbeResult{}
	if len(items) > 0 {
		var (
			results []gintegrationsyfon.BulkStorageProbeResult
			err     error
		)
		if useListValidation {
			results, err = service.storage.BulkListStorageObjects(ctx, authorizationHeader, items)
		} else {
			results, err = service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, items)
		}
		if err != nil {
			if useListValidation {
				log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
				return nil, nil, fmt.Errorf("list-validate syfon storage objects: %w", err)
			}
			log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
			return nil, nil, fmt.Errorf("probe syfon storage objects: %w", err)
		}
		for _, result := range results {
			result.Operation = operation
			resultsByKey[strings.TrimSpace(result.ID)] = result
		}
	}
	log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d result_count=%d duration_ms=%d", mode, len(items), len(resultsByKey), time.Since(started).Milliseconds())

	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				keys := sarecord.UniqueStrings(recordProbeKeysByObjectID[record.ObjectID])
				probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(keys))
				for _, key := range keys {
					if result, ok := resultsByKey[key]; ok {
						result.Operation = operation
						probes = append(probes, result)
					}
				}
				clone.AccessProbes = append(append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...), probes...)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}

	return attach(recordsByChecksum), attach(allProjectRecords), nil
}

func cloneRecordStateMap(input map[string][]sarecord.State) map[string][]sarecord.State {
	out := make(map[string][]sarecord.State, len(input))
	for checksum, group := range input {
		out[checksum] = append([]sarecord.State(nil), group...)
	}
	return out
}

func countRecordStates(input map[string][]sarecord.State) int {
	count := 0
	for _, group := range input {
		count += len(group)
	}
	return count
}
