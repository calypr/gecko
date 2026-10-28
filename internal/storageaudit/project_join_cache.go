package storageaudit

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	"github.com/go-git/go-git/v5/plumbing"
)

const projectJoinCacheTTL = 45 * time.Second

type cachedProjectJoinState struct {
	expiresAt         time.Time
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type inflightProjectJoinState struct {
	done              chan struct{}
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
	err               error
}

type projectFileUsageBackend interface {
	ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
}

type projectJoinCache struct {
	mu       sync.RWMutex
	entries  map[string]cachedProjectJoinState
	inflight map[string]*inflightProjectJoinState
	exact    storecache.ExactJoinCache
	chain    *chain.Service
	storage  projectFileUsageBackend
}

func newProjectJoinCache(chainSvc *chain.Service, storage projectFileUsageBackend) *projectJoinCache {
	return &projectJoinCache{
		entries:  map[string]cachedProjectJoinState{},
		inflight: map[string]*inflightProjectJoinState{},
		chain:    chainSvc,
		storage:  storage,
	}
}

func (cache *projectJoinCache) enableExactCacheFromEnv() {
	cache.exact = storecache.NewStorageExactProjectJoinCacheFromEnv()
}

func (cache *projectJoinCache) load(ctx context.Context, authorizationHeader string, organization string, project string, hash plumbing.Hash, inventory []RepoInventoryFile, forceRefresh bool) (map[string][]sarecord.State, map[string]gintegrationsyfon.FileUsage, error) {
	cacheKey := cache.key(organization, project, hash.String())
	redisKey := storecache.StorageExactProjectJoinCacheKey(organization, project, hash.String())
	cache.mu.Lock()
	if !forceRefresh {
		if cached, ok := cache.entries[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
			cache.mu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=memory records_by_checksum=%d usage_rows=%d", organization, project, len(cached.RecordsByChecksum), len(cached.usageByObjectID))
			return cloneRecordStateMap(cached.RecordsByChecksum), storecache.CloneFileUsageMap(cached.usageByObjectID), nil
		}
	}
	if inflight, ok := cache.inflight[cacheKey]; ok {
		cache.mu.Unlock()
		<-inflight.done
		return cloneRecordStateMap(inflight.RecordsByChecksum), storecache.CloneFileUsageMap(inflight.usageByObjectID), inflight.err
	}
	inflight := &inflightProjectJoinState{done: make(chan struct{})}
	cache.inflight[cacheKey] = inflight
	cache.mu.Unlock()
	defer func() {
		cache.mu.Lock()
		delete(cache.inflight, cacheKey)
		close(inflight.done)
		cache.mu.Unlock()
	}()

	if !forceRefresh && cache.exact != nil {
		redisStart := time.Now()
		cached, ok, err := cache.exact.Get(ctx, redisKey)
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=get error=%q", organization, project, cache.exact.Source(), err.Error())
		}
		if ok {
			state := cachedProjectJoinState{
				expiresAt:         time.Now().Add(projectJoinCacheTTL),
				RecordsByChecksum: cloneRecordStateMap(cached.RecordsByChecksum),
				usageByObjectID:   storecache.CloneFileUsageMap(cached.UsageByObjectID),
			}
			cache.mu.Lock()
			cache.entries[cacheKey] = state
			cache.mu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=%s age_seconds=%d records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, cache.exact.Source(), int64(time.Since(cached.CachedAt).Seconds()), len(cached.RecordsByChecksum), len(cached.UsageByObjectID), time.Since(redisStart).Milliseconds())
			inflight.RecordsByChecksum = cloneRecordStateMap(cached.RecordsByChecksum)
			inflight.usageByObjectID = storecache.CloneFileUsageMap(cached.UsageByObjectID)
			return cloneRecordStateMap(cached.RecordsByChecksum), storecache.CloneFileUsageMap(cached.UsageByObjectID), nil
		}
		log.Printf("storage_folder_exact_join_cache_miss org=%s project=%s source=%s duration_ms=%d", organization, project, cache.exact.Source(), time.Since(redisStart).Milliseconds())
	}

	recordStart := time.Now()
	projectRecords, err := cache.chain.LoadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		inflight.err = err
		return nil, nil, inflight.err
	}
	checksums := inventoryChecksumSet(inventory)
	matchedRecords := filterProjectRecordsByChecksum(projectRecords, checksums)
	log.Printf("storage_folder_exact_syfon_local_join org=%s project=%s git_checksums=%d project_records=%d matched_records=%d duration_ms=%d", organization, project, len(checksums), len(projectRecords), len(matchedRecords), time.Since(recordStart).Milliseconds())

	usageStart := time.Now()
	objectIDs := projectRecordObjectIDs(matchedRecords)
	usageByObjectID, err := cache.listProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs, cleanupInactiveDays)
	if err != nil {
		inflight.err = fmt.Errorf("list syfon project file usage: %w", err)
		return nil, nil, inflight.err
	}
	log.Printf("storage_folder_exact_bulk_usage_lookup org=%s project=%s object_ids=%d usage_rows=%d duration_ms=%d", organization, project, len(objectIDs), len(usageByObjectID), time.Since(usageStart).Milliseconds())

	recordsByChecksum := buildProjectJoinRecordsByChecksum(matchedRecords, usageByObjectID)
	cache.mu.Lock()
	cache.entries[cacheKey] = cachedProjectJoinState{
		expiresAt:         time.Now().Add(projectJoinCacheTTL),
		RecordsByChecksum: cloneRecordStateMap(recordsByChecksum),
		usageByObjectID:   storecache.CloneFileUsageMap(usageByObjectID),
	}
	cache.mu.Unlock()
	if cache.exact != nil {
		setStart := time.Now()
		err := cache.exact.Set(ctx, redisKey, storecache.CachedExactJoinState{
			CachedAt:          time.Now(),
			RecordsByChecksum: recordsByChecksum,
			UsageByObjectID:   usageByObjectID,
		}, storecache.StorageChainAuditCacheTTL())
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=set error=%q", organization, project, cache.exact.Source(), err.Error())
		} else {
			log.Printf("storage_folder_exact_join_cache_store org=%s project=%s source=%s records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, cache.exact.Source(), len(recordsByChecksum), len(usageByObjectID), time.Since(setStart).Milliseconds())
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

func (cache *projectJoinCache) listProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	usageByObjectID := make(map[string]gintegrationsyfon.FileUsage)
	if len(objectIDs) == 0 {
		return usageByObjectID, nil
	}
	for start := 0; start < len(objectIDs); start += projectFileUsageBulkChunkSize {
		end := start + projectFileUsageBulkChunkSize
		if end > len(objectIDs) {
			end = len(objectIDs)
		}
		chunkUsage, err := cache.storage.ListProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs[start:end], inactiveDays)
		if err != nil {
			if gintegrationsyfon.IsHTTPStatus(err, http.StatusNotFound, http.StatusMethodNotAllowed) {
				return cache.storage.ListProjectFileUsage(ctx, authorizationHeader, organization, project, inactiveDays)
			}
			return nil, err
		}
		for objectID, usage := range chunkUsage {
			usageByObjectID[objectID] = usage
		}
	}
	return usageByObjectID, nil
}

func (cache *projectJoinCache) key(organization string, project string, commitHash string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::" + strings.TrimSpace(commitHash)
}

func (cache *projectJoinCache) evict(organization string, project string) {
	prefix := strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::"
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for key := range cache.entries {
		if strings.HasPrefix(key, prefix) {
			delete(cache.entries, key)
		}
	}
}
