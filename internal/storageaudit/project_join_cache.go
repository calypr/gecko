package storageaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/sharedwork"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	"github.com/go-git/go-git/v5/plumbing"
)

type projectJoinResult struct {
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type projectFileUsageBackend interface {
	ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
}

type projectJoinCache struct {
	work    *sharedwork.Group[projectJoinResult]
	exact   storecache.ExactJoinCache
	chain   *chain.Service
	storage projectFileUsageBackend
}

func newProjectJoinCache(chainSvc *chain.Service, storage projectFileUsageBackend) *projectJoinCache {
	return &projectJoinCache{
		work:    sharedwork.New[projectJoinResult](sharedwork.DefaultBudget),
		chain:   chainSvc,
		storage: storage,
	}
}

func (cache *projectJoinCache) load(ctx context.Context, authorizationHeader string, organization string, project string, hash plumbing.Hash, inventory []RepoInventoryFile, forceRefresh bool) (map[string][]sarecord.State, map[string]gintegrationsyfon.FileUsage, error) {
	started := time.Now()
	cacheKey := cache.key(organization, project, hash.String())
	redisKey := storecache.StorageExactProjectJoinCacheKey(organization, project, hash.String())
	checksums := inventoryChecksumSet(inventory)
	workKey := projectJoinWorkKey(cacheKey, checksums)
	result, joined, err := cache.work.Do(ctx, projectAuditKey(organization, project), sharedwork.Demand{
		AuthorizationScope: sharedwork.AuthorizationScope(authorizationHeader),
		SemanticKey:        workKey,
		ForceAudit:         forceRefresh,
	}, func(workCtx context.Context) (projectJoinResult, error) {
		if !forceRefresh && cache.exact != nil {
			redisStart := time.Now()
			cached, ok, err := cache.exact.Get(workCtx, redisKey)
			if err != nil {
				log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=get error=%q", organization, project, cache.exact.Source(), err.Error())
				return projectJoinResult{}, fmt.Errorf("load storage folder exact join from %s cache: %w", cache.exact.Source(), err)
			}
			if ok {
				log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=%s age_seconds=%d records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, cache.exact.Source(), int64(time.Since(cached.CachedAt).Seconds()), len(cached.RecordsByChecksum), len(cached.UsageByObjectID), time.Since(redisStart).Milliseconds())
				return projectJoinResult{RecordsByChecksum: cloneRecordStateMap(cached.RecordsByChecksum), usageByObjectID: storecache.CloneFileUsageMap(cached.UsageByObjectID)}, nil
			}
			log.Printf("storage_folder_exact_join_cache_miss org=%s project=%s source=%s duration_ms=%d", organization, project, cache.exact.Source(), time.Since(redisStart).Milliseconds())
		}

		recordStart := time.Now()
		projectRecords, err := cache.chain.LoadProjectAuditRecords(workCtx, authorizationHeader, organization, project)
		if err != nil {
			return projectJoinResult{}, err
		}
		matchedRecords := filterProjectRecordsByChecksum(projectRecords, checksums)
		log.Printf("storage_folder_exact_syfon_local_join org=%s project=%s git_checksums=%d project_records=%d matched_records=%d duration_ms=%d", organization, project, len(checksums), len(projectRecords), len(matchedRecords), time.Since(recordStart).Milliseconds())

		usageStart := time.Now()
		objectIDs := projectRecordObjectIDs(matchedRecords)
		usageByObjectID, err := cache.listProjectFileUsageByObjectIDs(workCtx, authorizationHeader, organization, project, objectIDs, cleanupInactiveDays)
		if err != nil {
			return projectJoinResult{}, fmt.Errorf("list syfon project file usage: %w", err)
		}
		log.Printf("storage_folder_exact_bulk_usage_lookup org=%s project=%s object_ids=%d usage_rows=%d duration_ms=%d", organization, project, len(objectIDs), len(usageByObjectID), time.Since(usageStart).Milliseconds())

		recordsByChecksum := buildProjectJoinRecordsByChecksum(matchedRecords, usageByObjectID)
		if cache.exact != nil {
			setStart := time.Now()
			err := cache.exact.Set(workCtx, redisKey, storecache.CachedExactJoinState{
				CachedAt:          time.Now(),
				RecordsByChecksum: recordsByChecksum,
				UsageByObjectID:   usageByObjectID,
			}, storecache.StorageExactProjectJoinCacheTTL())
			if err != nil {
				log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=set error=%q", organization, project, cache.exact.Source(), err.Error())
				return projectJoinResult{}, fmt.Errorf("save storage folder exact join to %s cache: %w", cache.exact.Source(), err)
			}
			log.Printf("storage_folder_exact_join_cache_store org=%s project=%s source=%s records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, cache.exact.Source(), len(recordsByChecksum), len(usageByObjectID), time.Since(setStart).Milliseconds())
		}
		return projectJoinResult{RecordsByChecksum: recordsByChecksum, usageByObjectID: usageByObjectID}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if joined {
		log.Printf("storage_folder_exact_join_refresh_join org=%s project=%s duration_ms=%d", organization, project, time.Since(started).Milliseconds())
	}
	return cloneRecordStateMap(result.RecordsByChecksum), storecache.CloneFileUsageMap(result.usageByObjectID), nil
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

func projectAuditKey(organization string, project string) string {
	return strings.TrimSpace(organization) + "\x00" + strings.TrimSpace(project)
}

func projectJoinWorkKey(cacheKey string, checksums map[string]struct{}) string {
	values := make([]string, 0, len(checksums))
	for checksum := range checksums {
		values = append(values, checksum)
	}
	sort.Strings(values)
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return cacheKey + "::inventory::" + hex.EncodeToString(sum[:])
}
