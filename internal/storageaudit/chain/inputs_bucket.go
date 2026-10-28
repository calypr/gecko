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
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

func (service *Service) loadProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	inventory, err := service.deps.Buckets.ListProjectBucketObjects(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, sarecord.BucketInventoryMetadata{}, fmt.Errorf("list syfon project bucket objects: %w", err)
	}
	objects, lookup := buildBucketObjectLookup(inventory.Objects)
	metadata := sarecord.BucketInventoryMetadata{Complete: inventory.Complete, Warning: strings.TrimSpace(inventory.Warning), Observed: strings.TrimSpace(inventory.ObservedAt), Source: "live"}
	return objects, lookup, metadata, nil
}

func (service *Service) loadProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	inventory, err := service.deps.Buckets.ListProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
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

func (service *Service) loadCachedProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string, forceRefresh bool) ([]domain.StorageBucketScope, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project)
	service.chainInputMu.RLock()
	cached, ok := service.projectScopeCache[cacheKey]
	service.chainInputMu.RUnlock()
	if !forceRefresh && ok && time.Now().Before(cached.expiresAt) && cached.scopes != nil {
		return append([]domain.StorageBucketScope(nil), cached.scopes...), nil
	}
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	service.chainInputMu.Lock()
	service.projectScopeCache[cacheKey] = cachedProjectScopeState{expiresAt: time.Now().Add(projectScopeCacheTTL), scopes: append([]domain.StorageBucketScope(nil), scopes...)}
	service.chainInputMu.Unlock()
	return scopes, nil
}

func (service *Service) loadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	return service.loadCachedProjectBucketValidationInventoryWithLease(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceRefresh, nil)
}

func (service *Service) loadCachedProjectBucketValidationInventoryWithLease(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool, lease storecache.ProjectAuditLease) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, sarecord.BucketInventoryMetadata, error) {
	var stale storecache.CachedBucketInventory
	var hasStale bool
	if cache := service.ProjectBucketCache; cache != nil {
		redisKey := storecache.ProjectBucketInventoryCacheKey(organization, project, bucketPathPrefix)
		cached, ok, err := cache.Get(ctx, redisKey)
		switch {
		case err != nil:
			log.Printf("syfon_project_bucket_inventory_cache_error org=%s project=%s path_prefix=%q source=%s operation=get error=%q", organization, project, bucketPathPrefix, cache.Source(), err.Error())
			return nil, nil, sarecord.BucketInventoryMetadata{}, storecache.WrapCacheOperationError(cache.Source(), "get", err)
		case ok && !forceRefresh && time.Since(cached.CachedAt) < storecache.ProjectBucketInventoryRefreshInterval:
			objects, lookup := buildBucketObjectLookup(cached.Objects)
			log.Printf("syfon_project_bucket_inventory_cache_hit org=%s project=%s path_prefix=%q source=%s object_count=%d age_seconds=%d", organization, project, bucketPathPrefix, cache.Source(), len(objects), int64(time.Since(cached.CachedAt).Seconds()))
			return objects, lookup, sarecord.BucketInventoryMetadata{Complete: cached.Complete, Warning: cached.Warning, Observed: cached.Observed, Source: cache.Source()}, nil
		case ok:
			stale = cached
			hasStale = true
		}
	}
	bucketObjects, bucketObjectsByURL, metadata, err := service.loadProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		if forceRefresh {
			return nil, nil, sarecord.BucketInventoryMetadata{Complete: false, Warning: err.Error(), Source: "live"}, err
		}
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
	if cache := service.ProjectBucketCache; cache != nil && metadata.Complete {
		redisKey := storecache.ProjectBucketInventoryCacheKey(organization, project, bucketPathPrefix)
		value := storecache.CachedBucketInventory{CachedAt: time.Now(), Objects: append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...), Complete: metadata.Complete, Warning: metadata.Warning, Observed: metadata.Observed}
		var err error
		if lease != nil {
			err = lease.PublishBucketInventory(ctx, redisKey, value, storecache.ProjectBucketInventoryStaleTTL)
		} else {
			err = cache.Set(ctx, redisKey, value, storecache.ProjectBucketInventoryStaleTTL)
		}
		if err != nil {
			log.Printf("syfon_project_bucket_inventory_cache_error org=%s project=%s path_prefix=%q source=%s operation=set error=%q", organization, project, bucketPathPrefix, cache.Source(), err.Error())
			return nil, nil, metadata, storecache.WrapCacheOperationError(cache.Source(), "set", err)
		} else {
			log.Printf("syfon_project_bucket_inventory_cache_store org=%s project=%s path_prefix=%q source=%s object_count=%d refresh_interval_seconds=%d stale_ttl_seconds=%d", organization, project, bucketPathPrefix, cache.Source(), len(bucketObjects), int64(storecache.ProjectBucketInventoryRefreshInterval.Seconds()), int64(storecache.ProjectBucketInventoryStaleTTL.Seconds()))
		}
	}
	objects, lookup := cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	return objects, lookup, metadata, nil
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
					SizeBytes:    sarecord.DerefInt64(probe.SizeBytes),
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
