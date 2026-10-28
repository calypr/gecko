package storageaudit

import (
	"context"
	"fmt"
	"sync"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

type testResponseCacheEntry struct {
	value storecache.CachedResponse
}

type testResponseCache struct {
	mu      sync.RWMutex
	entries map[string]testResponseCacheEntry
	getErr  error
	setErr  error
}

func newTestResponseCache() *testResponseCache {
	return &testResponseCache{entries: map[string]testResponseCacheEntry{}}
}

func (cache *testResponseCache) Get(_ context.Context, key string) (storecache.CachedResponse, bool, error) {
	cache.mu.RLock()
	if cache.getErr != nil {
		err := cache.getErr
		cache.mu.RUnlock()
		return storecache.CachedResponse{}, false, err
	}
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok {
		return storecache.CachedResponse{}, false, nil
	}
	return storecache.CloneCachedResponse(entry.value), true, nil
}

func (cache *testResponseCache) Set(_ context.Context, key string, value storecache.CachedResponse) error {
	cache.mu.Lock()
	if cache.setErr != nil {
		err := cache.setErr
		cache.mu.Unlock()
		return err
	}
	cache.entries[key] = testResponseCacheEntry{value: storecache.CloneCachedResponse(value)}
	cache.mu.Unlock()
	return nil
}

func (cache *testResponseCache) Source() string { return "test" }

func (cache *testResponseCache) setRefreshDuration(duration int64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for key, entry := range cache.entries {
		entry.value.RefreshDurationMillis = duration
		cache.entries[key] = entry
	}
}

func (cache *testResponseCache) setCachedAt(key string, cachedAt time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[key]
	entry.value.CachedAt = cachedAt
	cache.entries[key] = entry
}

func (cache *testResponseCache) count() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.entries)
}

type testProjectAuditCoordinator struct {
	response storecache.ResponseCache
	bucket   storecache.BucketInventoryCache
}

type testProjectAuditLease struct {
	ctx      context.Context
	response storecache.ResponseCache
	bucket   storecache.BucketInventoryCache
}

func newTestProjectAuditCoordinator(response storecache.ResponseCache, bucket storecache.BucketInventoryCache) storecache.ProjectAuditCoordinator {
	return testProjectAuditCoordinator{response: response, bucket: bucket}
}

func (coordinator testProjectAuditCoordinator) AcquireProject(ctx context.Context, _, _ string) (storecache.ProjectAuditLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return testProjectAuditLease{ctx: ctx, response: coordinator.response, bucket: coordinator.bucket}, nil
}

func (lease testProjectAuditLease) Context() context.Context { return lease.ctx }

func (lease testProjectAuditLease) PublishResponse(ctx context.Context, key string, value storecache.CachedResponse) error {
	return lease.response.Set(ctx, key, value)
}

func (lease testProjectAuditLease) PublishBucketInventory(ctx context.Context, key string, value storecache.CachedBucketInventory, ttl time.Duration) error {
	if lease.bucket == nil {
		return nil
	}
	return lease.bucket.Set(ctx, key, value, ttl)
}

func (testProjectAuditLease) Release(context.Context) error { return nil }

func configureTestAuditResponseCache(service *StorageAnalyticsService, response storecache.ResponseCache) {
	service.chainSvc.ChainAuditResponseCache = response
	service.chainSvc.ProjectAuditCoordinator = newTestProjectAuditCoordinator(response, service.chainSvc.ProjectBucketCache)
}

func configureTestProjectBucketCache(service *StorageAnalyticsService, bucket storecache.BucketInventoryCache) {
	service.chainSvc.ProjectBucketCache = bucket
	if response := service.chainSvc.ChainAuditResponseCache; response != nil {
		service.chainSvc.ProjectAuditCoordinator = newTestProjectAuditCoordinator(response, bucket)
	}
}

type testBucketCacheEntry struct {
	value     storecache.CachedBucketInventory
	expiresAt time.Time
}

type testBucketCache struct {
	mu      sync.RWMutex
	entries map[string]testBucketCacheEntry
	getErr  error
	setErr  error
}

func newTestBucketCache() *testBucketCache {
	return &testBucketCache{entries: map[string]testBucketCacheEntry{}}
}

func (cache *testBucketCache) Get(_ context.Context, key string) (storecache.CachedBucketInventory, bool, error) {
	cache.mu.RLock()
	if cache.getErr != nil {
		err := cache.getErr
		cache.mu.RUnlock()
		return storecache.CachedBucketInventory{}, false, err
	}
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			cache.mu.Lock()
			delete(cache.entries, key)
			cache.mu.Unlock()
		}
		return storecache.CachedBucketInventory{}, false, nil
	}
	return storecache.CloneCachedBucketInventory(entry.value), true, nil
}

func (cache *testBucketCache) Set(_ context.Context, key string, value storecache.CachedBucketInventory, ttl time.Duration) error {
	cache.mu.Lock()
	if cache.setErr != nil {
		err := cache.setErr
		cache.mu.Unlock()
		return err
	}
	cache.entries[key] = testBucketCacheEntry{value: storecache.CloneCachedBucketInventory(value), expiresAt: time.Now().Add(ttl)}
	cache.mu.Unlock()
	return nil
}

func (cache *testBucketCache) Source() string { return "test" }

func (cache *testBucketCache) setCachedAt(key string, cachedAt time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[key]
	entry.value.CachedAt = cachedAt
	cache.entries[key] = entry
}

type testExactJoinCacheEntry struct {
	value     storecache.CachedExactJoinState
	expiresAt time.Time
}

type testExactJoinCache struct {
	mu      sync.RWMutex
	entries map[string]testExactJoinCacheEntry
	getErr  error
	setErr  error
}

func newTestExactJoinCache() *testExactJoinCache {
	return &testExactJoinCache{entries: map[string]testExactJoinCacheEntry{}}
}

func (cache *testExactJoinCache) Get(_ context.Context, key string) (storecache.CachedExactJoinState, bool, error) {
	cache.mu.RLock()
	if cache.getErr != nil {
		err := cache.getErr
		cache.mu.RUnlock()
		return storecache.CachedExactJoinState{}, false, err
	}
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return storecache.CachedExactJoinState{}, false, nil
	}
	return storecache.CloneCachedExactJoinState(entry.value), true, nil
}

func (cache *testExactJoinCache) Set(_ context.Context, key string, value storecache.CachedExactJoinState, ttl time.Duration) error {
	cache.mu.Lock()
	if cache.setErr != nil {
		err := cache.setErr
		cache.mu.Unlock()
		return err
	}
	cache.entries[key] = testExactJoinCacheEntry{value: storecache.CloneCachedExactJoinState(value), expiresAt: time.Now().Add(ttl)}
	cache.mu.Unlock()
	return nil
}

func (cache *testExactJoinCache) Source() string { return "test" }

type testObservationCache struct {
	mu       sync.RWMutex
	entries  map[string]storecache.CachedStorageObservation
	versions map[string]storecache.StorageObservationVersion
	getErr   error
	setErr   error
}

func newTestObservationCache() *testObservationCache {
	return &testObservationCache{entries: map[string]storecache.CachedStorageObservation{}, versions: map[string]storecache.StorageObservationVersion{}}
}

func (cache *testObservationCache) GetMany(_ context.Context, _ string, _ string, _ storecache.StorageObservationVersion, keys []string) (map[string]storecache.CachedStorageObservation, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if cache.getErr != nil {
		return nil, cache.getErr
	}
	out := make(map[string]storecache.CachedStorageObservation, len(keys))
	for _, key := range keys {
		if value, ok := cache.entries[key]; ok {
			out[key] = cloneCachedStorageObservation(value)
		}
	}
	return out, nil
}

func (cache *testObservationCache) EnsureFences(_ context.Context, organization string, project string, version storecache.StorageObservationVersion, keys []string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.versions[fmt.Sprintf("%s/%s", organization, project)].Epoch != version.Epoch {
		return fmt.Errorf("storage observation version changed")
	}
	currentVersion := cache.versions[fmt.Sprintf("%s/%s", organization, project)]
	for _, key := range keys {
		current, ok := cache.entries[key]
		if !ok || current.Version.Epoch != version.Epoch {
			if currentVersion.Counter != version.Counter {
				return fmt.Errorf("stale storage observation fence initializer")
			}
			counter := version.Counter - 1
			cache.entries[key] = storecache.CachedStorageObservation{Version: storecache.StorageObservationVersion{Epoch: version.Epoch, Counter: counter}, Retired: true}
		}
	}
	return nil
}

func (cache *testObservationCache) SetMany(_ context.Context, organization string, project string, writes []storecache.StorageObservationWrite) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.setErr != nil {
		return cache.setErr
	}
	currentVersion := cache.versions[fmt.Sprintf("%s/%s", organization, project)]
	for _, write := range writes {
		key := write.Key
		value := write.Observation
		if value.Version.Epoch != currentVersion.Epoch {
			return fmt.Errorf("storage observation version changed")
		}
		fence, ok := cache.entries[write.FenceKey]
		if !ok || !fence.Retired || fence.Version.Epoch != value.Version.Epoch {
			return fmt.Errorf("storage observation fence missing or changed")
		}
		if value.Retired {
			if value.Version.Counter >= fence.Version.Counter {
				cache.entries[key] = cloneCachedStorageObservation(value)
			}
			continue
		}
		if value.Version.Counter <= fence.Version.Counter {
			continue
		}
		current, ok := cache.entries[key]
		if ok && current.Version.Epoch == value.Version.Epoch && current.Version.Counter > value.Version.Counter {
			continue
		}
		if ok && current.Version.Epoch == value.Version.Epoch && current.Version.Counter == value.Version.Counter && current.Retired && !value.Retired {
			continue
		}
		cache.entries[key] = cloneCachedStorageObservation(value)
	}
	return nil
}

func (cache *testObservationCache) NextVersion(_ context.Context, organization string, project string) (storecache.StorageObservationVersion, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	key := fmt.Sprintf("%s/%s", organization, project)
	version := cache.versions[key]
	if version.Epoch == "" {
		version.Epoch = "test-epoch"
	}
	version.Counter++
	cache.versions[key] = version
	return version, nil
}

func (cache *testObservationCache) Source() string { return "test" }

func cloneCachedStorageObservation(value storecache.CachedStorageObservation) storecache.CachedStorageObservation {
	value.Result = cloneStorageProbeResult(value.Result)
	return value
}

func cloneStorageProbeResult(result gintegrationsyfon.BulkStorageProbeResult) gintegrationsyfon.BulkStorageProbeResult {
	if result.SizeBytes != nil {
		value := *result.SizeBytes
		result.SizeBytes = &value
	}
	if result.SizeMatch != nil {
		value := *result.SizeMatch
		result.SizeMatch = &value
	}
	if result.NameMatch != nil {
		value := *result.NameMatch
		result.NameMatch = &value
	}
	if result.SHA256Match != nil {
		value := *result.SHA256Match
		result.SHA256Match = &value
	}
	result.ValidationMismatches = append([]string(nil), result.ValidationMismatches...)
	return result
}

func (cache *testExactJoinCache) failGet(err error) {
	cache.mu.Lock()
	cache.getErr = err
	cache.mu.Unlock()
}

func (cache *testExactJoinCache) failSet(err error) {
	cache.mu.Lock()
	cache.setErr = err
	cache.mu.Unlock()
}

var _ storecache.ResponseCache = (*testResponseCache)(nil)
var _ storecache.ExactJoinCache = (*testExactJoinCache)(nil)
var _ storecache.BucketInventoryCache = (*testBucketCache)(nil)
