package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/redis/go-redis/v9"
)

const (
	defaultStorageChainAuditCacheTTL = 5 * time.Minute
	// projectBucketInventoryRefreshInterval controls when Gecko asks Syfon for
	// a newer inventory. The cache retains the last successful result longer so
	// an intermittent provider failure does not turn into a user-facing 502.
	projectBucketInventoryRefreshInterval = 10 * time.Minute
	projectBucketInventoryStaleTTL        = 24 * time.Hour
	storageChainAuditCacheKeyPrefix       = "gecko:storage_chain_audit:v3:"
	storageExactJoinCacheKeyPrefix        = "gecko:storage_exact_join:v1:"
	projectBucketInventoryKeyPrefix       = "gecko:project_bucket_inventory:v2:"
)

type storageChainAuditResponseCache interface {
	Get(ctx context.Context, key string) (cachedStorageChainAuditResponse, bool, error)
	Set(ctx context.Context, key string, value cachedStorageChainAuditResponse, ttl time.Duration) error
	Source() string
}

type cachedStorageChainAuditResponse struct {
	CachedAt              time.Time                    `json:"cached_at"`
	RefreshDurationMillis int64                        `json:"refresh_duration_ms,omitempty"`
	Response              GitStorageChainAuditResponse `json:"response"`
}

type inflightStorageChainAuditRefresh struct {
	done               chan struct{}
	forceBucketRefresh bool
	value              cachedStorageChainAuditResponse
	err                error
}

// coalesceStorageChainAuditRefresh ensures a root audit has one authoritative
// refresh result per cache key. The UI can issue overlapping forced refreshes;
// without this guard, a partial bucket LIST from the later request can replace
// the completed response from the earlier request.
func (service *StorageAnalyticsService) coalesceStorageChainAuditRefresh(ctx context.Context, key string, forceBucketRefresh bool, refresh func() (cachedStorageChainAuditResponse, error)) (cachedStorageChainAuditResponse, bool, error) {
	var work *inflightStorageChainAuditRefresh
	for {
		service.chainAuditRefreshMu.Lock()
		work = service.chainAuditRefreshWork[key]
		if work == nil {
			work = &inflightStorageChainAuditRefresh{done: make(chan struct{}), forceBucketRefresh: forceBucketRefresh}
			service.chainAuditRefreshWork[key] = work
			service.chainAuditRefreshMu.Unlock()
			break
		}
		service.chainAuditRefreshMu.Unlock()
		select {
		case <-ctx.Done():
			return cachedStorageChainAuditResponse{}, true, ctx.Err()
		case <-work.done:
			if !forceBucketRefresh || work.forceBucketRefresh {
				return cloneCachedStorageChainAuditResponse(work.value), true, work.err
			}
		}
	}

	value, err := refresh()
	service.chainAuditRefreshMu.Lock()
	work.value = cloneCachedStorageChainAuditResponse(value)
	work.err = err
	delete(service.chainAuditRefreshWork, key)
	close(work.done)
	service.chainAuditRefreshMu.Unlock()
	return value, false, err
}

type storageExactProjectJoinCache interface {
	Get(ctx context.Context, key string) (cachedExactProjectJoinState, bool, error)
	Set(ctx context.Context, key string, value cachedExactProjectJoinState, ttl time.Duration) error
	Source() string
}

type cachedExactProjectJoinState struct {
	CachedAt          time.Time                              `json:"cached_at"`
	RecordsByChecksum map[string][]projectRecordState        `json:"records_by_checksum"`
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage `json:"usage_by_object_id"`
}

type projectBucketInventoryCache interface {
	Get(ctx context.Context, key string) (cachedProjectBucketInventory, bool, error)
	Set(ctx context.Context, key string, value cachedProjectBucketInventory, ttl time.Duration) error
	Source() string
}

type cachedProjectBucketInventory struct {
	CachedAt time.Time                               `json:"cached_at"`
	Objects  []gintegrationsyfon.ProjectBucketObject `json:"objects"`
	Complete bool                                    `json:"complete"`
	Warning  string                                  `json:"warning,omitempty"`
	Observed string                                  `json:"observed_at,omitempty"`
}

type memoryStorageChainAuditResponseCache struct {
	mu      sync.RWMutex
	entries map[string]memoryStorageChainAuditResponseEntry
}

type memoryStorageChainAuditResponseEntry struct {
	value     cachedStorageChainAuditResponse
	expiresAt time.Time
}

func newMemoryStorageChainAuditResponseCache() *memoryStorageChainAuditResponseCache {
	return &memoryStorageChainAuditResponseCache{entries: map[string]memoryStorageChainAuditResponseEntry{}}
}

func (cache *memoryStorageChainAuditResponseCache) Get(_ context.Context, key string) (cachedStorageChainAuditResponse, bool, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			cache.mu.Lock()
			delete(cache.entries, key)
			cache.mu.Unlock()
		}
		return cachedStorageChainAuditResponse{}, false, nil
	}
	return cloneCachedStorageChainAuditResponse(entry.value), true, nil
}

func (cache *memoryStorageChainAuditResponseCache) Set(_ context.Context, key string, value cachedStorageChainAuditResponse, ttl time.Duration) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries[key] = memoryStorageChainAuditResponseEntry{
		value:     cloneCachedStorageChainAuditResponse(value),
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (cache *memoryStorageChainAuditResponseCache) Source() string {
	return "memory"
}

type memoryStorageExactProjectJoinCache struct {
	mu      sync.RWMutex
	entries map[string]memoryStorageExactProjectJoinEntry
}

type memoryProjectBucketInventoryCache struct {
	mu      sync.RWMutex
	entries map[string]memoryProjectBucketInventoryEntry
}

type memoryProjectBucketInventoryEntry struct {
	value     cachedProjectBucketInventory
	expiresAt time.Time
}

func newMemoryProjectBucketInventoryCache() *memoryProjectBucketInventoryCache {
	return &memoryProjectBucketInventoryCache{entries: map[string]memoryProjectBucketInventoryEntry{}}
}

func (cache *memoryProjectBucketInventoryCache) Get(_ context.Context, key string) (cachedProjectBucketInventory, bool, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			cache.mu.Lock()
			delete(cache.entries, key)
			cache.mu.Unlock()
		}
		return cachedProjectBucketInventory{}, false, nil
	}
	return cloneCachedProjectBucketInventory(entry.value), true, nil
}

func (cache *memoryProjectBucketInventoryCache) Set(_ context.Context, key string, value cachedProjectBucketInventory, ttl time.Duration) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries[key] = memoryProjectBucketInventoryEntry{value: cloneCachedProjectBucketInventory(value), expiresAt: time.Now().Add(ttl)}
	return nil
}

func (cache *memoryProjectBucketInventoryCache) Source() string { return "memory" }

type memoryStorageExactProjectJoinEntry struct {
	value     cachedExactProjectJoinState
	expiresAt time.Time
}

func newMemoryStorageExactProjectJoinCache() *memoryStorageExactProjectJoinCache {
	return &memoryStorageExactProjectJoinCache{entries: map[string]memoryStorageExactProjectJoinEntry{}}
}

func (cache *memoryStorageExactProjectJoinCache) Get(_ context.Context, key string) (cachedExactProjectJoinState, bool, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			cache.mu.Lock()
			delete(cache.entries, key)
			cache.mu.Unlock()
		}
		return cachedExactProjectJoinState{}, false, nil
	}
	return cloneCachedExactProjectJoinState(entry.value), true, nil
}

func (cache *memoryStorageExactProjectJoinCache) Set(_ context.Context, key string, value cachedExactProjectJoinState, ttl time.Duration) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries[key] = memoryStorageExactProjectJoinEntry{
		value:     cloneCachedExactProjectJoinState(value),
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (cache *memoryStorageExactProjectJoinCache) Source() string {
	return "memory"
}

type redisStorageChainAuditResponseCache struct {
	client *redis.Client
}

type redisStorageExactProjectJoinCache struct {
	client *redis.Client
}

type redisProjectBucketInventoryCache struct {
	client *redis.Client
}

func newRedisStorageChainAuditResponseCache(redisURL string) (*redisStorageChainAuditResponseCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &redisStorageChainAuditResponseCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func newRedisStorageExactProjectJoinCache(redisURL string) (*redisStorageExactProjectJoinCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &redisStorageExactProjectJoinCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func newRedisProjectBucketInventoryCache(redisURL string) (*redisProjectBucketInventoryCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &redisProjectBucketInventoryCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func (cache *redisStorageChainAuditResponseCache) Get(ctx context.Context, key string) (cachedStorageChainAuditResponse, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return cachedStorageChainAuditResponse{}, false, nil
	}
	if err != nil {
		return cachedStorageChainAuditResponse{}, false, err
	}
	var value cachedStorageChainAuditResponse
	if err := json.Unmarshal(raw, &value); err != nil {
		return cachedStorageChainAuditResponse{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return value, true, nil
}

func (cache *redisStorageChainAuditResponseCache) Set(ctx context.Context, key string, value cachedStorageChainAuditResponse, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *redisStorageChainAuditResponseCache) Source() string {
	return "redis"
}

func (cache *redisStorageExactProjectJoinCache) Get(ctx context.Context, key string) (cachedExactProjectJoinState, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return cachedExactProjectJoinState{}, false, nil
	}
	if err != nil {
		return cachedExactProjectJoinState{}, false, err
	}
	var value cachedExactProjectJoinState
	if err := json.Unmarshal(raw, &value); err != nil {
		return cachedExactProjectJoinState{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return value, true, nil
}

func (cache *redisStorageExactProjectJoinCache) Set(ctx context.Context, key string, value cachedExactProjectJoinState, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *redisStorageExactProjectJoinCache) Source() string {
	return "redis"
}

func (cache *redisProjectBucketInventoryCache) Get(ctx context.Context, key string) (cachedProjectBucketInventory, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return cachedProjectBucketInventory{}, false, nil
	}
	if err != nil {
		return cachedProjectBucketInventory{}, false, err
	}
	var value cachedProjectBucketInventory
	if err := json.Unmarshal(raw, &value); err != nil {
		return cachedProjectBucketInventory{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return cloneCachedProjectBucketInventory(value), true, nil
}

func (cache *redisProjectBucketInventoryCache) Set(ctx context.Context, key string, value cachedProjectBucketInventory, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *redisProjectBucketInventoryCache) Source() string { return "redis" }

func NewStorageChainAuditResponseCacheFromEnv() storageChainAuditResponseCache {
	if !storageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := storageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := newRedisStorageChainAuditResponseCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return newMemoryStorageChainAuditResponseCache()
}

func NewStorageExactProjectJoinCacheFromEnv() storageExactProjectJoinCache {
	if !storageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := storageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := newRedisStorageExactProjectJoinCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return newMemoryStorageExactProjectJoinCache()
}

func NewProjectBucketInventoryCacheFromEnv() projectBucketInventoryCache {
	if !storageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := storageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := newRedisProjectBucketInventoryCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return newMemoryProjectBucketInventoryCache()
}

func storageChainAuditRedisURLFromEnv() string {
	redisURL := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL"))
	if redisURL == "" {
		redisURL = strings.TrimSpace(os.Getenv("REDIS_URL"))
	}
	return redisURLWithPassword(redisURL, strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD")))
}

func redisURLWithPassword(redisURL string, password string) string {
	redisURL = strings.TrimSpace(redisURL)
	password = strings.TrimSpace(password)
	if redisURL == "" || password == "" || !strings.HasPrefix(redisURL, "redis://") || strings.Contains(redisURL, "@") {
		return redisURL
	}
	return "redis://:" + password + "@" + strings.TrimPrefix(redisURL, "redis://")
}

func storageChainAuditCacheEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	return err == nil && enabled
}

func storageChainAuditCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS"))
	if raw == "" {
		return defaultStorageChainAuditCacheTTL
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 60 {
		return defaultStorageChainAuditCacheTTL
	}
	return time.Duration(seconds) * time.Second
}

func storageChainAuditResponseCacheKey(organization string, project string, ref string, gitSubpath string, probeMode string, validationMode string, bucketMode string, bucketPathPrefix string, hash string, syfonRevision string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nref=%s\nhash=%s\nsyfon_revision=%s\ngit_subpath=%s\nprobe_mode=%s\nvalidation_mode=%s\nbucket_mode=%s\nbucket_path_prefix=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(ref),
		strings.TrimSpace(hash),
		strings.TrimSpace(syfonRevision),
		normalizeRepoSubpath(gitSubpath),
		strings.TrimSpace(probeMode),
		strings.TrimSpace(validationMode),
		strings.TrimSpace(bucketMode),
		normalizeRepoSubpath(bucketPathPrefix),
	)
	sum := sha256.Sum256([]byte(body))
	return storageChainAuditCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func storageExactProjectJoinCacheKey(organization string, project string, hash string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nhash=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(hash),
	)
	sum := sha256.Sum256([]byte(body))
	return storageExactJoinCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func projectBucketInventoryCacheKey(organization string, project string, bucketPathPrefix string) string {
	body := fmt.Sprintf("org=%s\nproject=%s\nbucket_path_prefix=%s", strings.TrimSpace(organization), strings.TrimSpace(project), normalizeRepoSubpath(bucketPathPrefix))
	sum := sha256.Sum256([]byte(body))
	return projectBucketInventoryKeyPrefix + hex.EncodeToString(sum[:])
}

func cloneCachedStorageChainAuditResponse(value cachedStorageChainAuditResponse) cachedStorageChainAuditResponse {
	return cachedStorageChainAuditResponse{
		CachedAt:              value.CachedAt,
		RefreshDurationMillis: value.RefreshDurationMillis,
		Response:              cloneStorageChainAuditResponse(value.Response),
	}
}

func cloneCachedProjectBucketInventory(value cachedProjectBucketInventory) cachedProjectBucketInventory {
	return cachedProjectBucketInventory{
		CachedAt: value.CachedAt,
		Objects:  append([]gintegrationsyfon.ProjectBucketObject(nil), value.Objects...),
		Complete: value.Complete,
		Warning:  value.Warning,
		Observed: value.Observed,
	}
}

func cloneStorageChainAuditResponse(response GitStorageChainAuditResponse) GitStorageChainAuditResponse {
	response.Findings = append([]GitStorageChainFinding(nil), response.Findings...)
	response.Groups = append([]GitStorageChainIssueGroup(nil), response.Groups...)
	response.Summary.CountsByKind = cloneStringIntMap(response.Summary.CountsByKind)
	return response
}

func cloneCachedExactProjectJoinState(value cachedExactProjectJoinState) cachedExactProjectJoinState {
	return cachedExactProjectJoinState{
		CachedAt:          value.CachedAt,
		RecordsByChecksum: cloneRecordStateMap(value.RecordsByChecksum),
		UsageByObjectID:   cloneFileUsageMap(value.UsageByObjectID),
	}
}

func cloneFileUsageMap(input map[string]gintegrationsyfon.FileUsage) map[string]gintegrationsyfon.FileUsage {
	if input == nil {
		return nil
	}
	out := make(map[string]gintegrationsyfon.FileUsage, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneStringIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	out := make(map[string]int, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
