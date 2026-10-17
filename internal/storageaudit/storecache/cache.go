package storecache

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
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultResponseCacheTTL = 5 * time.Minute
	// ProjectBucketInventoryRefreshInterval controls when Gecko asks Syfon for
	// a newer inventory. The cache retains the last successful result longer so
	// an intermittent provider failure does not turn into a user-facing 502.
	ProjectBucketInventoryRefreshInterval = 10 * time.Minute
	ProjectBucketInventoryStaleTTL        = 24 * time.Hour
	ResponseCacheKeyPrefix                = "gecko:storage_chain_audit:v3:"
	ExactJoinCacheKeyPrefix               = "gecko:storage_exact_join:v1:"
	BucketInventoryKeyPrefix              = "gecko:project_bucket_inventory:v2:"
)

type ResponseCache interface {
	Get(ctx context.Context, key string) (CachedResponse, bool, error)
	Set(ctx context.Context, key string, value CachedResponse, ttl time.Duration) error
	Source() string
}

type CachedResponse struct {
	CachedAt              time.Time                `json:"cached_at"`
	RefreshDurationMillis int64                    `json:"refresh_duration_ms,omitempty"`
	Response              model.ChainAuditResponse `json:"response"`
}

type ExactJoinCache interface {
	Get(ctx context.Context, key string) (CachedExactJoinState, bool, error)
	Set(ctx context.Context, key string, value CachedExactJoinState, ttl time.Duration) error
	Source() string
}

type CachedExactJoinState struct {
	CachedAt          time.Time                              `json:"cached_at"`
	RecordsByChecksum map[string][]record.State              `json:"records_by_checksum"`
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage `json:"usage_by_object_id"`
}

type BucketInventoryCache interface {
	Get(ctx context.Context, key string) (CachedBucketInventory, bool, error)
	Set(ctx context.Context, key string, value CachedBucketInventory, ttl time.Duration) error
	Source() string
}

type CachedBucketInventory struct {
	CachedAt time.Time                               `json:"cached_at"`
	Objects  []gintegrationsyfon.ProjectBucketObject `json:"objects"`
	Complete bool                                    `json:"complete"`
	Warning  string                                  `json:"warning,omitempty"`
	Observed string                                  `json:"observed_at,omitempty"`
}

type MemoryResponseCache struct {
	Mu      sync.RWMutex
	Entries map[string]MemoryResponseEntry
}

type MemoryResponseEntry struct {
	Value     CachedResponse
	ExpiresAt time.Time
}

func NewMemoryStorageChainAuditResponseCache() *MemoryResponseCache {
	return &MemoryResponseCache{Entries: map[string]MemoryResponseEntry{}}
}

func (cache *MemoryResponseCache) Get(_ context.Context, key string) (CachedResponse, bool, error) {
	cache.Mu.RLock()
	entry, ok := cache.Entries[key]
	cache.Mu.RUnlock()
	if !ok || time.Now().After(entry.ExpiresAt) {
		if ok {
			cache.Mu.Lock()
			delete(cache.Entries, key)
			cache.Mu.Unlock()
		}
		return CachedResponse{}, false, nil
	}
	return CloneCachedResponse(entry.Value), true, nil
}

func (cache *MemoryResponseCache) Set(_ context.Context, key string, value CachedResponse, ttl time.Duration) error {
	cache.Mu.Lock()
	defer cache.Mu.Unlock()
	cache.Entries[key] = MemoryResponseEntry{
		Value:     CloneCachedResponse(value),
		ExpiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (cache *MemoryResponseCache) Source() string {
	return "memory"
}

type MemoryExactJoinCache struct {
	Mu      sync.RWMutex
	Entries map[string]MemoryExactJoinEntry
}

type MemoryBucketInventoryCache struct {
	Mu      sync.RWMutex
	Entries map[string]MemoryBucketInventoryEntry
}

type MemoryBucketInventoryEntry struct {
	Value     CachedBucketInventory
	ExpiresAt time.Time
}

func NewMemoryProjectBucketInventoryCache() *MemoryBucketInventoryCache {
	return &MemoryBucketInventoryCache{Entries: map[string]MemoryBucketInventoryEntry{}}
}

func (cache *MemoryBucketInventoryCache) Get(_ context.Context, key string) (CachedBucketInventory, bool, error) {
	cache.Mu.RLock()
	entry, ok := cache.Entries[key]
	cache.Mu.RUnlock()
	if !ok || time.Now().After(entry.ExpiresAt) {
		if ok {
			cache.Mu.Lock()
			delete(cache.Entries, key)
			cache.Mu.Unlock()
		}
		return CachedBucketInventory{}, false, nil
	}
	return CloneCachedBucketInventory(entry.Value), true, nil
}

func (cache *MemoryBucketInventoryCache) Set(_ context.Context, key string, value CachedBucketInventory, ttl time.Duration) error {
	cache.Mu.Lock()
	defer cache.Mu.Unlock()
	cache.Entries[key] = MemoryBucketInventoryEntry{Value: CloneCachedBucketInventory(value), ExpiresAt: time.Now().Add(ttl)}
	return nil
}

func (cache *MemoryBucketInventoryCache) Source() string { return "memory" }

type MemoryExactJoinEntry struct {
	Value     CachedExactJoinState
	ExpiresAt time.Time
}

func NewMemoryStorageExactProjectJoinCache() *MemoryExactJoinCache {
	return &MemoryExactJoinCache{Entries: map[string]MemoryExactJoinEntry{}}
}

func (cache *MemoryExactJoinCache) Get(_ context.Context, key string) (CachedExactJoinState, bool, error) {
	cache.Mu.RLock()
	entry, ok := cache.Entries[key]
	cache.Mu.RUnlock()
	if !ok || time.Now().After(entry.ExpiresAt) {
		if ok {
			cache.Mu.Lock()
			delete(cache.Entries, key)
			cache.Mu.Unlock()
		}
		return CachedExactJoinState{}, false, nil
	}
	return CloneCachedExactJoinState(entry.Value), true, nil
}

func (cache *MemoryExactJoinCache) Set(_ context.Context, key string, value CachedExactJoinState, ttl time.Duration) error {
	cache.Mu.Lock()
	defer cache.Mu.Unlock()
	cache.Entries[key] = MemoryExactJoinEntry{
		Value:     CloneCachedExactJoinState(value),
		ExpiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (cache *MemoryExactJoinCache) Source() string {
	return "memory"
}

type RedisResponseCache struct {
	client *redis.Client
}

type RedisExactJoinCache struct {
	client *redis.Client
}

type RedisBucketInventoryCache struct {
	client *redis.Client
}

func NewRedisStorageChainAuditResponseCache(redisURL string) (*RedisResponseCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &RedisResponseCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func NewRedisStorageExactProjectJoinCache(redisURL string) (*RedisExactJoinCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &RedisExactJoinCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func NewRedisProjectBucketInventoryCache(redisURL string) (*RedisBucketInventoryCache, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	cache := &RedisBucketInventoryCache{client: redis.NewClient(options)}
	if err := cache.client.Ping(context.Background()).Err(); err != nil {
		_ = cache.client.Close()
		return nil, err
	}
	return cache, nil
}

func (cache *RedisResponseCache) Get(ctx context.Context, key string) (CachedResponse, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return CachedResponse{}, false, nil
	}
	if err != nil {
		return CachedResponse{}, false, err
	}
	var value CachedResponse
	if err := json.Unmarshal(raw, &value); err != nil {
		return CachedResponse{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return value, true, nil
}

func (cache *RedisResponseCache) Set(ctx context.Context, key string, value CachedResponse, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *RedisResponseCache) Source() string {
	return "redis"
}

func (cache *RedisExactJoinCache) Get(ctx context.Context, key string) (CachedExactJoinState, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return CachedExactJoinState{}, false, nil
	}
	if err != nil {
		return CachedExactJoinState{}, false, err
	}
	var value CachedExactJoinState
	if err := json.Unmarshal(raw, &value); err != nil {
		return CachedExactJoinState{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return value, true, nil
}

func (cache *RedisExactJoinCache) Set(ctx context.Context, key string, value CachedExactJoinState, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *RedisExactJoinCache) Source() string {
	return "redis"
}

func (cache *RedisBucketInventoryCache) Get(ctx context.Context, key string) (CachedBucketInventory, bool, error) {
	raw, err := cache.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return CachedBucketInventory{}, false, nil
	}
	if err != nil {
		return CachedBucketInventory{}, false, err
	}
	var value CachedBucketInventory
	if err := json.Unmarshal(raw, &value); err != nil {
		return CachedBucketInventory{}, false, err
	}
	if value.CachedAt.IsZero() {
		value.CachedAt = time.Now()
	}
	return CloneCachedBucketInventory(value), true, nil
}

func (cache *RedisBucketInventoryCache) Set(ctx context.Context, key string, value CachedBucketInventory, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, ttl).Err()
}

func (cache *RedisBucketInventoryCache) Source() string { return "redis" }

func NewStorageChainAuditResponseCacheFromEnv() ResponseCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisStorageChainAuditResponseCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryStorageChainAuditResponseCache()
}

func NewStorageExactProjectJoinCacheFromEnv() ExactJoinCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisStorageExactProjectJoinCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryStorageExactProjectJoinCache()
}

func NewProjectBucketInventoryCacheFromEnv() BucketInventoryCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisProjectBucketInventoryCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryProjectBucketInventoryCache()
}

func StorageChainAuditRedisURLFromEnv() string {
	redisURL := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL"))
	if redisURL == "" {
		redisURL = strings.TrimSpace(os.Getenv("REDIS_URL"))
	}
	return RedisURLWithPassword(redisURL, strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD")))
}

func RedisURLWithPassword(redisURL string, password string) string {
	redisURL = strings.TrimSpace(redisURL)
	password = strings.TrimSpace(password)
	if redisURL == "" || password == "" || !strings.HasPrefix(redisURL, "redis://") || strings.Contains(redisURL, "@") {
		return redisURL
	}
	return "redis://:" + password + "@" + strings.TrimPrefix(redisURL, "redis://")
}

func StorageChainAuditCacheEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	return err == nil && enabled
}

func StorageChainAuditCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS"))
	if raw == "" {
		return DefaultResponseCacheTTL
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 60 {
		return DefaultResponseCacheTTL
	}
	return time.Duration(seconds) * time.Second
}

func StorageChainAuditResponseCacheKey(organization string, project string, ref string, gitSubpath string, probeMode string, validationMode string, bucketMode string, bucketPathPrefix string, hash string, syfonRevision string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nref=%s\nhash=%s\nsyfon_revision=%s\ngit_subpath=%s\nprobe_mode=%s\nvalidation_mode=%s\nbucket_mode=%s\nbucket_path_prefix=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(ref),
		strings.TrimSpace(hash),
		strings.TrimSpace(syfonRevision),
		record.NormalizeRepoSubpath(gitSubpath),
		strings.TrimSpace(probeMode),
		strings.TrimSpace(validationMode),
		strings.TrimSpace(bucketMode),
		record.NormalizeRepoSubpath(bucketPathPrefix),
	)
	sum := sha256.Sum256([]byte(body))
	return ResponseCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func StorageExactProjectJoinCacheKey(organization string, project string, hash string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nhash=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(hash),
	)
	sum := sha256.Sum256([]byte(body))
	return ExactJoinCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func ProjectBucketInventoryCacheKey(organization string, project string, bucketPathPrefix string) string {
	body := fmt.Sprintf("org=%s\nproject=%s\nbucket_path_prefix=%s", strings.TrimSpace(organization), strings.TrimSpace(project), record.NormalizeRepoSubpath(bucketPathPrefix))
	sum := sha256.Sum256([]byte(body))
	return BucketInventoryKeyPrefix + hex.EncodeToString(sum[:])
}

func CloneCachedResponse(value CachedResponse) CachedResponse {
	return CachedResponse{
		CachedAt:              value.CachedAt,
		RefreshDurationMillis: value.RefreshDurationMillis,
		Response:              CloneResponse(value.Response),
	}
}

func CloneCachedBucketInventory(value CachedBucketInventory) CachedBucketInventory {
	return CachedBucketInventory{
		CachedAt: value.CachedAt,
		Objects:  append([]gintegrationsyfon.ProjectBucketObject(nil), value.Objects...),
		Complete: value.Complete,
		Warning:  value.Warning,
		Observed: value.Observed,
	}
}

func CloneResponse(response model.ChainAuditResponse) model.ChainAuditResponse {
	response.Findings = append([]model.ChainFinding(nil), response.Findings...)
	response.Groups = append([]model.ChainIssueGroup(nil), response.Groups...)
	response.Summary.CountsByKind = CloneStringIntMap(response.Summary.CountsByKind)
	return response
}

func CloneCachedExactJoinState(value CachedExactJoinState) CachedExactJoinState {
	return CachedExactJoinState{
		CachedAt:          value.CachedAt,
		RecordsByChecksum: cloneRecordStateMap(value.RecordsByChecksum),
		UsageByObjectID:   CloneFileUsageMap(value.UsageByObjectID),
	}
}

func CloneFileUsageMap(input map[string]gintegrationsyfon.FileUsage) map[string]gintegrationsyfon.FileUsage {
	if input == nil {
		return nil
	}
	out := make(map[string]gintegrationsyfon.FileUsage, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func CloneStringIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	out := make(map[string]int, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneRecordStateMap(input map[string][]record.State) map[string][]record.State {
	if input == nil {
		return nil
	}
	out := make(map[string][]record.State, len(input))
	for checksum, group := range input {
		states := make([]record.State, 0, len(group))
		for _, state := range group {
			clone := state
			clone.CanonicalAccessURLs = append([]string(nil), state.CanonicalAccessURLs...)
			clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), state.AccessProbes...)
			states = append(states, clone)
		}
		out[checksum] = states
	}
	return out
}
