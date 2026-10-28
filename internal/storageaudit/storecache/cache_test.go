package storecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/redis/go-redis/v9"
)

func TestStorageChainAuditResponseCacheKeyIgnoresChangingRevisionsAndIsolatesScope(t *testing.T) {
	first := StorageChainAuditResponseCacheKey("org", "proj", "main", "data", "inventory_only", "inventory", "validate", "")
	repeated := StorageChainAuditResponseCacheKey("org", "proj", "main", "data", "inventory_only", "inventory", "validate", "")
	if first != repeated {
		t.Fatalf("expected the same organization/project/ref/path/modes to reuse the saved snapshot key, got %q != %q", first, repeated)
	}
	otherProject := StorageChainAuditResponseCacheKey("org", "other-project", "main", "data", "inventory_only", "inventory", "validate", "")
	if first == otherProject {
		t.Fatalf("expected project scope to isolate response snapshots, got %q", first)
	}
}

func TestStorageObservationCacheKeyUsesTargetInputsAndInventoryIdentity(t *testing.T) {
	key := StorageObservationCacheKey("org", "proj", "list", "s3://bucket/a", "12", "", "a", "scope", "absent")
	ambiguousLeft := StorageObservationCacheKey("org\nproject=proj", "x", "list", "s3://bucket/a", "12", "", "a", "scope", "absent")
	ambiguousRight := StorageObservationCacheKey("org", "project=proj\nx", "list", "s3://bucket/a", "12", "", "a", "scope", "absent")
	if ambiguousLeft == ambiguousRight {
		t.Fatal("structured observation identity collided on embedded separators")
	}
	for _, changed := range []string{
		StorageObservationCacheKey("other", "proj", "list", "s3://bucket/a", "12", "", "a", "scope", "absent"),
		StorageObservationCacheKey("org", "proj", "metadata", "s3://bucket/a", "12", "", "a", "scope", "absent"),
		StorageObservationCacheKey("org", "proj", "list", "s3://bucket/b", "12", "", "a", "scope", "absent"),
		StorageObservationCacheKey("org", "proj", "list", "s3://bucket/a", "13", "", "a", "scope", "absent"),
		StorageObservationCacheKey("org", "proj", "list", "s3://bucket/a", "12", "", "a", "new-scope", "absent"),
		StorageObservationCacheKey("org", "proj", "list", "s3://bucket/a", "12", "", "a", "scope", "etag:new"),
	} {
		if changed == key {
			t.Fatalf("observation key ignored a target identity component")
		}
	}
}

func TestExactJoinCacheTTLUsesNewSettingAndLegacyAlias(t *testing.T) {
	t.Setenv("STORAGE_EXACT_PROJECT_JOIN_CACHE_TTL_SECONDS", "90")
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS", "120")
	if got := StorageExactProjectJoinCacheTTL(); got != 90*time.Second {
		t.Fatalf("new exact join TTL setting = %s, want 90s", got)
	}
	t.Setenv("STORAGE_EXACT_PROJECT_JOIN_CACHE_TTL_SECONDS", "")
	if got := StorageExactProjectJoinCacheTTL(); got != 120*time.Second {
		t.Fatalf("legacy exact join TTL alias = %s, want 120s", got)
	}
}

func TestStorageAuditCacheConstructorsRequireRedisURL(t *testing.T) {
	clearStorageAuditCacheEnv(t)
	constructors := []struct {
		name string
		new  func() (any, error)
	}{
		{name: "response", new: func() (any, error) { return NewStorageChainAuditResponseCacheFromEnv() }},
		{name: "exact join", new: func() (any, error) { return NewStorageExactProjectJoinCacheFromEnv() }},
		{name: "bucket inventory", new: func() (any, error) { return NewProjectBucketInventoryCacheFromEnv() }},
		{name: "storage observations", new: func() (any, error) { return NewStorageObservationCacheFromEnv() }},
	}
	for _, test := range constructors {
		t.Run(test.name, func(t *testing.T) {
			cache, err := test.new()
			if err == nil || !strings.Contains(err.Error(), "STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL or REDIS_URL is required") {
				t.Fatalf("expected missing Redis configuration error, cache=%v err=%v", cache, err)
			}
		})
	}
}

func TestStorageAuditCacheRejectsDisabledAndMalformedConfiguration(t *testing.T) {
	clearStorageAuditCacheEnv(t)
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED", "false")
	if _, err := NewStorageChainAuditResponseCacheFromEnv(); err == nil || !strings.Contains(err.Error(), "require Redis") {
		t.Fatalf("expected disabled cache setting to be rejected, got %v", err)
	}

	clearStorageAuditCacheEnv(t)
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL", "not-a-redis-url")
	if _, err := NewStorageChainAuditResponseCacheFromEnv(); err == nil || !strings.Contains(err.Error(), "parse Redis URL") {
		t.Fatalf("expected malformed Redis URL error, got %v", err)
	}
}

func TestStorageAuditCacheRejectsUnreachableRedisDuringInitialization(t *testing.T) {
	clearStorageAuditCacheEnv(t)
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL", "redis://127.0.0.1:1/0")
	if _, err := NewStorageChainAuditResponseCacheFromEnv(); err == nil || !strings.Contains(err.Error(), "connect to Redis") {
		t.Fatalf("expected Redis connection error, got %v", err)
	}
}

func clearStorageAuditCacheEnv(t *testing.T) {
	t.Helper()
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED", "")
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL", "")
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD", "")
	t.Setenv("REDIS_URL", "")
}

func TestRedisCachesRetainAuditSnapshotsAndKeepInputTTLs(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis cache behavior")
	}
	ctx := context.Background()
	responseCache, err := NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		t.Fatalf("connect response cache to Redis: %v", err)
	}
	defer responseCache.client.Close()
	exactJoinCache, err := NewRedisStorageExactProjectJoinCache(redisURL)
	if err != nil {
		t.Fatalf("connect exact-join cache to Redis: %v", err)
	}
	defer exactJoinCache.client.Close()
	bucketCache, err := NewRedisProjectBucketInventoryCache(redisURL)
	if err != nil {
		t.Fatalf("connect bucket cache to Redis: %v", err)
	}
	defer bucketCache.client.Close()
	observationCache, err := NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect observation cache to Redis: %v", err)
	}
	defer observationCache.client.Close()

	key := fmt.Sprintf("gecko:test:%s:%d", t.Name(), time.Now().UnixNano())
	defer responseCache.client.Del(ctx, key)
	first := CachedResponse{
		CachedAt:  time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Inventory: []model.RepoInventoryFile{{RepoPath: "data/file.bin", Name: "file.bin", Checksum: "abc", Size: 3}},
	}
	if err := responseCache.Set(ctx, key, first); err != nil {
		t.Fatalf("set first saved response: %v", err)
	}
	rawResponse, err := responseCache.client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("read first saved response bytes: %v", err)
	}
	if len(rawResponse) < responseCacheCodecHeaderSize || !bytes.Equal(rawResponse[:responseCacheCodecHeaderSize], responseCacheCodecHeader[:]) {
		t.Fatalf("saved response header = %q, want compressed codec header %q", rawResponse[:min(len(rawResponse), responseCacheCodecHeaderSize)], responseCacheCodecHeader)
	}
	ttl, err := responseCache.client.Do(ctx, "TTL", key).Int64()
	if err != nil || ttl != -1 {
		t.Fatalf("expected Redis response snapshot to have no expiry, TTL=%d err=%v", ttl, err)
	}
	got, ok, err := responseCache.Get(ctx, key)
	if err != nil || !ok || !reflect.DeepEqual(got, first) {
		t.Fatalf("read first saved response: value=%+v ok=%t err=%v", got, ok, err)
	}
	legacyKey := key + ":legacy-json"
	defer responseCache.client.Del(ctx, legacyKey)
	legacyJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal legacy response: %v", err)
	}
	if err := responseCache.client.Set(ctx, legacyKey, legacyJSON, 0).Err(); err != nil {
		t.Fatalf("write legacy JSON response: %v", err)
	}
	legacy, ok, err := responseCache.Get(ctx, legacyKey)
	if err != nil || !ok || !reflect.DeepEqual(legacy, first) {
		t.Fatalf("read legacy JSON response: value=%+v ok=%t err=%v", legacy, ok, err)
	}
	corruptKey := key + ":corrupt"
	defer responseCache.client.Del(ctx, corruptKey)
	corruptValue := append(append([]byte(nil), responseCacheCodecHeader[:]...), []byte("invalid frame")...)
	if err := responseCache.client.Set(ctx, corruptKey, corruptValue, 0).Err(); err != nil {
		t.Fatalf("write corrupt response fixture: %v", err)
	}
	if value, ok, err := responseCache.Get(ctx, corruptKey); err == nil || ok {
		t.Fatalf("read corrupt response = (%+v, %t, %v), want decode error and no cache hit", value, ok, err)
	}

	updated := first
	updated.CachedAt = first.CachedAt.Add(time.Hour)
	updated.Response.Summary.GitRevision = "refreshed-revision"
	if err := responseCache.Set(ctx, key, updated); err != nil {
		t.Fatalf("replace saved response: %v", err)
	}
	got, ok, err = responseCache.Get(ctx, key)
	if err != nil || !ok || !reflect.DeepEqual(got, updated) {
		t.Fatalf("read replaced response: value=%+v ok=%t err=%v", got, ok, err)
	}

	exactKey := key + ":exact-join"
	defer exactJoinCache.client.Del(ctx, exactKey)
	if err := exactJoinCache.Set(ctx, exactKey, CachedExactJoinState{}, 45*time.Second); err != nil {
		t.Fatalf("set exact-join entry: %v", err)
	}
	if ttl, err := exactJoinCache.client.Do(ctx, "PTTL", exactKey).Int64(); err != nil || ttl <= 0 || ttl > 45_000 {
		t.Fatalf("expected exact-join entry to retain its TTL, PTTL=%d err=%v", ttl, err)
	}

	bucketKey := key + ":bucket-inventory"
	defer bucketCache.client.Del(ctx, bucketKey)
	if err := bucketCache.Set(ctx, bucketKey, CachedBucketInventory{}, 24*time.Hour); err != nil {
		t.Fatalf("set bucket inventory entry: %v", err)
	}
	if ttl, err := bucketCache.client.Do(ctx, "PTTL", bucketKey).Int64(); err != nil || ttl <= 0 || ttl > 24*60*60*1000 {
		t.Fatalf("expected bucket inventory entry to retain its TTL, PTTL=%d err=%v", ttl, err)
	}

	observationKey := key + ":observation"
	defer observationCache.client.Del(ctx, observationKey)
	fenceKey := key + ":fence"
	defer observationCache.client.Del(ctx, fenceKey)
	versionOne, err := observationCache.NextVersion(ctx, "org", "proj")
	if err != nil || versionOne.Counter == 0 || versionOne.Epoch == "" {
		t.Fatalf("allocate initial observation version: version=%+v err=%v", versionOne, err)
	}
	versionKey := StorageObservationVersionKey("org", "proj")
	defer observationCache.client.Del(ctx, versionKey)
	if ttl, err := observationCache.client.Do(ctx, "TTL", versionKey).Int64(); err != nil || ttl != -1 {
		t.Fatalf("expected observation version key to have no expiry, TTL=%d err=%v", ttl, err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionOne, []string{fenceKey}); err != nil {
		t.Fatalf("initialize observation fence: %v", err)
	}
	observation := CachedStorageObservation{Version: versionOne, ObservedAt: "2026-10-01T01:00:00Z", Result: gintegrationsyfon.BulkStorageProbeResult{ID: "target", Exists: true, Status: "present"}}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: observation}}); err != nil {
		t.Fatalf("set observation: %v", err)
	}
	if ttl, err := observationCache.client.Do(ctx, "TTL", observationKey).Int64(); err != nil || ttl != -1 {
		t.Fatalf("expected observation to have no expiry, TTL=%d err=%v", ttl, err)
	}
	versionTwo, err := observationCache.NextVersion(ctx, "org", "proj")
	if err != nil || versionTwo.Epoch != versionOne.Epoch || versionTwo.Counter <= versionOne.Counter {
		t.Fatalf("allocate revalidation version: version=%+v err=%v", versionTwo, err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionTwo, []string{fenceKey}); err != nil {
		t.Fatalf("preserve observation fence during revalidation: %v", err)
	}
	retirement := CachedStorageObservation{Version: versionTwo, ObservedAt: "2026-10-01T02:00:00Z", Retired: true}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: fenceKey, FenceKey: fenceKey, Observation: retirement}}); err != nil {
		t.Fatalf("retire contradicted observation: %v", err)
	}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: observation}}); err != nil {
		t.Fatalf("attempt to restore older observation: %v", err)
	}
	observations, err := observationCache.GetMany(ctx, "org", "proj", versionTwo, []string{observationKey, fenceKey, key + ":missing"})
	if err != nil || len(observations) != 2 || !observations[fenceKey].Retired || observations[fenceKey].Version != versionTwo || observations[observationKey].Retired {
		t.Fatalf("expected newer retirement to reject older positive: values=%+v err=%v", observations, err)
	}
	versionThree, err := observationCache.NextVersion(ctx, "org", "proj")
	if err != nil || versionThree.Epoch != versionTwo.Epoch || versionThree.Counter <= versionTwo.Counter {
		t.Fatalf("allocate later successful observation version: version=%+v err=%v", versionThree, err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionThree, []string{fenceKey}); err != nil {
		t.Fatalf("preserve observation fence before new positive: %v", err)
	}
	newerObservation := CachedStorageObservation{Version: versionThree, ObservedAt: "2026-10-01T03:00:00Z", Result: gintegrationsyfon.BulkStorageProbeResult{ID: "target", Exists: true, Status: "present"}}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: newerObservation}}); err != nil {
		t.Fatalf("store later positive observation: %v", err)
	}
	observations, err = observationCache.GetMany(ctx, "org", "proj", versionThree, []string{observationKey, fenceKey})
	if err != nil || len(observations) != 2 || observations[observationKey].Retired || observations[observationKey].Version != versionThree || observations[fenceKey].Version != versionTwo {
		t.Fatalf("expected later positive to replace retirement, values=%+v err=%v", observations, err)
	}
	if err := observationCache.client.Del(ctx, versionKey).Err(); err != nil {
		t.Fatalf("simulate Redis eviction of the version key: %v", err)
	}
	versionAfterEviction, err := observationCache.NextVersion(ctx, "org", "proj")
	if err != nil || versionAfterEviction.Epoch == versionThree.Epoch {
		t.Fatalf("version epoch did not change after key eviction: before=%+v after=%+v err=%v", versionThree, versionAfterEviction, err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionAfterEviction, []string{fenceKey}); err != nil {
		t.Fatalf("reinitialize observation fence after version eviction: %v", err)
	}
	observations, err = observationCache.GetMany(ctx, "org", "proj", versionAfterEviction, []string{observationKey, fenceKey})
	if err != nil || len(observations) != 1 || !observations[fenceKey].Retired || observations[fenceKey].Version.Epoch != versionAfterEviction.Epoch {
		t.Fatalf("evicted version key allowed old observations to be reused: values=%+v err=%v", observations, err)
	}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: newerObservation}}); err == nil {
		t.Fatal("old-epoch callback published after the version key was evicted")
	}
	currentEpochObservation := CachedStorageObservation{Version: versionAfterEviction, ObservedAt: "2026-10-01T04:00:00Z", Result: gintegrationsyfon.BulkStorageProbeResult{ID: "target", Exists: true, Status: "present"}}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: currentEpochObservation}}); err != nil {
		t.Fatalf("set current-epoch observation before fence eviction: %v", err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionAfterEviction, []string{fenceKey}); err != nil {
		t.Fatalf("ensure fence after evicted version key: %v", err)
	}
	versionAfterMarkerEviction, err := observationCache.NextVersion(ctx, "org", "proj")
	if err != nil {
		t.Fatalf("allocate version before retirement fence eviction: %v", err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionAfterMarkerEviction, []string{fenceKey}); err != nil {
		t.Fatalf("prepare fence before retirement: %v", err)
	}
	retirementAfterVersionEviction := CachedStorageObservation{Version: versionAfterMarkerEviction, ObservedAt: "2026-10-01T05:00:00Z", Retired: true}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: fenceKey, FenceKey: fenceKey, Observation: retirementAfterVersionEviction}}); err != nil {
		t.Fatalf("record retirement before fence eviction: %v", err)
	}
	if err := observationCache.client.Del(ctx, fenceKey).Err(); err != nil {
		t.Fatalf("simulate retirement fence eviction: %v", err)
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionAfterEviction, []string{fenceKey}); err == nil {
		t.Fatal("delayed older request initialized an evicted retirement fence")
	}
	if err := observationCache.EnsureFences(ctx, "org", "proj", versionAfterMarkerEviction, []string{fenceKey}); err != nil {
		t.Fatalf("current request reinitialized evicted retirement fence: %v", err)
	}
	observations, err = observationCache.GetMany(ctx, "org", "proj", versionAfterMarkerEviction, []string{observationKey, fenceKey})
	if err != nil || len(observations) != 2 || observations[observationKey].Version.Counter > observations[fenceKey].Version.Counter || observations[fenceKey].Version.Counter != versionAfterMarkerEviction.Counter-1 {
		t.Fatalf("evicted fence did not invalidate the older positive: observations=%+v err=%v", observations, err)
	}
	if err := observationCache.SetMany(ctx, "org", "proj", []StorageObservationWrite{{Key: observationKey, FenceKey: fenceKey, Observation: currentEpochObservation}}); err != nil {
		t.Fatalf("late stale callback after fence reinitialization: %v", err)
	}
	observations, err = observationCache.GetMany(ctx, "org", "proj", versionAfterMarkerEviction, []string{observationKey, fenceKey})
	if err != nil || len(observations) != 2 || observations[observationKey].Version.Counter > observations[fenceKey].Version.Counter {
		t.Fatalf("late callback crossed the reinitialized fence: observations=%+v err=%v", observations, err)
	}
	malformedKey := key + ":missing-observed-at"
	defer observationCache.client.Del(ctx, malformedKey)
	if err := observationCache.client.Set(ctx, malformedKey, `{"generation":1,"result":{"ID":"target","Exists":true,"Status":"present"}}`, 0).Err(); err != nil {
		t.Fatalf("write malformed observation fixture: %v", err)
	}
	if _, err := observationCache.GetMany(ctx, "org", "proj", versionAfterEviction, []string{malformedKey}); err == nil || !strings.Contains(err.Error(), "missing observed_at") {
		t.Fatalf("expected missing observation timestamp to fail instead of being manufactured, got %v", err)
	}
}

func TestProjectBucketInventoryCacheRetention(t *testing.T) {
	if ProjectBucketInventoryRefreshInterval.String() != "10m0s" {
		t.Fatalf("ProjectBucketInventoryRefreshInterval = %s, want 10m", ProjectBucketInventoryRefreshInterval)
	}
	if ProjectBucketInventoryStaleTTL.String() != "24h0m0s" {
		t.Fatalf("ProjectBucketInventoryStaleTTL = %s, want 24h", ProjectBucketInventoryStaleTTL)
	}
}

func TestRedisURLWithPasswordMatchesFenceStyle(t *testing.T) {
	got := RedisURLWithPassword("redis://authz-cache-service:6379/0", "secret")
	want := "redis://:secret@authz-cache-service:6379/0"
	if got != want {
		t.Fatalf("RedisURLWithPassword() = %q, want %q", got, want)
	}
}

func TestRedisURLWithPasswordKeepsExistingCredential(t *testing.T) {
	input := "redis://:existing@authz-cache-service:6379/0"
	got := RedisURLWithPassword(input, "secret")
	if got != input {
		t.Fatalf("RedisURLWithPassword() = %q, want %q", got, input)
	}
}

func TestRedisResponseCachePropagatesUnavailableWarmReadAndWrite(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis failure behavior")
	}
	cache, err := NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		t.Fatalf("connect response cache to Redis: %v", err)
	}
	adminClient := cache.client
	defer adminClient.Close()
	ctx := context.Background()
	key := fmt.Sprintf("gecko:test:%s:%d", t.Name(), time.Now().UnixNano())
	defer adminClient.Del(ctx, key)
	if err := cache.Set(ctx, key, CachedResponse{CachedAt: time.Now()}); err != nil {
		t.Fatalf("seed warm response snapshot: %v", err)
	}
	if _, ok, err := cache.Get(ctx, key); err != nil || !ok {
		t.Fatalf("read seeded warm response snapshot: ok=%t err=%v", ok, err)
	}

	options := adminClient.Options()
	cache.client = redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		Username:     options.Username,
		Password:     options.Password,
		DB:           options.DB,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   0,
	})
	defer cache.Close()
	if _, ok, err := cache.Get(ctx, key); err == nil || ok {
		t.Fatalf("expected Redis outage to fail a warm GET, ok=%t err=%v", ok, err)
	}
	if err := cache.Set(ctx, key, CachedResponse{CachedAt: time.Now()}); err == nil {
		t.Fatal("expected Redis outage to fail SET")
	}
}

func TestRedisResponseCachePropagatesRedisWriteDenial(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis write failure behavior")
	}
	adminCache, err := NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		t.Fatalf("connect response cache to Redis: %v", err)
	}
	defer adminCache.Close()
	ctx := context.Background()
	username := fmt.Sprintf("gecko-test-%d", time.Now().UnixNano())
	password := fmt.Sprintf("gecko-test-password-%d", time.Now().UnixNano())
	if err := adminCache.client.Do(ctx, "ACL", "SETUSER", username, "reset", "on", ">"+password, "-@all", "+ping", "+get", "+acl|whoami", "~*").Err(); err != nil {
		t.Skipf("Redis ACL users unavailable for write-denial test: %v", err)
	}
	defer adminCache.client.Do(ctx, "ACL", "DELUSER", username)

	options := adminCache.client.Options()
	readOnlyClient := redis.NewClient(&redis.Options{
		Addr:         options.Addr,
		Username:     username,
		Password:     password,
		DB:           options.DB,
		DialTimeout:  options.DialTimeout,
		ReadTimeout:  options.ReadTimeout,
		WriteTimeout: options.WriteTimeout,
	})
	defer readOnlyClient.Close()
	if err := readOnlyClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect read-only Redis user: %v", err)
	}
	whoami, err := readOnlyClient.Do(ctx, "ACL", "WHOAMI").Text()
	if err != nil || whoami != username {
		t.Fatalf("read-only Redis identity = %q, want %q, err=%v", whoami, username, err)
	}
	readOnlyCache := &RedisResponseCache{client: readOnlyClient}
	if _, _, err := readOnlyCache.Get(ctx, "gecko:test:write-denial"); err != nil {
		t.Fatalf("read with read-only Redis user: %v", err)
	}
	if err := readOnlyCache.Set(ctx, "gecko:test:write-denial", CachedResponse{CachedAt: time.Now()}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "noperm") {
		t.Fatalf("expected Redis permission denial from SET, got %v", err)
	}
}
