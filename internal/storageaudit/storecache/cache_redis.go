package storecache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisResponseCache struct {
	client *redis.Client
}

type RedisExactJoinCache struct {
	client *redis.Client
}

type RedisBucketInventoryCache struct {
	client *redis.Client
}

type RedisStorageObservationCache struct {
	client *redis.Client
}

const storageObservationEnsureFenceLua = `
local clock = redis.call('GET', KEYS[1])
if not clock then
	return -1
end
local epoch, counter = string.match(clock, '^([^:]+):(%d+)$')
if not epoch or epoch ~= ARGV[1] then
	return -1
end
local current = redis.call('GET', KEYS[2])
if current then
	local ok, decoded = pcall(cjson.decode, current)
	if ok and decoded.version and decoded.version.epoch == epoch and decoded.retired == true then
		return 0
	end
end
if tonumber(counter) ~= tonumber(ARGV[2]) then
	return -1
end
local cutoff = tonumber(counter) - 1
if cutoff < 0 then
	cutoff = 0
end
local fence = cjson.encode({version={epoch=epoch,counter=cutoff},retired=true})
redis.call('SET', KEYS[2], fence)
return 1
`

const storageObservationSetLua = `
local clock = redis.call('GET', KEYS[1])
if not clock then
	return -1
end
local epoch, counter = string.match(clock, '^([^:]+):(%d+)$')
if not epoch or epoch ~= ARGV[2] then
	return -1
end
local fence = redis.call('GET', KEYS[3])
if not fence then
	return -1
end
local fenceOk, fenceValue = pcall(cjson.decode, fence)
if not fenceOk or not fenceValue.version or fenceValue.version.epoch ~= epoch or fenceValue.retired ~= true then
	return -1
end
local incomingCounter = tonumber(ARGV[3])
local fenceCounter = tonumber(fenceValue.version.counter or 0)
local incomingRetired = ARGV[4] == '1'
if incomingRetired then
	if incomingCounter < fenceCounter then
		return 0
	end
	redis.call('SET', KEYS[3], ARGV[1])
	return 1
end
if incomingCounter <= fenceCounter then
	return 0
end
local current = redis.call('GET', KEYS[2])
if current then
	local ok, decoded = pcall(cjson.decode, current)
	if ok and decoded.version and decoded.version.epoch == epoch then
		local currentCounter = tonumber(decoded.version.counter or 0)
		if currentCounter > incomingCounter then
			return 0
		end
		if currentCounter == incomingCounter and decoded.retired == true then
			return 0
		end
	end
end
redis.call('SET', KEYS[2], ARGV[1])
return 1
`

const storageObservationVersionLua = `
local current = redis.call('GET', KEYS[1])
local epoch
local counter
if current then
	epoch, counter = string.match(current, '^([^:]+):(%d+)$')
	if not epoch then
		return redis.error_reply('invalid storage observation version key')
	end
else
	epoch = ARGV[1]
	counter = '0'
end
local nextCounter = tonumber(counter) + 1
local version = epoch .. ':' .. string.format('%.0f', nextCounter)
redis.call('SET', KEYS[1], version)
return version
`

func NewRedisStorageChainAuditResponseCache(redisURL string) (*RedisResponseCache, error) {
	client, err := newRedisClient(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisResponseCache{client: client}, nil
}

func NewRedisStorageExactProjectJoinCache(redisURL string) (*RedisExactJoinCache, error) {
	client, err := newRedisClient(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisExactJoinCache{client: client}, nil
}

func NewRedisProjectBucketInventoryCache(redisURL string) (*RedisBucketInventoryCache, error) {
	client, err := newRedisClient(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisBucketInventoryCache{client: client}, nil
}

func NewRedisStorageObservationCache(redisURL string) (*RedisStorageObservationCache, error) {
	client, err := newRedisClient(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisStorageObservationCache{client: client}, nil
}

func newRedisClient(redisURL string) (*redis.Client, error) {
	options, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}
	return client, nil
}

func (cache *RedisResponseCache) Close() error { return cache.client.Close() }

func (cache *RedisExactJoinCache) Close() error { return cache.client.Close() }

func (cache *RedisBucketInventoryCache) Close() error { return cache.client.Close() }

func (cache *RedisStorageObservationCache) Close() error { return cache.client.Close() }

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

func (cache *RedisResponseCache) Set(ctx context.Context, key string, value CachedResponse) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cache.client.Set(ctx, key, raw, 0).Err()
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

func (cache *RedisStorageObservationCache) GetMany(ctx context.Context, organization string, project string, version StorageObservationVersion, keys []string) (map[string]CachedStorageObservation, error) {
	out := make(map[string]CachedStorageObservation, len(keys))
	for start := 0; start < len(keys); start += storageObservationRedisBatchSize {
		end := start + storageObservationRedisBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		redisKeys := make([]string, 0, len(batch)+1)
		redisKeys = append(redisKeys, StorageObservationVersionKey(organization, project))
		redisKeys = append(redisKeys, batch...)
		values, err := cache.client.MGet(ctx, redisKeys...).Result()
		if err != nil {
			return nil, err
		}
		if len(values) == 0 || values[0] == nil {
			return nil, fmt.Errorf("storage observation version key disappeared during lookup")
		}
		currentVersionRaw, ok := values[0].(string)
		if !ok {
			return nil, fmt.Errorf("decode storage observation version: expected string, got %T", values[0])
		}
		currentVersion, err := parseStorageObservationVersion(currentVersionRaw)
		if err != nil {
			return nil, err
		}
		if currentVersion.Epoch != version.Epoch {
			return nil, fmt.Errorf("storage observation version changed during lookup")
		}
		for index, value := range values[1:] {
			if value == nil {
				continue
			}
			raw, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("decode storage observation cache value for %s: expected string, got %T", batch[index], value)
			}
			var observation CachedStorageObservation
			if err := json.Unmarshal([]byte(raw), &observation); err != nil {
				return nil, fmt.Errorf("decode storage observation cache value for %s: %w", batch[index], err)
			}
			if !observation.Retired && strings.TrimSpace(observation.ObservedAt) == "" {
				return nil, fmt.Errorf("decode storage observation cache value for %s: missing observed_at", batch[index])
			}
			if observation.Version.Epoch != version.Epoch {
				continue
			}
			out[batch[index]] = observation
		}
	}
	return out, nil
}

func (cache *RedisStorageObservationCache) EnsureFences(ctx context.Context, organization string, project string, version StorageObservationVersion, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	keys = uniqueKeys(keys)
	sort.Strings(keys)
	for start := 0; start < len(keys); start += storageObservationRedisBatchSize {
		end := start + storageObservationRedisBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		pipeline := cache.client.Pipeline()
		commands := make([]*redis.Cmd, 0, len(batch))
		for _, key := range batch {
			commands = append(commands, pipeline.Eval(ctx, storageObservationEnsureFenceLua, []string{StorageObservationVersionKey(organization, project), key}, version.Epoch, version.Counter))
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return err
		}
		for _, command := range commands {
			result, err := command.Int64()
			if err != nil {
				return err
			}
			if result < 0 {
				return fmt.Errorf("storage observation version changed before checkpoint")
			}
		}
	}
	return nil
}

func (cache *RedisStorageObservationCache) SetMany(ctx context.Context, organization string, project string, writes []StorageObservationWrite) error {
	if len(writes) == 0 {
		return nil
	}
	sort.Slice(writes, func(i, j int) bool { return writes[i].Key < writes[j].Key })
	for start := 0; start < len(writes); start += storageObservationRedisBatchSize {
		end := start + storageObservationRedisBatchSize
		if end > len(writes) {
			end = len(writes)
		}
		batch := writes[start:end]
		pipeline := cache.client.Pipeline()
		commands := make([]*redis.Cmd, 0, len(batch))
		for _, write := range batch {
			if write.Key == "" || write.FenceKey == "" || write.Observation.Version.Epoch == "" || write.Observation.Version.Counter == 0 {
				return fmt.Errorf("encode storage observation cache write: missing key, fence, or version")
			}
			raw, err := json.Marshal(write.Observation)
			if err != nil {
				return fmt.Errorf("encode storage observation cache value for %s: %w", write.Key, err)
			}
			retired := "0"
			if write.Observation.Retired {
				retired = "1"
			}
			commands = append(commands, pipeline.Eval(ctx, storageObservationSetLua, []string{StorageObservationVersionKey(organization, project), write.Key, write.FenceKey}, raw, write.Observation.Version.Epoch, write.Observation.Version.Counter, retired))
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return err
		}
		for _, command := range commands {
			result, err := command.Int64()
			if err != nil {
				return err
			}
			if result < 0 {
				return fmt.Errorf("storage observation version or fence changed before checkpoint")
			}
		}
	}
	return nil
}

func uniqueKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	unique := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}

func (cache *RedisStorageObservationCache) Source() string { return "redis" }

func (cache *RedisStorageObservationCache) NextVersion(ctx context.Context, organization string, project string) (StorageObservationVersion, error) {
	randomEpoch := make([]byte, 16)
	if _, err := rand.Read(randomEpoch); err != nil {
		return StorageObservationVersion{}, fmt.Errorf("create storage observation epoch: %w", err)
	}
	versionRaw, err := cache.client.Eval(ctx, storageObservationVersionLua, []string{StorageObservationVersionKey(organization, project)}, hex.EncodeToString(randomEpoch)).Text()
	if err != nil {
		return StorageObservationVersion{}, err
	}
	return parseStorageObservationVersion(versionRaw)
}

func parseStorageObservationVersion(raw string) (StorageObservationVersion, error) {
	epoch, counterRaw, ok := strings.Cut(raw, ":")
	if !ok || strings.TrimSpace(epoch) == "" {
		return StorageObservationVersion{}, fmt.Errorf("decode storage observation version %q", raw)
	}
	counter, err := strconv.ParseUint(counterRaw, 10, 64)
	if err != nil || counter == 0 {
		return StorageObservationVersion{}, fmt.Errorf("decode storage observation version counter %q", counterRaw)
	}
	return StorageObservationVersion{Epoch: epoch, Counter: counter}, nil
}

const storageObservationRedisBatchSize = 500
