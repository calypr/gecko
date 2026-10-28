package storecache

import (
	"context"
	"encoding/json"
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
