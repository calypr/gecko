package storecache

import (
	"context"
	"sync"
	"time"
)

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
