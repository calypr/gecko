package storecache

import (
	"testing"
)

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
