package git

import "testing"

func TestProjectBucketInventoryCacheRetention(t *testing.T) {
	if projectBucketInventoryRefreshInterval.String() != "10m0s" {
		t.Fatalf("projectBucketInventoryRefreshInterval = %s, want 10m", projectBucketInventoryRefreshInterval)
	}
	if projectBucketInventoryStaleTTL.String() != "24h0m0s" {
		t.Fatalf("projectBucketInventoryStaleTTL = %s, want 24h", projectBucketInventoryStaleTTL)
	}
}

func TestRedisURLWithPasswordMatchesFenceStyle(t *testing.T) {
	got := redisURLWithPassword("redis://authz-cache-service:6379/0", "secret")
	want := "redis://:secret@authz-cache-service:6379/0"
	if got != want {
		t.Fatalf("redisURLWithPassword() = %q, want %q", got, want)
	}
}

func TestRedisURLWithPasswordKeepsExistingCredential(t *testing.T) {
	input := "redis://:existing@authz-cache-service:6379/0"
	got := redisURLWithPassword(input, "secret")
	if got != input {
		t.Fatalf("redisURLWithPassword() = %q, want %q", got, input)
	}
}
