package git

import "testing"

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
