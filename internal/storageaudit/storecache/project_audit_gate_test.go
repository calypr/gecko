package storecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisProjectAuditCoordinatorsExcludeSameProject(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	first := newProjectAuditTestCoordinator(t, redisURL)
	second := newProjectAuditTestCoordinator(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	firstLease, err := first.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("first AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, firstLease)

	waiterCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if lease, err := second.AcquireProject(waiterCtx, organization, project); !errors.Is(err, context.DeadlineExceeded) || lease != nil {
		t.Fatalf("second AcquireProject() = (%v, %v), want canceled wait", lease, err)
	}
}

func TestRedisProjectAuditCoordinatorsAllowDifferentProjects(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	first := newProjectAuditTestCoordinator(t, redisURL)
	second := newProjectAuditTestCoordinator(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	firstLease, err := first.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("AcquireProject(first) error = %v", err)
	}
	defer releaseProjectAuditLease(t, firstLease)

	secondLease, err := second.AcquireProject(context.Background(), organization, project+"-other")
	if err != nil {
		t.Fatalf("AcquireProject(other project) error = %v", err)
	}
	defer releaseProjectAuditLease(t, secondLease)
}

func TestRedisProjectAuditLeaseRenewalKeepsProjectExclusive(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	first := newProjectAuditTestCoordinatorWithTiming(t, redisURL, 180*time.Millisecond, 30*time.Millisecond)
	second := newProjectAuditTestCoordinatorWithTiming(t, redisURL, 180*time.Millisecond, 30*time.Millisecond)
	organization, project := uniqueProjectAuditIdentity(t)
	firstLease, err := first.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("first AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, firstLease)

	time.Sleep(300 * time.Millisecond)
	waiterCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if lease, err := second.AcquireProject(waiterCtx, organization, project); !errors.Is(err, context.DeadlineExceeded) || lease != nil {
		t.Fatalf("AcquireProject() after renewal = (%v, %v), want continued exclusion", lease, err)
	}
}

func TestRedisProjectAuditWaiterCancellationAndPreCanceledAcquire(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	coordinator := newProjectAuditTestCoordinator(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	owner, err := coordinator.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("owner AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, owner)

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	if lease, err := coordinator.AcquireProject(waiterCtx, organization, project); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("pre-canceled AcquireProject() = (%v, %v), want context canceled", lease, err)
	}

	key, err := projectAuditLeaseKey(organization, project)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := coordinator.client.Exists(context.Background(), key).Result(); err != nil || exists != 1 {
		t.Fatalf("pre-canceled acquisition changed lease key, exists=%d err=%v", exists, err)
	}
}

func TestRedisProjectAuditLeaseLossFencesPublicationAndRelease(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	first := newProjectAuditTestCoordinator(t, redisURL)
	second := newProjectAuditTestCoordinator(t, redisURL)
	admin := newProjectAuditTestAdmin(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	firstLease, err := first.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("first AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, firstLease)
	key, err := projectAuditLeaseKey(organization, project)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("delete first lease key: %v", err)
	}
	secondLease, err := second.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("second AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, secondLease)

	select {
	case <-firstLease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("former owner context stayed active after publication observed lease loss")
	}

	responseKey := projectAuditCacheTestKey(t, ResponseCacheKeyPrefix, admin)
	if err := firstLease.PublishResponse(context.Background(), responseKey, CachedResponse{}); !errors.Is(err, ErrProjectAuditLeaseLost) {
		t.Fatalf("former owner PublishResponse() error = %v, want lease lost", err)
	}
	if err := firstLease.Release(context.Background()); err != nil {
		t.Fatalf("former owner Release() error = %v", err)
	}
	currentOwner, err := admin.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read current project owner: %v", err)
	}
	if currentOwner != secondLease.(*redisProjectAuditLease).owner {
		t.Fatal("former owner release deleted or changed the newer owner's lease")
	}
	if exists, err := admin.Exists(context.Background(), responseKey).Result(); err != nil || exists != 0 {
		t.Fatalf("stale response was published, exists=%d err=%v", exists, err)
	}
}

func TestRedisProjectAuditLeaseRedisErrorCancelsOwnedWork(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	client, err := newRedisClient(redisURL)
	if err != nil {
		t.Fatalf("connect to test Redis: %v", err)
	}
	coordinator := newRedisProjectAuditCoordinator(client, 2*time.Second, 30*time.Millisecond)
	clientClosed := false
	t.Cleanup(func() {
		if !clientClosed {
			if err := coordinator.Close(); err != nil {
				t.Errorf("close test Redis coordinator: %v", err)
			}
		}
	})
	admin := newProjectAuditTestAdmin(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	lease, err := coordinator.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("AcquireProject() error = %v", err)
	}
	key, err := projectAuditLeaseKey(organization, project)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatalf("close Redis connection to simulate an error: %v", err)
	}
	clientClosed = true

	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease context stayed active after Redis renewal error")
	}
	select {
	case <-lease.(*redisProjectAuditLease).renewDone:
	case <-time.After(time.Second):
		t.Fatal("renewal goroutine remained after Redis renewal error")
	}
	if err := admin.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("delete test lease key: %v", err)
	}
}

func TestRedisProjectAuditLeasePublicationKeepsResponseWithoutTTL(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	coordinator := newProjectAuditTestCoordinator(t, redisURL)
	admin := newProjectAuditTestAdmin(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	lease, err := coordinator.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("AcquireProject() error = %v", err)
	}
	defer releaseProjectAuditLease(t, lease)

	responseKey := projectAuditCacheTestKey(t, ResponseCacheKeyPrefix, admin)
	want := CachedResponse{CachedAt: time.Now().UTC()}
	if err := lease.PublishResponse(context.Background(), responseKey, want); err != nil {
		t.Fatalf("PublishResponse() error = %v", err)
	}
	if ttl, err := admin.PTTL(context.Background(), responseKey).Result(); err != nil || ttl != -1 {
		t.Fatalf("response PTTL = %s, err=%v, want no expiration (-1)", ttl, err)
	}
	raw, err := admin.Get(context.Background(), responseKey).Bytes()
	if err != nil {
		t.Fatalf("read published response: %v", err)
	}
	var got CachedResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode published response: %v", err)
	}
	if !got.CachedAt.Equal(want.CachedAt) {
		t.Fatalf("published response CachedAt = %v, want %v", got.CachedAt, want.CachedAt)
	}

	bucketKey := projectAuditCacheTestKey(t, BucketInventoryKeyPrefix, admin)
	if err := lease.PublishBucketInventory(context.Background(), bucketKey, CachedBucketInventory{Complete: true}, 2*time.Minute); err != nil {
		t.Fatalf("PublishBucketInventory() error = %v", err)
	}
	if ttl, err := admin.PTTL(context.Background(), bucketKey).Result(); err != nil || ttl <= 0 || ttl > 2*time.Minute {
		t.Fatalf("bucket inventory PTTL = %s, err=%v, want a bounded positive TTL", ttl, err)
	}
}

func TestRedisProjectAuditLeaseReleaseStopsRenewal(t *testing.T) {
	redisURL := requireProjectAuditRedisURL(t)
	coordinator := newProjectAuditTestCoordinator(t, redisURL)
	organization, project := uniqueProjectAuditIdentity(t)
	lease, err := coordinator.AcquireProject(context.Background(), organization, project)
	if err != nil {
		t.Fatalf("AcquireProject() error = %v", err)
	}
	internalLease := lease.(*redisProjectAuditLease)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	select {
	case <-internalLease.renewDone:
	case <-time.After(time.Second):
		t.Fatal("renewal goroutine remained after Release")
	}
}

func requireProjectAuditRedisURL(t *testing.T) string {
	t.Helper()
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify project audit coordination against Redis")
	}
	return redisURL
}

func newProjectAuditTestCoordinator(t *testing.T, redisURL string) *RedisProjectAuditCoordinator {
	t.Helper()
	return newProjectAuditTestCoordinatorWithTiming(t, redisURL, 2*time.Second, 50*time.Millisecond)
}

func newProjectAuditTestCoordinatorWithTiming(t *testing.T, redisURL string, leaseDuration time.Duration, renewInterval time.Duration) *RedisProjectAuditCoordinator {
	t.Helper()
	client, err := newRedisClient(redisURL)
	if err != nil {
		t.Fatalf("connect to test Redis: %v", err)
	}
	coordinator := newRedisProjectAuditCoordinator(client, leaseDuration, renewInterval)
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Errorf("close test Redis coordinator: %v", err)
		}
	})
	return coordinator
}

func newProjectAuditTestAdmin(t *testing.T, redisURL string) *redis.Client {
	t.Helper()
	client, err := newRedisClient(redisURL)
	if err != nil {
		t.Fatalf("connect to test Redis admin client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close test Redis admin client: %v", err)
		}
	})
	return client
}

func uniqueProjectAuditIdentity(t *testing.T) (string, string) {
	t.Helper()
	return "project-audit-test", fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func projectAuditCacheTestKey(t *testing.T, prefix string, client *redis.Client) string {
	t.Helper()
	key := fmt.Sprintf("%s%s-%d", prefix, t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		if err := client.Del(context.Background(), key).Err(); err != nil {
			t.Errorf("delete test Redis cache key: %v", err)
		}
	})
	return key
}

func releaseProjectAuditLease(t *testing.T, lease ProjectAuditLease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		t.Errorf("Release() error = %v", err)
	}
}
