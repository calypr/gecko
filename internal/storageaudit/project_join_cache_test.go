package storageaudit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/go-git/go-git/v5/plumbing"
)

func TestProjectJoinUsesWarmRedisEntryAndFailsWhenRedisGoesDown(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-a", Checksum: strings.Repeat("a", 64), Organization: "org", Project: "proj",
		}},
		usageByObject: map[string]gintegrationsyfon.FileUsage{"obj-a": {ObjectID: "obj-a", DownloadCount: 1}},
	}
	service := NewStorageAnalyticsService(backend)
	cache := newTestExactJoinCache()
	service.join.exact = cache
	hash := plumbing.NewHash(strings.Repeat("b", 40))
	inventory := []RepoInventoryFile{{Checksum: strings.Repeat("a", 64)}}
	if _, _, err := service.join.load(context.Background(), "Bearer token", "org", "proj", hash, inventory, false); err != nil {
		t.Fatalf("build and save exact join: %v", err)
	}
	cache.failGet(errors.New("redis unavailable"))
	if _, _, err := service.join.load(context.Background(), "Bearer token", "org", "proj", hash, inventory, false); err == nil || !strings.Contains(err.Error(), "redis unavailable") {
		t.Fatalf("expected warm exact-join Redis read failure, got %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected failed Redis read to stop rebuilding the join, record loads=%d", backend.listProjectAuditRecordsCalls)
	}
}

func TestProjectJoinDoesNotKeepResultAfterRedisWriteFailure(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-a", Checksum: strings.Repeat("a", 64), Organization: "org", Project: "proj",
		}},
		usageByObject: map[string]gintegrationsyfon.FileUsage{"obj-a": {ObjectID: "obj-a", DownloadCount: 1}},
	}
	service := NewStorageAnalyticsService(backend)
	cache := newTestExactJoinCache()
	cache.failSet(errors.New("redis write failed"))
	service.join.exact = cache
	hash := plumbing.NewHash(strings.Repeat("b", 40))
	inventory := []RepoInventoryFile{{Checksum: strings.Repeat("a", 64)}}
	if _, _, err := service.join.load(context.Background(), "Bearer token", "org", "proj", hash, inventory, false); err == nil || !strings.Contains(err.Error(), "redis write failed") {
		t.Fatalf("expected Redis write failure to fail join request, got %v", err)
	}
	cache.failSet(nil)
	if _, _, err := service.join.load(context.Background(), "Bearer token", "org", "proj", hash, inventory, false); err != nil {
		t.Fatalf("rebuild after failed Redis write: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 2 {
		t.Fatalf("expected failed write not to populate an in-process join result, record loads=%d", backend.listProjectAuditRecordsCalls)
	}
}

func TestProjectJoinLeaderCancellationKeepsWorkForAnotherViewer(t *testing.T) {
	base := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-a", Checksum: strings.Repeat("a", 64), Organization: "org", Project: "proj",
		}},
		usageByObject: map[string]gintegrationsyfon.FileUsage{"obj-a": {ObjectID: "obj-a", DownloadCount: 1}},
	}
	backend := &blockingProjectJoinBackend{fakeStorageAnalyticsBackend: base, started: make(chan struct{}), release: make(chan struct{})}
	service := NewStorageAnalyticsService(backend)
	hash := plumbing.NewHash(strings.Repeat("b", 40))
	inventory := []RepoInventoryFile{{Checksum: strings.Repeat("a", 64)}}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := service.join.load(leaderCtx, "Bearer token", "org", "proj", hash, inventory, false)
		leaderDone <- err
	}()
	<-backend.started

	followerObserved := make(chan struct{})
	followerCtx, cancelFollower := newProjectJoinObservedContext(followerObserved, time.Second)
	defer cancelFollower()
	followerDone := make(chan struct {
		records map[string][]sarecord.State
		usage   map[string]gintegrationsyfon.FileUsage
		err     error
	}, 1)
	go func() {
		records, usage, err := service.join.load(followerCtx, "Bearer token", "org", "proj", hash, inventory, false)
		followerDone <- struct {
			records map[string][]sarecord.State
			usage   map[string]gintegrationsyfon.FileUsage
			err     error
		}{records, usage, err}
	}()
	<-followerObserved
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want request cancellation", err)
	}
	close(backend.release)
	result := <-followerDone
	if result.err != nil || len(result.records[strings.Repeat("a", 64)]) != 1 || result.usage["obj-a"].DownloadCount != 1 {
		t.Fatalf("follower result records=%v usage=%v error=%v", result.records, result.usage, result.err)
	}
	if base.listProjectAuditRecordsCalls != 1 || base.listProjectFileUsageByObjectIDsCalls != 1 {
		t.Fatalf("leader cancellation repeated shared work: record loads=%d usage loads=%d", base.listProjectAuditRecordsCalls, base.listProjectFileUsageByObjectIDsCalls)
	}
}

type blockingProjectJoinBackend struct {
	*fakeStorageAnalyticsBackend
	started chan struct{}
	release chan struct{}
	start   sync.Once
}

func (backend *blockingProjectJoinBackend) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error) {
	backend.start.Do(func() { close(backend.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-backend.release:
	}
	return backend.fakeStorageAnalyticsBackend.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, pathPrefix)
}

type projectJoinObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
	deadline time.Time
}

func (ctx projectJoinObservedContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func newProjectJoinObservedContext(observed chan struct{}, timeout time.Duration) (projectJoinObservedContext, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	deadline, _ := ctx.Deadline()
	return projectJoinObservedContext{Context: ctx, observed: observed, deadline: deadline}, cancel
}

func (ctx projectJoinObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}
