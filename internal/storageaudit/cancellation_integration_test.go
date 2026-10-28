package storageaudit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func TestRedisProjectAuditCoordinatesIndependentServicesAndProjects(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis-backed project audit coordination")
	}

	organization, project := redisTestProject(t)
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer(strings.Repeat("a", 64), 100),
	})
	tracker := newProjectAuditCallTracker()
	releaseRoot := make(chan struct{})
	var releaseRootOnce sync.Once
	defer releaseRootOnce.Do(func() { close(releaseRoot) })

	rootStarted := make(chan struct{})
	rootBackend := newCoordinatedAuditBackend(organization, project, tracker, releaseRoot, rootStarted)
	rootService := newRedisStorageAuditService(t, redisURL, rootBackend, nil)
	rootDone := startStorageChainAudit(t, rootService, context.Background(), organization, project, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
		ForceAuditRefresh:   true,
	})
	waitSignal(t, rootStarted, "root provider audit")

	subpathAttempted := make(chan struct{})
	subpathBackend := newCoordinatedAuditBackend(organization, project, tracker, closedSignal(), make(chan struct{}))
	subpathService := newRedisStorageAuditService(t, redisURL, subpathBackend, subpathAttempted)
	subpathDone := startStorageChainAudit(t, subpathService, context.Background(), organization, project, "data", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
		FindingKind:         "missing_storage",
		FindingLimit:        1,
	})
	waitSignal(t, subpathAttempted, "subpath project lease attempt")

	otherProject := project + "-independent"
	otherStarted := make(chan struct{})
	otherBackend := newCoordinatedAuditBackend(organization, otherProject, tracker, closedSignal(), otherStarted)
	otherService := newRedisStorageAuditService(t, redisURL, otherBackend, nil)
	otherDone := startStorageChainAudit(t, otherService, context.Background(), organization, otherProject, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
		ForceAuditRefresh:   true,
	})
	waitSignal(t, otherStarted, "independent project provider audit")
	if active := tracker.maximumCalls(); active != 2 {
		t.Fatalf("independent project did not progress alongside the root refresh: maximum concurrent provider audits=%d, want 2", active)
	}

	select {
	case <-subpathBackend.started:
		t.Fatal("queued subpath request started a second provider audit for the same project")
	case <-time.After(400 * time.Millisecond):
	}

	releaseRootOnce.Do(func() { close(releaseRoot) })
	waitAuditResult(t, rootDone, "root refresh")
	waitAuditResult(t, otherDone, "independent project refresh")
	subpathResult := waitAuditResult(t, subpathDone, "subpath projection")
	if subpathResult.response.PathPrefix != "data" || !subpathResult.response.Summary.AuditCacheHit || !strings.Contains(subpathResult.response.Summary.AuditCacheSource, ":root") {
		t.Fatalf("queued subpath did not use the published root response: path=%q cache_hit=%t source=%q", subpathResult.response.PathPrefix, subpathResult.response.Summary.AuditCacheHit, subpathResult.response.Summary.AuditCacheSource)
	}
	if got := subpathBackend.callCount(); got != 0 {
		t.Fatalf("queued subpath performed %d provider audit calls after the root snapshot was published, want 0", got)
	}
	if got := rootBackend.callCount(); got != 1 {
		t.Fatalf("root refresh made %d provider audit calls, want 1", got)
	}
	if got := otherBackend.callCount(); got != 1 {
		t.Fatalf("independent project made %d provider audit calls, want 1", got)
	}
	if got := tracker.maximumForProject(organization, project); got != 1 {
		t.Fatalf("same project had %d concurrent provider audits, want 1", got)
	}
}

func TestRedisProjectAuditCancellationReleasesLeaseAfterWorkDrains(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis-backed project audit cancellation")
	}

	organization, project := redisTestProject(t)
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer(strings.Repeat("b", 64), 100),
	})
	tracker := newProjectAuditCallTracker()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirst) })
	firstBackend := newCoordinatedAuditBackend(organization, project, tracker, releaseFirst, firstStarted)
	firstService := newRedisStorageAuditService(t, redisURL, firstBackend, nil)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	t.Cleanup(cancelFirst)
	firstDone := startStorageChainAudit(t, firstService, firstCtx, organization, project, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
		ForceAuditRefresh:   true,
	})
	waitSignal(t, firstStarted, "cancelable provider audit")

	secondAttempted := make(chan struct{})
	secondStarted := make(chan struct{})
	secondBackend := newCoordinatedAuditBackend(organization, project, tracker, closedSignal(), secondStarted)
	secondService := newRedisStorageAuditService(t, redisURL, secondBackend, secondAttempted)
	secondDone := startStorageChainAudit(t, secondService, context.Background(), organization, project, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
		ForceAuditRefresh:   true,
	})
	waitSignal(t, secondAttempted, "retry project lease attempt")

	select {
	case <-secondStarted:
		t.Fatal("retry started before the canceled project audit drained")
	case <-time.After(300 * time.Millisecond):
	}
	cancelFirst()
	firstResult := receiveAuditResult(t, firstDone, "canceled provider audit")
	if !errors.Is(firstResult.err, context.Canceled) {
		t.Fatalf("canceled audit returned %v, want context.Canceled", firstResult.err)
	}
	waitSignal(t, secondStarted, "retry provider audit after cancellation")
	waitAuditResult(t, secondDone, "retry after cancellation")
	if got := tracker.maximumForProject(organization, project); got != 1 {
		t.Fatalf("same project had %d concurrent provider audits across cancellation and retry, want 1", got)
	}
	if got := secondBackend.callCount(); got != 1 {
		t.Fatalf("retry made %d provider audit calls, want 1", got)
	}
}

func TestRedisAuditProgressSurvivesCancellationAndRetriesOnlyUnresolvedTarget(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis-backed storage audit progress")
	}

	organization, project := redisTestProject(t)
	repoFiles := make(map[string]string, 201)
	projectRecords := make([]gintegrationsyfon.ProjectRecord, 0, 201)
	for index := 0; index < 201; index++ {
		checksum := fmt.Sprintf("%064x", index+1)
		objectURL := fmt.Sprintf("s3://bucket/object-%d", index)
		repoFiles[fmt.Sprintf("data/object-%d.txt", index)] = lfsPointer(checksum, int64(index+1))
		projectRecords = append(projectRecords, gintegrationsyfon.ProjectRecord{
			ObjectID:     fmt.Sprintf("object-id-%d", index),
			Name:         fmt.Sprintf("object-%d.txt", index),
			Checksum:     checksum,
			Organization: organization,
			Project:      project,
			Size:         int64(index + 1),
			AccessURLs:   []string{objectURL},
		})
	}
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, repoFiles)
	scopes := []domain.StorageBucketScope{{Bucket: "bucket", Organization: organization, ProjectID: project, Path: "s3://bucket"}}
	checkpointed := make(chan struct{})
	singletonStarted := make(chan struct{})
	unresolvedTarget := make(chan string, 1)
	var firstRequested atomic.Int64
	var singletonOnce sync.Once
	firstServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Items []integrationProbeTarget `json:"items"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		firstRequested.Add(int64(len(body.Items)))
		if len(body.Items) == 1 {
			singletonOnce.Do(func() {
				unresolvedTarget <- body.Items[0].ObjectURL
				close(singletonStarted)
			})
			<-request.Context().Done()
			return
		}
		writeIntegrationProbeResults(writer, body.Items)
	}))
	t.Cleanup(firstServer.Close)
	firstManager := gintegrationsyfon.NewManager(firstServer.URL, firstServer.Client())
	firstBackend := &checkpointObservingRedisProgressBackend{
		redisProgressStorageAuditBackend: &redisProgressStorageAuditBackend{
			fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{projectRecords: projectRecords, projectScopes: scopes},
			manager:                     firstManager,
		},
		checkpointed: checkpointed,
	}
	firstService := newRedisStorageAuditService(t, redisURL, firstBackend, nil)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)
	firstDone := startStorageChainAudit(t, firstService, requestCtx, organization, project, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeFull,
		ValidationMode:      StorageChainValidationModeMetadata,
		BucketInventoryMode: StorageChainBucketModeValidate,
	})
	waitSignal(t, checkpointed, "200 completed provider targets checkpointed")
	waitSignal(t, singletonStarted, "remaining provider target still running")
	cancelRequest()
	firstResult := receiveAuditResult(t, firstDone, "aborted 201-target audit")
	if !errors.Is(firstResult.err, context.Canceled) {
		t.Fatalf("aborted audit returned %v, want context.Canceled", firstResult.err)
	}
	if got := firstRequested.Load(); got != 201 {
		t.Fatalf("first service submitted %d targets before cancellation, want 201", got)
	}

	secondSubmitted := make(chan []string, 1)
	secondServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Items []integrationProbeTarget `json:"items"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		urls := make([]string, 0, len(body.Items))
		for _, item := range body.Items {
			urls = append(urls, item.ObjectURL)
		}
		secondSubmitted <- urls
		writeIntegrationProbeResults(writer, body.Items)
	}))
	t.Cleanup(secondServer.Close)
	secondBackend := &redisProgressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{projectRecords: projectRecords, projectScopes: scopes},
		manager:                     gintegrationsyfon.NewManager(secondServer.URL, secondServer.Client()),
	}
	secondService := newRedisStorageAuditService(t, redisURL, secondBackend, nil)
	secondResult := waitAuditResult(t, startStorageChainAudit(t, secondService, context.Background(), organization, project, "", repo, mirrorPath, refName, hash, StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeFull,
		ValidationMode:      StorageChainValidationModeMetadata,
		BucketInventoryMode: StorageChainBucketModeValidate,
	}), "resumed 201-target audit")
	if secondResult.err != nil {
		t.Fatalf("resumed audit failed: %v", secondResult.err)
	}
	var gotSubmitted []string
	select {
	case gotSubmitted = <-secondSubmitted:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed provider call was not observed")
	}
	wantUnresolved := <-unresolvedTarget
	if len(gotSubmitted) != 1 || gotSubmitted[0] != wantUnresolved {
		t.Fatalf("new service submitted %v, want only unresolved target %q", gotSubmitted, wantUnresolved)
	}
}

type projectAuditCallTracker struct {
	mu           sync.Mutex
	active       int
	maximum      int
	activeByKey  map[string]int
	maximumByKey map[string]int
}

func newProjectAuditCallTracker() *projectAuditCallTracker {
	return &projectAuditCallTracker{activeByKey: map[string]int{}, maximumByKey: map[string]int{}}
}

func (tracker *projectAuditCallTracker) enter(organization string, project string) {
	key := organization + "\x00" + project
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.active++
	if tracker.active > tracker.maximum {
		tracker.maximum = tracker.active
	}
	tracker.activeByKey[key]++
	if tracker.activeByKey[key] > tracker.maximumByKey[key] {
		tracker.maximumByKey[key] = tracker.activeByKey[key]
	}
}

func (tracker *projectAuditCallTracker) leave(organization string, project string) {
	key := organization + "\x00" + project
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.active--
	tracker.activeByKey[key]--
}

func (tracker *projectAuditCallTracker) maximumCalls() int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.maximum
}

func (tracker *projectAuditCallTracker) maximumForProject(organization string, project string) int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.maximumByKey[organization+"\x00"+project]
}

type coordinatedAuditBackend struct {
	*fakeStorageAnalyticsBackend
	tracker   *projectAuditCallTracker
	release   <-chan struct{}
	started   chan struct{}
	startOnce sync.Once
}

func newCoordinatedAuditBackend(organization string, project string, tracker *projectAuditCallTracker, release <-chan struct{}, started chan struct{}) *coordinatedAuditBackend {
	return &coordinatedAuditBackend{
		fakeStorageAnalyticsBackend: auditFixtureBackend(organization, project, "a"),
		tracker:                     tracker,
		release:                     release,
		started:                     started,
	}
}

func (backend *coordinatedAuditBackend) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error) {
	backend.tracker.enter(organization, project)
	backend.startOnce.Do(func() { close(backend.started) })
	select {
	case <-backend.release:
	case <-ctx.Done():
	}
	backend.tracker.leave(organization, project)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return backend.fakeStorageAnalyticsBackend.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, pathPrefix)
}

func (backend *coordinatedAuditBackend) callCount() int {
	backend.fakeStorageAnalyticsBackend.mu.Lock()
	defer backend.fakeStorageAnalyticsBackend.mu.Unlock()
	return backend.fakeStorageAnalyticsBackend.listProjectAuditRecordsCalls
}

type observingProjectAuditCoordinator struct {
	storecache.ProjectAuditCoordinator
	attempted chan struct{}
	once      sync.Once
}

func (coordinator *observingProjectAuditCoordinator) AcquireProject(ctx context.Context, organization string, project string) (storecache.ProjectAuditLease, error) {
	coordinator.once.Do(func() { close(coordinator.attempted) })
	return coordinator.ProjectAuditCoordinator.AcquireProject(ctx, organization, project)
}

type storageAuditResult struct {
	response *ChainAuditResponse
	err      error
}

func newRedisStorageAuditService(t *testing.T, redisURL string, backend storageAnalyticsBackend, attempted chan struct{}) *StorageAnalyticsService {
	t.Helper()
	response, err := storecache.NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		t.Fatalf("connect response cache: %v", err)
	}
	t.Cleanup(func() {
		if err := response.Close(); err != nil {
			t.Errorf("close response cache: %v", err)
		}
	})
	exactJoin, err := storecache.NewRedisStorageExactProjectJoinCache(redisURL)
	if err != nil {
		t.Fatalf("connect exact-join cache: %v", err)
	}
	t.Cleanup(func() {
		if err := exactJoin.Close(); err != nil {
			t.Errorf("close exact-join cache: %v", err)
		}
	})
	bucketInventory, err := storecache.NewRedisProjectBucketInventoryCache(redisURL)
	if err != nil {
		t.Fatalf("connect bucket inventory cache: %v", err)
	}
	t.Cleanup(func() {
		if err := bucketInventory.Close(); err != nil {
			t.Errorf("close bucket inventory cache: %v", err)
		}
	})
	observations, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect observation cache: %v", err)
	}
	t.Cleanup(func() {
		if err := observations.Close(); err != nil {
			t.Errorf("close observation cache: %v", err)
		}
	})
	coordinator, err := storecache.NewRedisProjectAuditCoordinator(redisURL)
	if err != nil {
		t.Fatalf("connect project audit coordinator: %v", err)
	}
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Errorf("close project audit coordinator: %v", err)
		}
	})
	var projectCoordinator storecache.ProjectAuditCoordinator = coordinator
	if attempted != nil {
		projectCoordinator = &observingProjectAuditCoordinator{ProjectAuditCoordinator: coordinator, attempted: attempted}
	}
	service, err := NewStorageAnalyticsServiceWithCaches(backend, StorageAuditCaches{
		Response:        response,
		ExactJoin:       exactJoin,
		BucketInventory: bucketInventory,
		Observations:    observations,
		Coordinator:     projectCoordinator,
	})
	if err != nil {
		t.Fatalf("construct Redis-backed storage audit service: %v", err)
	}
	return service
}

func auditFixtureBackend(organization string, project string, checksumPrefix string) *fakeStorageAnalyticsBackend {
	checksum := strings.Repeat(checksumPrefix, 64)
	objectURL := "s3://bucket/data/present.txt"
	return &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID:     "obj-present-" + project,
			Name:         "present.txt",
			Checksum:     checksum,
			Organization: organization,
			Project:      project,
			Size:         100,
			AccessURLs:   []string{objectURL},
		}},
		projectScopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: organization, ProjectID: project, Path: "s3://bucket"}},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: objectURL,
			Bucket:    "bucket",
			Key:       "data/present.txt",
			Path:      "data/present.txt",
			SizeBytes: 100,
		}},
	}
}

func startStorageChainAudit(t *testing.T, service *StorageAnalyticsService, parent context.Context, organization string, project string, gitSubpath string, repo *gogit.Repository, mirrorPath string, refName string, hash plumbing.Hash, options StorageChainAuditOptions) <-chan storageAuditResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	t.Cleanup(cancel)
	done := make(chan storageAuditResult, 1)
	go func() {
		response, err := service.BuildStorageChainAuditWithOptions(ctx, "Bearer token", organization, project, refName, gitSubpath, mirrorPath, repo, hash, options)
		done <- storageAuditResult{response: response, err: err}
	}()
	return done
}

func waitAuditResult(t *testing.T, done <-chan storageAuditResult, label string) storageAuditResult {
	t.Helper()
	result := receiveAuditResult(t, done, label)
	if result.err != nil {
		t.Fatalf("%s failed: %v", label, result.err)
	}
	return result
}

func receiveAuditResult(t *testing.T, done <-chan storageAuditResult, label string) storageAuditResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(15 * time.Second):
		t.Fatalf("%s did not finish", label)
		return storageAuditResult{}
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func closedSignal() <-chan struct{} {
	closed := make(chan struct{})
	close(closed)
	return closed
}

type checkpointObservingRedisProgressBackend struct {
	*redisProgressStorageAuditBackend
	checkpointed   chan struct{}
	checkpointOnce sync.Once
}

func (backend *checkpointObservingRedisProgressBackend) BulkProbeStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.manager.BulkProbeStorageObjectsWithProgress(ctx, authorizationHeader, items, func(batchItems []gintegrationsyfon.BulkStorageProbeItem, results []gintegrationsyfon.BulkStorageProbeResult) error {
		if err := onBatch(batchItems, results); err != nil {
			return err
		}
		if len(batchItems) == 200 {
			backend.checkpointOnce.Do(func() { close(backend.checkpointed) })
		}
		return nil
	})
}

func (backend *checkpointObservingRedisProgressBackend) BulkListStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.manager.BulkListStorageObjectsWithProgress(ctx, authorizationHeader, items, func(batchItems []gintegrationsyfon.BulkStorageProbeItem, results []gintegrationsyfon.BulkStorageProbeResult) error {
		if err := onBatch(batchItems, results); err != nil {
			return err
		}
		if len(batchItems) == 200 {
			backend.checkpointOnce.Do(func() { close(backend.checkpointed) })
		}
		return nil
	})
}

type integrationProbeTarget struct {
	ID        string `json:"id"`
	ObjectURL string `json:"object_url"`
}

func writeIntegrationProbeResults(writer http.ResponseWriter, items []integrationProbeTarget) {
	results := make([]map[string]any, 0, len(items))
	for _, item := range items {
		bucket, key, _ := record.ParseStorageURL(item.ObjectURL)
		results = append(results, map[string]any{
			"id":         item.ID,
			"object_url": item.ObjectURL,
			"bucket":     bucket,
			"key":        key,
			"exists":     true,
			"status":     "present",
		})
	}
	if err := json.NewEncoder(writer).Encode(map[string]any{"items": results}); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}
