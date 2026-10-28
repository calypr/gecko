package storageaudit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/redis/go-redis/v9"
)

func TestRedisAuditCachesFailWarmRequestsWhenConnectionCloses(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify Redis-backed request failures")
	}
	t.Run("response snapshot", func(t *testing.T) {
		testRedisResponseCacheWarmReadFailure(t, redisURL)
	})
	t.Run("exact join", func(t *testing.T) {
		testRedisExactJoinWarmReadFailure(t, redisURL)
	})
	t.Run("bucket inventory", func(t *testing.T) {
		testRedisBucketInventoryWarmReadFailure(t, redisURL)
	})
}

func testRedisResponseCacheWarmReadFailure(t *testing.T, redisURL string) {
	t.Helper()
	ctx := context.Background()
	organization, project := redisTestProject(t)
	options := StorageChainAuditOptions{
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ValidationMode:      StorageChainValidationModeInventory,
		BucketInventoryMode: StorageChainBucketModeValidate,
	}
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer(strings.Repeat("a", 64), 100),
	})
	key := storecache.StorageChainAuditResponseCacheKey(organization, project, refName, "", options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, "")
	adminClient := redisTestAdminClient(t, redisURL)
	defer adminClient.Close()
	defer adminClient.Del(ctx, key)
	cache, err := storecache.NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		t.Fatalf("connect response cache: %v", err)
	}
	defer cache.Close()

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-present", Checksum: strings.Repeat("a", 64), Organization: organization, Project: project, Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"},
		}},
		projectScopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: organization, ProjectID: project, Path: "s3://bucket"}},
	}
	service := NewStorageAnalyticsService(backend)
	configureTestAuditResponseCache(service, cache)
	if _, err := service.BuildStorageChainAuditWithOptions(ctx, "Bearer token", organization, project, refName, "", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build warm Redis response snapshot: %v", err)
	}
	recordLoads := backend.listProjectAuditRecordsCalls
	if err := cache.Close(); err != nil {
		t.Fatalf("close response cache client: %v", err)
	}
	if _, err := service.BuildStorageChainAuditWithOptions(ctx, "Bearer token", organization, project, refName, "", mirrorPath, repo, hash, options); err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("expected warm response Redis read error, got %v", err)
	}
	if backend.listProjectAuditRecordsCalls != recordLoads {
		t.Fatalf("expected response cache error to stop fresh rebuild, record loads changed from %d to %d", recordLoads, backend.listProjectAuditRecordsCalls)
	}
}

func testRedisExactJoinWarmReadFailure(t *testing.T, redisURL string) {
	t.Helper()
	organization, project := redisTestProject(t)
	cache, err := storecache.NewRedisStorageExactProjectJoinCache(redisURL)
	if err != nil {
		t.Fatalf("connect exact-join cache: %v", err)
	}
	defer cache.Close()
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-a", Checksum: strings.Repeat("a", 64), Organization: organization, Project: project,
		}},
		usageByObject: map[string]gintegrationsyfon.FileUsage{"obj-a": {ObjectID: "obj-a", DownloadCount: 1}},
	}
	service := NewStorageAnalyticsService(backend)
	service.join.exact = cache
	hash := plumbing.NewHash(strings.Repeat("b", 40))
	inventory := []RepoInventoryFile{{Checksum: strings.Repeat("a", 64)}}
	if _, _, err := service.join.load(context.Background(), "Bearer token", organization, project, hash, inventory, false); err != nil {
		t.Fatalf("build warm Redis exact join: %v", err)
	}
	recordLoads := backend.listProjectAuditRecordsCalls
	if err := cache.Close(); err != nil {
		t.Fatalf("close exact-join cache client: %v", err)
	}
	if _, _, err := service.join.load(context.Background(), "Bearer token", organization, project, hash, inventory, false); err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("expected warm exact-join Redis read error, got %v", err)
	}
	if backend.listProjectAuditRecordsCalls != recordLoads {
		t.Fatalf("expected exact-join cache error to stop fresh rebuilding, record loads changed from %d to %d", recordLoads, backend.listProjectAuditRecordsCalls)
	}
}

func testRedisBucketInventoryWarmReadFailure(t *testing.T, redisURL string) {
	t.Helper()
	organization, project := redisTestProject(t)
	cache, err := storecache.NewRedisProjectBucketInventoryCache(redisURL)
	if err != nil {
		t.Fatalf("connect bucket inventory cache: %v", err)
	}
	defer cache.Close()
	backend := &fakeStorageAnalyticsBackend{
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100,
		}},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainSvc.ProjectBucketCache = cache
	if _, _, _, err := service.LoadCachedProjectBucketValidationInventory(context.Background(), "Bearer token", organization, project, "", false); err != nil {
		t.Fatalf("build warm Redis bucket inventory: %v", err)
	}
	listCalls := backend.listProjectBucketInventoryCalls
	if err := cache.Close(); err != nil {
		t.Fatalf("close bucket inventory cache client: %v", err)
	}
	if _, _, _, err := service.LoadCachedProjectBucketValidationInventory(context.Background(), "Bearer token", organization, project, "", false); err == nil || !storecache.IsCacheOperationError(err) {
		t.Fatalf("expected warm bucket inventory Redis read error, got %v", err)
	}
	if backend.listProjectBucketInventoryCalls != listCalls {
		t.Fatalf("expected bucket cache error to stop fresh Syfon listing, calls changed from %d to %d", listCalls, backend.listProjectBucketInventoryCalls)
	}
}

func TestRedisObservationProgressSurvivesServiceRestartAndRetriesOnlyUnresolved(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify durable storage observation progress")
	}
	organization, project := redisTestProject(t)
	observationCache1, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect first observation cache: %v", err)
	}
	defer observationCache1.Close()
	observationCache2, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect second observation cache: %v", err)
	}
	defer observationCache2.Close()

	records := make(map[string][]sarecord.State, 201)
	for index := 0; index < 201; index++ {
		checksum := fmt.Sprintf("%064x", index+1)
		objectURL := "s3://bucket/object-" + strconv.Itoa(index)
		records[checksum] = []sarecord.State{{ProjectRecord: gintegrationsyfon.ProjectRecord{
			ObjectID:     "object-id-" + strconv.Itoa(index),
			Name:         "object-" + strconv.Itoa(index),
			Checksum:     checksum,
			Organization: organization,
			Project:      project,
			Size:         int64(index + 1),
			AccessURLs:   []string{objectURL},
		}}}
	}
	options := chain.AuditEvidenceOptions{
		Organization:            organization,
		Project:                 project,
		Scopes:                  []domain.StorageBucketScope{{Bucket: "bucket", Organization: organization, ProjectID: project, Path: "s3://bucket"}},
		BucketInventoryComplete: true,
		BucketObjectsByURL:      map[string]gintegrationsyfon.ProjectBucketObject{},
	}
	cleanupKeys := []string{storecache.StorageObservationVersionKey(organization, project)}
	for _, group := range records {
		for _, state := range group {
			url := sarecord.ProbeAccessURLsForRecord(state)[0]
			size := state.Size
			item := gintegrationsyfon.BulkStorageProbeItem{ID: sarecord.StorageProbeRequestKey(url, state.Size, state.Checksum), ObjectURL: url, ExpectedSizeBytes: &size, ExpectedSHA256: state.Checksum}
			keys := storageObservationCacheKeys(options, StorageChainValidationModeMetadata, item)
			cleanupKeys = append(cleanupKeys, keys.observation, keys.retirement)
		}
	}
	adminClient := redisTestAdminClient(t, redisURL)
	defer adminClient.Close()
	defer adminClient.Del(context.Background(), cleanupKeys...)

	var requestedItems atomic.Int64
	var singletonBatches atomic.Int32
	failSingleton := atomic.Bool{}
	failSingleton.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/data/inspect/bulk" {
			http.Error(writer, "unexpected endpoint", http.StatusNotFound)
			return
		}
		var body struct {
			Items []struct {
				ID        string `json:"id"`
				ObjectURL string `json:"object_url"`
			} `json:"items"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		requestedItems.Add(int64(len(body.Items)))
		if len(body.Items) == 1 {
			singletonBatches.Add(1)
			if failSingleton.CompareAndSwap(true, false) {
				http.Error(writer, "temporary batch error", http.StatusServiceUnavailable)
				return
			}
		}
		responseItems := make([]map[string]any, 0, len(body.Items))
		for _, item := range body.Items {
			_, key, _ := sarecord.ParseStorageURL(item.ObjectURL)
			responseItems = append(responseItems, map[string]any{
				"id":         item.ID,
				"object_url": item.ObjectURL,
				"bucket":     "bucket",
				"key":        key,
				"exists":     true,
				"status":     "present",
			})
		}
		if err := json.NewEncoder(writer).Encode(map[string]any{"items": responseItems}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	newService := func(cache storecache.StorageObservationCache) *StorageAnalyticsService {
		manager := gintegrationsyfon.NewManager(server.URL, server.Client())
		backend := &redisProgressStorageAuditBackend{fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{}, manager: manager}
		service := NewStorageAnalyticsService(backend)
		service.observations = cache
		return service
	}
	firstService := newService(observationCache1)
	if _, _, err := firstService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("first service should fail after partial batch progress, got %v", err)
	}
	if requestedItems.Load() != 201 || singletonBatches.Load() != 1 {
		t.Fatalf("first run requested items=%d singleton_batches=%d, want 201 and 1", requestedItems.Load(), singletonBatches.Load())
	}

	secondService := newService(observationCache2)
	attached, _, err := secondService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options)
	if err != nil {
		t.Fatalf("second service should finish from saved positives plus one retry: %v", err)
	}
	if requestedItems.Load() != 202 || singletonBatches.Load() != 2 {
		t.Fatalf("restart requested items=%d singleton_batches=%d, want just one unresolved retry", requestedItems.Load(), singletonBatches.Load())
	}
	probeCount, cacheHitCount := 0, 0
	for _, group := range attached {
		for _, state := range group {
			if len(state.AccessProbes) != 1 {
				t.Fatalf("record %q has %d attached probes, want one", state.ObjectID, len(state.AccessProbes))
			}
			probeCount++
			if state.AccessProbes[0].CacheHit {
				cacheHitCount++
			}
			if strings.TrimSpace(state.AccessProbes[0].ObservedAt) == "" {
				t.Fatalf("record %q lost its original probe observation time", state.ObjectID)
			}
		}
	}
	if probeCount != 201 || cacheHitCount != 200 {
		t.Fatalf("second service returned probes=%d cache_hits=%d, want 201 and 200", probeCount, cacheHitCount)
	}
}

func TestRedisObservationRetirementBlocksOlderInFlightPositive(t *testing.T) {
	redisURL := os.Getenv("GECKO_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set GECKO_TEST_REDIS_URL to verify concurrent observation retirement")
	}
	organization, project := redisTestProject(t)
	cacheOld, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect older-run cache: %v", err)
	}
	defer cacheOld.Close()
	cacheRevalidation, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect revalidation cache: %v", err)
	}
	defer cacheRevalidation.Close()
	cacheFollowup, err := storecache.NewRedisStorageObservationCache(redisURL)
	if err != nil {
		t.Fatalf("connect follow-up cache: %v", err)
	}
	defer cacheFollowup.Close()

	records := auditProbeRecords("s3://bucket/race")
	optionsOld := chain.AuditEvidenceOptions{
		Organization:            organization,
		Project:                 project,
		Scopes:                  []domain.StorageBucketScope{{Bucket: "bucket", Organization: organization, ProjectID: project, Path: "s3://bucket"}},
		BucketInventoryComplete: true,
		BucketObjects:           []gintegrationsyfon.ProjectBucketObject{{ObjectURL: "s3://bucket/race", Bucket: "bucket", Key: "race", SizeBytes: 12, ETag: "etag-old"}},
		BucketObjectsByURL:      map[string]gintegrationsyfon.ProjectBucketObject{"s3://bucket/race": {ObjectURL: "s3://bucket/race", Bucket: "bucket", Key: "race", SizeBytes: 12, ETag: "etag-old"}},
	}
	cleanupKeys := []string{storecache.StorageObservationVersionKey(organization, project)}
	for _, group := range records {
		for _, state := range group {
			item := gintegrationsyfon.BulkStorageProbeItem{ID: sarecord.StorageProbeRequestKey("s3://bucket/race", state.Size, state.Checksum), ObjectURL: "s3://bucket/race", ExpectedSizeBytes: &state.Size, ExpectedSHA256: state.Checksum}
			keys := storageObservationCacheKeys(optionsOld, StorageChainValidationModeMetadata, item)
			cleanupKeys = append(cleanupKeys, keys.observation, keys.retirement)
			state.Size++
			item = gintegrationsyfon.BulkStorageProbeItem{ID: sarecord.StorageListValidationRequestKey("s3://bucket/race", state.Size, sarecord.ExpectedStorageObjectNameForListValidation("s3://bucket/race", state.Name)), ObjectURL: "s3://bucket/race", ExpectedSizeBytes: &state.Size, ExpectedName: sarecord.ExpectedStorageObjectNameForListValidation("s3://bucket/race", state.Name)}
			keys = storageObservationCacheKeys(chain.AuditEvidenceOptions{Organization: organization, Project: project, Scopes: optionsOld.Scopes, BucketInventoryComplete: true}, StorageChainValidationModeList, item)
			cleanupKeys = append(cleanupKeys, keys.observation, keys.retirement)
		}
	}
	adminClient := redisTestAdminClient(t, redisURL)
	defer adminClient.Close()
	defer adminClient.Del(context.Background(), cleanupKeys...)

	olderStarted := make(chan struct{})
	releaseOlder := make(chan struct{})
	var bulkRequests atomic.Int32
	var listRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Items []struct {
				ID        string `json:"id"`
				ObjectURL string `json:"object_url"`
			} `json:"items"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if request.URL.Path == "/data/inspect/bulk" {
			call := bulkRequests.Add(1)
			if call == 1 {
				close(olderStarted)
				<-releaseOlder
			}
		} else if request.URL.Path == "/data/inspect/bulk-list" {
			listRequests.Add(1)
		} else {
			http.Error(writer, "unexpected endpoint", http.StatusNotFound)
			return
		}
		responseItems := make([]map[string]any, 0, len(body.Items))
		for _, item := range body.Items {
			_, key, _ := sarecord.ParseStorageURL(item.ObjectURL)
			response := map[string]any{"id": item.ID, "object_url": item.ObjectURL, "bucket": "bucket", "key": key}
			if request.URL.Path == "/data/inspect/bulk-list" {
				response["exists"] = false
				response["status"] = "not_found"
				response["error_kind"] = "object_not_found"
			} else {
				response["exists"] = true
				response["status"] = "present"
			}
			responseItems = append(responseItems, response)
		}
		if err := json.NewEncoder(writer).Encode(map[string]any{"items": responseItems}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()
	newService := func(cache storecache.StorageObservationCache) *StorageAnalyticsService {
		manager := gintegrationsyfon.NewManager(server.URL, server.Client())
		service := NewStorageAnalyticsService(&redisProgressStorageAuditBackend{fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{}, manager: manager})
		service.observations = cache
		return service
	}
	olderService := newService(cacheOld)
	olderDone := make(chan error, 1)
	go func() {
		_, _, err := olderService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, optionsOld)
		olderDone <- err
	}()
	<-olderStarted

	missingRecords := auditProbeRecords("s3://bucket/race")
	for checksum, group := range missingRecords {
		group[0].Size++
		missingRecords[checksum] = group
	}
	optionsRevalidation := chain.AuditEvidenceOptions{
		Organization:             organization,
		Project:                  project,
		Scopes:                   optionsOld.Scopes,
		BucketInventoryComplete:  true,
		ForceStorageRevalidation: true,
	}
	revalidationService := newService(cacheRevalidation)
	if _, _, err := revalidationService.attachStorageListValidationsForAudit(context.Background(), "Bearer token", missingRecords, missingRecords, optionsRevalidation); err != nil {
		t.Fatalf("force revalidation to missing: %v", err)
	}
	close(releaseOlder)
	if err := <-olderDone; err != nil {
		t.Fatalf("older positive observation: %v", err)
	}

	followupService := newService(cacheFollowup)
	followup, _, err := followupService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, optionsOld)
	if err != nil {
		t.Fatalf("follow up after forced missing observation: %v", err)
	}
	if bulkRequests.Load() != 2 || listRequests.Load() != 1 {
		t.Fatalf("older positive resurrected after revalidation: bulk requests=%d list requests=%d", bulkRequests.Load(), listRequests.Load())
	}
	for _, group := range followup {
		for _, state := range group {
			if len(state.AccessProbes) != 1 || state.AccessProbes[0].CacheHit {
				t.Fatalf("follow-up reused the retired positive observation: %+v", state.AccessProbes)
			}
		}
	}
	reused, _, err := followupService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, optionsOld)
	if err != nil {
		t.Fatalf("reuse observation written after retirement: %v", err)
	}
	if bulkRequests.Load() != 2 {
		t.Fatalf("later positive observation did not become reusable, bulk requests=%d", bulkRequests.Load())
	}
	for _, group := range reused {
		for _, state := range group {
			if len(state.AccessProbes) != 1 || !state.AccessProbes[0].CacheHit {
				t.Fatalf("later positive observation was not reused: %+v", state.AccessProbes)
			}
		}
	}
}

type redisProgressStorageAuditBackend struct {
	*fakeStorageAnalyticsBackend
	manager *gintegrationsyfon.Manager
}

func (backend *redisProgressStorageAuditBackend) BulkProbeStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.manager.BulkProbeStorageObjectsWithProgress(ctx, authorizationHeader, items, onBatch)
}

func (backend *redisProgressStorageAuditBackend) BulkListStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.manager.BulkListStorageObjectsWithProgress(ctx, authorizationHeader, items, onBatch)
}

func redisTestProject(t *testing.T) (string, string) {
	t.Helper()
	return fmt.Sprintf("redis-test-%d", time.Now().UnixNano()), "project"
}

func redisTestAdminClient(t *testing.T, redisURL string) *redis.Client {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse test Redis URL: %v", err)
	}
	return redis.NewClient(options)
}
