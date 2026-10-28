package storageaudit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

func TestStorageObservationInventoryFingerprintRequiresCompleteScopeCoverage(t *testing.T) {
	options := chain.AuditEvidenceOptions{
		Organization:            "org",
		Project:                 "proj",
		Scopes:                  []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket/project"}},
		BucketInventoryComplete: true,
		BucketObjectsByURL: map[string]gintegrationsyfon.ProjectBucketObject{
			"s3://bucket/project/file": {ObjectURL: "s3://bucket/project/file", Bucket: "bucket", Key: "project/file", SizeBytes: 12, ETag: "etag-1"},
		},
	}
	if got := storageObservationInventoryFingerprint(options, "s3://bucket/project/file"); got == "" {
		t.Fatal("expected scoped listed object metadata to fingerprint")
	}
	if got := storageObservationInventoryFingerprint(options, "s3://bucket/outside/file"); got != "" {
		t.Fatalf("outside-scope target received an inventory fingerprint: %q", got)
	}
	options.BucketObjectsByURL = nil
	if got := storageObservationInventoryFingerprint(options, "s3://bucket/project/absent"); got == "" {
		t.Fatal("expected complete scoped listing absence sentinel")
	}
	options.BucketPathPrefix = "project/nested"
	if got := storageObservationInventoryFingerprint(options, "s3://bucket/project/absent"); got != "" {
		t.Fatalf("target outside the listed prefix received an absence sentinel: %q", got)
	}
	options.BucketInventoryComplete = false
	if got := storageObservationInventoryFingerprint(options, "s3://bucket/project/absent"); got != "" {
		t.Fatalf("incomplete inventory received an absence fingerprint: %q", got)
	}
}

func TestSuccessfulStorageObservationRequiresExplicitPresentStatus(t *testing.T) {
	for _, status := range []string{"present", "PRESENT"} {
		if !successfulStorageObservation(gintegrationsyfon.BulkStorageProbeResult{Exists: true, Status: status}) {
			t.Fatalf("present status %q was not accepted", status)
		}
	}
	for _, status := range []string{"", "unknown", "error", "not_found", "unsupported"} {
		if successfulStorageObservation(gintegrationsyfon.BulkStorageProbeResult{Exists: true, Status: status}) {
			t.Fatalf("non-present status %q was accepted", status)
		}
	}
	if successfulStorageObservation(gintegrationsyfon.BulkStorageProbeResult{Exists: true, Status: "present", Error: "partial metadata failure"}) {
		t.Fatal("present result with an error was accepted")
	}
}

func TestCompletedObservationCheckpointSurvivesWorkCancellation(t *testing.T) {
	cache := newTestObservationCache()
	version, err := cache.NextVersion(context.Background(), "org", "proj")
	if err != nil {
		t.Fatalf("allocate observation version: %v", err)
	}
	item := gintegrationsyfon.BulkStorageProbeItem{ID: "request-1", ObjectURL: "s3://bucket/file"}
	options := chain.AuditEvidenceOptions{Organization: "org", Project: "proj"}
	keys := storageObservationCacheKeys(options, StorageChainValidationModeMetadata, item)
	if err := cache.EnsureFences(context.Background(), "org", "proj", version, []string{keys.retirement}); err != nil {
		t.Fatalf("initialize observation fence: %v", err)
	}
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	service.observations = cache
	workCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.checkpointStorageObservationBatch(workCtx, []gintegrationsyfon.BulkStorageProbeItem{item}, []gintegrationsyfon.BulkStorageProbeResult{presentProbe(item, false)}, StorageChainValidationModeMetadata, options, map[string]storageObservationKeys{item.ID: keys}, version); err != nil {
		t.Fatalf("checkpoint a completed batch after cancellation: %v", err)
	}
	observations, err := cache.GetMany(context.Background(), "org", "proj", version, []string{keys.observation})
	if err != nil {
		t.Fatalf("read observation checkpoint: %v", err)
	}
	if got := observations[keys.observation]; !successfulStorageObservation(got.Result) {
		t.Fatalf("completed batch was not retained after cancellation: %+v", got)
	}
}

func TestAuditProgressCheckpointsPositivesAndRetriesOnlyOmittedTargets(t *testing.T) {
	cache := newTestObservationCache()
	firstRequest := true
	var successfulURL string
	var backendSubmitted [][]gintegrationsyfon.BulkStorageProbeItem
	backend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			backendSubmitted = append(backendSubmitted, append([]gintegrationsyfon.BulkStorageProbeItem(nil), items...))
			if firstRequest {
				firstRequest = false
				successfulURL = items[0].ObjectURL
				if err := onBatch(items, []gintegrationsyfon.BulkStorageProbeResult{presentProbe(items[0], true)}); err != nil {
					return nil, err
				}
				return nil, errors.New("probe batch failed")
			}
			results := make([]gintegrationsyfon.BulkStorageProbeResult, len(items))
			for index, item := range items {
				results[index] = presentProbe(item, false)
			}
			if err := onBatch(items, results); err != nil {
				return nil, err
			}
			return results, nil
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.observations = cache
	records := auditProbeRecords("s3://bucket/a", "s3://bucket/b")
	options := chain.AuditEvidenceOptions{Organization: "org", Project: "proj", Scopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}}}
	if _, _, err := service.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options); err == nil || !strings.Contains(err.Error(), "probe syfon storage objects") {
		t.Fatalf("expected first run to fail after checkpointing its completed batch, got %v", err)
	}
	if len(backendSubmitted) != 1 || len(backendSubmitted[0]) != 2 {
		t.Fatalf("first run submitted %d calls with item counts %v, want both targets once", len(backendSubmitted), submittedCounts(backendSubmitted))
	}
	unresolvedURL := ""
	for _, item := range backendSubmitted[0] {
		if item.ObjectURL != successfulURL {
			unresolvedURL = item.ObjectURL
		}
	}
	secondRecords, _, err := service.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options)
	if err != nil {
		t.Fatalf("retry unresolved target: %v", err)
	}
	if len(backendSubmitted) != 2 || len(backendSubmitted[1]) != 1 || backendSubmitted[1][0].ObjectURL != unresolvedURL {
		t.Fatalf("retry submitted %v, want only unresolved target %q", submittedURLs(backendSubmitted), unresolvedURL)
	}
	cacheHits := 0
	probeCount := 0
	for _, group := range secondRecords {
		for _, record := range group {
			for _, probe := range record.AccessProbes {
				probeCount++
				if probe.CacheHit {
					cacheHits++
				}
			}
		}
	}
	if probeCount != 2 || cacheHits != 1 {
		t.Fatalf("retry returned probe_count=%d cache_hits=%d, want two results with one cached", probeCount, cacheHits)
	}
}

func TestAuditObservationCacheErrorStopsWithoutBackendFallback(t *testing.T) {
	cache := newTestObservationCache()
	cache.getErr = errors.New("Redis unavailable")
	var backendCalls int
	backend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			backendCalls++
			return nil, nil
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.observations = cache
	records := auditProbeRecords("s3://bucket/a")
	_, _, err := service.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, chain.AuditEvidenceOptions{Organization: "org", Project: "proj"})
	if err == nil || !storecache.IsCacheOperationError(err) || backendCalls != 0 {
		t.Fatalf("cache failure did not stop the audit before storage: err=%v backend_calls=%d", err, backendCalls)
	}
}

func TestOlderObservationCannotResurrectTargetAfterForceRevalidation(t *testing.T) {
	cache := newTestObservationCache()
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	oldBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			close(oldStarted)
			<-releaseOld
			results := []gintegrationsyfon.BulkStorageProbeResult{presentProbe(items[0], false)}
			if err := onBatch(items, results); err != nil {
				return nil, err
			}
			return results, nil
		},
	}
	oldService := NewStorageAnalyticsService(oldBackend)
	oldService.observations = cache
	records := auditProbeRecords("s3://bucket/race")
	oldOptions := chain.AuditEvidenceOptions{
		Organization:            "org",
		Project:                 "proj",
		Scopes:                  []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}},
		BucketInventoryComplete: true,
		BucketObjects:           []gintegrationsyfon.ProjectBucketObject{{ObjectURL: "s3://bucket/race", Bucket: "bucket", Key: "race", SizeBytes: 12, ETag: "etag-old"}},
	}
	oldOptions.BucketObjectsByURL = map[string]gintegrationsyfon.ProjectBucketObject{"s3://bucket/race": oldOptions.BucketObjects[0]}
	oldDone := make(chan error, 1)
	go func() {
		_, _, err := oldService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, oldOptions)
		oldDone <- err
	}()
	<-oldStarted

	missingBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			result := gintegrationsyfon.BulkStorageProbeResult{ID: items[0].ID, ObjectURL: items[0].ObjectURL, Bucket: "bucket", Key: "race", Exists: false, Status: "not_found", ErrorKind: "object_not_found", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if err := onBatch(items, []gintegrationsyfon.BulkStorageProbeResult{result}); err != nil {
				return nil, err
			}
			return []gintegrationsyfon.BulkStorageProbeResult{result}, nil
		},
	}
	missingService := NewStorageAnalyticsService(missingBackend)
	missingService.observations = cache
	missingOptions := oldOptions
	missingOptions.BucketObjects = nil
	missingOptions.BucketObjectsByURL = map[string]gintegrationsyfon.ProjectBucketObject{}
	missingOptions.ForceStorageRevalidation = true
	missingRecords := auditProbeRecords("s3://bucket/race")
	for checksum, group := range missingRecords {
		group[0].Size++
		missingRecords[checksum] = group
	}
	if _, _, err := missingService.attachStorageListValidationsForAudit(context.Background(), "Bearer token", missingRecords, missingRecords, missingOptions); err != nil {
		t.Fatalf("full storage revalidation reporting missing target: %v", err)
	}
	close(releaseOld)
	if err := <-oldDone; err != nil {
		t.Fatalf("older ordinary request: %v", err)
	}

	var followupCalls int
	followupBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			followupCalls += len(items)
			results := []gintegrationsyfon.BulkStorageProbeResult{presentProbe(items[0], false)}
			if err := onBatch(items, results); err != nil {
				return nil, err
			}
			return results, nil
		},
	}
	followupService := NewStorageAnalyticsService(followupBackend)
	followupService.observations = cache
	if _, _, err := followupService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, oldOptions); err != nil {
		t.Fatalf("follow-up ordinary request after retirement: %v", err)
	}
	if followupCalls != 1 {
		t.Fatalf("older positive resurrected after forced missing observation; follow-up submitted %d targets, want 1", followupCalls)
	}
}

func TestOrdinaryMissingResponseRetiresOlderExpectationAcrossOperations(t *testing.T) {
	cache := newTestObservationCache()
	records := auditProbeRecords("s3://bucket/object")
	options := chain.AuditEvidenceOptions{
		Organization:            "org",
		Project:                 "proj",
		Scopes:                  []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}},
		BucketInventoryComplete: true,
		BucketObjectsByURL:      map[string]gintegrationsyfon.ProjectBucketObject{},
	}
	positiveBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			results := []gintegrationsyfon.BulkStorageProbeResult{presentProbe(items[0], false)}
			if err := onBatch(items, results); err != nil {
				return nil, err
			}
			return results, nil
		},
	}
	positiveService := NewStorageAnalyticsService(positiveBackend)
	positiveService.observations = cache
	if _, _, err := positiveService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options); err != nil {
		t.Fatalf("store initial positive observation: %v", err)
	}

	missingRecords := auditProbeRecords("s3://bucket/object")
	for checksum, group := range missingRecords {
		group[0].Size++
		missingRecords[checksum] = group
	}
	missingBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			result := gintegrationsyfon.BulkStorageProbeResult{ID: items[0].ID, ObjectURL: items[0].ObjectURL, Bucket: "bucket", Key: "object", Status: "not_found", ErrorKind: "object_not_found", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if err := onBatch(items, []gintegrationsyfon.BulkStorageProbeResult{result}); err != nil {
				return nil, err
			}
			return []gintegrationsyfon.BulkStorageProbeResult{result}, nil
		},
	}
	missingService := NewStorageAnalyticsService(missingBackend)
	missingService.observations = cache
	if _, _, err := missingService.attachStorageListValidationsForAudit(context.Background(), "Bearer token", missingRecords, missingRecords, options); err != nil {
		t.Fatalf("record ordinary missing response: %v", err)
	}

	var followupCalls int
	followupBackend := &progressStorageAuditBackend{
		fakeStorageAnalyticsBackend: &fakeStorageAnalyticsBackend{},
		probe: func(items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
			followupCalls += len(items)
			results := []gintegrationsyfon.BulkStorageProbeResult{presentProbe(items[0], false)}
			if err := onBatch(items, results); err != nil {
				return nil, err
			}
			return results, nil
		},
	}
	followupService := NewStorageAnalyticsService(followupBackend)
	followupService.observations = cache
	if _, _, err := followupService.attachStorageProbesForAudit(context.Background(), "Bearer token", records, records, options); err != nil {
		t.Fatalf("reverify after ordinary missing response: %v", err)
	}
	if followupCalls != 1 {
		t.Fatalf("ordinary missing response failed to retire older expectation; follow-up submitted %d targets, want 1", followupCalls)
	}
}

type progressStorageAuditBackend struct {
	*fakeStorageAnalyticsBackend
	probe func([]gintegrationsyfon.BulkStorageProbeItem, func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error)
}

func (backend *progressStorageAuditBackend) BulkProbeStorageObjectsWithProgress(_ context.Context, _ string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.probe(items, onBatch)
}

func (backend *progressStorageAuditBackend) BulkListStorageObjectsWithProgress(_ context.Context, _ string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return backend.probe(items, onBatch)
}

func auditProbeRecords(objectURLs ...string) map[string][]sarecord.State {
	records := make(map[string][]sarecord.State, len(objectURLs))
	for index, objectURL := range objectURLs {
		checksum := fmt.Sprintf("%064x", index+1)
		objectID := fmt.Sprintf("object-%d", index)
		records[checksum] = []sarecord.State{{ProjectRecord: gintegrationsyfon.ProjectRecord{ObjectID: objectID, Name: fmt.Sprintf("file-%d", index), Checksum: checksum, Size: 12, AccessURLs: []string{objectURL}}}}
	}
	return records
}

func presentProbe(item gintegrationsyfon.BulkStorageProbeItem, mismatch bool) gintegrationsyfon.BulkStorageProbeResult {
	_, key, _ := sarecord.ParseStorageURL(item.ObjectURL)
	sizeMatch := !mismatch
	validationStatus := "matched"
	var mismatches []string
	if mismatch {
		validationStatus = "mismatched"
		mismatches = []string{"size_mismatch"}
	}
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   item.ID,
		ObjectURL:            item.ObjectURL,
		Bucket:               "bucket",
		Key:                  key,
		Exists:               true,
		Status:               "present",
		ObservedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		ValidationStatus:     validationStatus,
		SizeMatch:            &sizeMatch,
		ValidationMismatches: mismatches,
	}
}

func submittedCounts(submissions [][]gintegrationsyfon.BulkStorageProbeItem) []int {
	counts := make([]int, len(submissions))
	for index, items := range submissions {
		counts[index] = len(items)
	}
	return counts
}

func submittedURLs(submissions [][]gintegrationsyfon.BulkStorageProbeItem) []string {
	urls := make([]string, 0)
	for _, items := range submissions {
		for _, item := range items {
			urls = append(urls, item.ObjectURL)
		}
	}
	return urls
}
