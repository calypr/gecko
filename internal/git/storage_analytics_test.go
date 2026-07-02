package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

type fakeStorageAnalyticsBackend struct {
	projectRecords                     []gintegrationsyfon.ProjectRecord
	bulkRecords                        map[string][]gintegrationsyfon.ProjectRecord
	buckets                            map[string]domain.StorageBucket
	bucketScopes                       map[string][]domain.StorageBucketScope
	projectScopes                      []domain.StorageBucketScope
	usageByObject                      map[string]gintegrationsyfon.FileUsage
	probeResults                       map[string]gintegrationsyfon.BulkStorageProbeResult
	listProbeResults                   map[string]gintegrationsyfon.BulkStorageProbeResult
	bucketObjects                      []gintegrationsyfon.ProjectBucketObject
	listProjectBucketObjectsErr        error
	listBucketsCalls                   int
	listBucketScopesCalls              int
	listProjectAuditRecordsCalls       int
	listProjectScopesCalls             int
	listProjectFileUsageCalls          int
	listProjectBucketObjectsCalls      int
	listProjectBucketObjectsPathPrefix string
	listProjectBucketSummaryCalls      int
	listProjectBucketSummaryMode       string
	probeCalls                         int
	probeItems                         []gintegrationsyfon.BulkStorageProbeItem
	listProbeCalls                     int
	listProbeItems                     []gintegrationsyfon.BulkStorageProbeItem
	deletedIDs                         []string
	updatedAccessMethods               map[string][]gintegrationsyfon.ProjectAccessMethod
	deletedBucketObjects               []string
}

func (fake *fakeStorageAnalyticsBackend) ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error) {
	fake.listBucketsCalls++
	out := make(map[string]domain.StorageBucket, len(fake.buckets))
	for bucket, metadata := range fake.buckets {
		out[bucket] = metadata
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error) {
	fake.listBucketScopesCalls++
	return append([]domain.StorageBucketScope(nil), fake.bucketScopes[bucket]...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return append([]gintegrationsyfon.ProjectRecord(nil), fake.projectRecords...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	fake.listProjectAuditRecordsCalls++
	return append([]gintegrationsyfon.ProjectRecord(nil), fake.projectRecords...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	fake.listProjectScopesCalls++
	if fake.projectScopes != nil {
		return append([]domain.StorageBucketScope(nil), fake.projectScopes...), nil
	}
	out := make([]domain.StorageBucketScope, 0)
	for _, scopes := range fake.bucketScopes {
		out = append(out, scopes...)
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	if fake.bulkRecords != nil {
		out := make(map[string][]gintegrationsyfon.ProjectRecord, len(fake.bulkRecords))
		for checksum, records := range fake.bulkRecords {
			out[checksum] = append([]gintegrationsyfon.ProjectRecord(nil), records...)
		}
		return out, nil
	}
	allowed := make(map[string]struct{}, len(checksums))
	for _, checksum := range checksums {
		allowed[strings.TrimSpace(checksum)] = struct{}{}
	}
	out := make(map[string][]gintegrationsyfon.ProjectRecord)
	for _, record := range fake.projectRecords {
		if _, ok := allowed[record.Checksum]; !ok {
			continue
		}
		if record.Organization != organization || record.Project != project {
			continue
		}
		out[record.Checksum] = append(out[record.Checksum], record)
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	fake.listProjectFileUsageCalls++
	out := make(map[string]gintegrationsyfon.FileUsage, len(fake.usageByObject))
	for objectID, usage := range fake.usageByObject {
		out[objectID] = usage
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, error) {
	fake.listProjectBucketObjectsCalls++
	fake.listProjectBucketObjectsPathPrefix = pathPrefix
	if fake.listProjectBucketObjectsErr != nil {
		return nil, fake.listProjectBucketObjectsErr
	}
	return append([]gintegrationsyfon.ProjectBucketObject(nil), fake.bucketObjects...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*gintegrationsyfon.ProjectBucketSummary, error) {
	fake.listProjectBucketSummaryCalls++
	fake.listProjectBucketSummaryMode = mode
	var totalBytes int64
	for _, item := range fake.bucketObjects {
		totalBytes += item.SizeBytes
	}
	return &gintegrationsyfon.ProjectBucketSummary{
		Provider:    "s3",
		Bucket:      "bucket",
		Prefix:      organization + "/" + project,
		ObjectURL:   "s3://bucket/" + organization + "/" + project,
		Exists:      len(fake.bucketObjects) > 0,
		ObjectCount: len(fake.bucketObjects),
		TotalBytes:  totalBytes,
		Mode:        mode,
	}, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	fake.probeCalls++
	fake.probeItems = append(fake.probeItems, items...)
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(items))
	for _, item := range items {
		if result, ok := fake.probeResults[item.ID]; ok {
			out = append(out, result)
			continue
		}
		exists := true
		out = append(out, gintegrationsyfon.BulkStorageProbeResult{
			ID:               item.ID,
			ObjectURL:        item.ObjectURL,
			Exists:           exists,
			Status:           "present",
			ValidationStatus: "matched",
			SizeBytes:        item.ExpectedSizeBytes,
			MetaSHA256:       item.ExpectedSHA256,
		})
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkListStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	fake.listProbeCalls++
	fake.listProbeItems = append(fake.listProbeItems, items...)
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(items))
	for _, item := range items {
		if result, ok := fake.listProbeResults[item.ID]; ok {
			out = append(out, result)
			continue
		}
		exists := true
		out = append(out, gintegrationsyfon.BulkStorageProbeResult{
			ID:               item.ID,
			ObjectURL:        item.ObjectURL,
			Exists:           exists,
			Status:           "present",
			ValidationStatus: "matched",
			SizeBytes:        item.ExpectedSizeBytes,
			SizeMatch:        ptrBool(true),
			NameMatch:        ptrBool(true),
		})
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	fake.deletedIDs = append(fake.deletedIDs, objectIDs...)
	return nil
}

func (fake *fakeStorageAnalyticsBackend) DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error) {
	fake.deletedBucketObjects = append(fake.deletedBucketObjects, objectURLs...)
	results := make([]gintegrationsyfon.ProjectBucketDeleteResult, 0, len(objectURLs))
	for _, objectURL := range objectURLs {
		results = append(results, gintegrationsyfon.ProjectBucketDeleteResult{
			ObjectURL: objectURL,
			Status:    "deleted",
		})
	}
	return results, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error {
	if fake.updatedAccessMethods == nil {
		fake.updatedAccessMethods = map[string][]gintegrationsyfon.ProjectAccessMethod{}
	}
	for objectID, methods := range updates {
		fake.updatedAccessMethods[objectID] = append([]gintegrationsyfon.ProjectAccessMethod(nil), methods...)
	}
	return nil
}

func TestBuildGitRepoInventoryAndStorageAnalytics(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/c.txt":        lfsPointer("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 300),
		"data/e.txt":        lfsPointer("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 50),
		"data/nested/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"plain.txt":         "not lfs\n",
	})
	inventory, err := BuildGitRepoInventory(refName, "data", repo, hash)
	if err != nil {
		t.Fatalf("build repo inventory: %v", err)
	}
	if len(inventory) != 4 {
		t.Fatalf("expected 4 lfs files under data, got %+v", inventory)
	}
	if inventory[0].RepoPath != "data/a.txt" || inventory[3].RepoPath != "data/nested/b.txt" {
		t.Fatalf("unexpected inventory ordering: %+v", inventory)
	}

	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	older := now.Add(-48 * time.Hour)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: inventory[0].Checksum, Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-d", Checksum: inventory[1].Checksum, Organization: "org", Project: "proj", Size: 300, UpdatedAt: &older},
			{ObjectID: "obj-b-old", Checksum: inventory[3].Checksum, Organization: "org", Project: "proj", Size: 200, UpdatedAt: &older, AccessURLs: []string{"s3://bucket/b-old"}},
			{ObjectID: "obj-b-new", Checksum: inventory[3].Checksum, Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/b-new"}},
			{ObjectID: "obj-orphan", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj", Size: 500, UpdatedAt: &older, AccessURLs: []string{"s3://bucket/orphan"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-a":      {ObjectID: "obj-a", DownloadCount: 5, LastDownloadTime: ptrTime(now)},
			"obj-b-old":  {ObjectID: "obj-b-old", DownloadCount: 0},
			"obj-b-new":  {ObjectID: "obj-b-new", DownloadCount: 10, LastDownloadTime: ptrTime(now)},
			"obj-orphan": {ObjectID: "obj-orphan", DownloadCount: 0},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{},
	}
	service := NewStorageAnalyticsService(backend)

	summary, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build storage summary: %v", err)
	}
	if summary.FileCount != 4 || summary.RecordCount != 4 || summary.DirectChildCount != 4 || summary.DuplicatePathCount != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.TotalBytes != 650 || summary.DownloadCount != 15 {
		t.Fatalf("unexpected summary bytes/downloads: %+v", summary)
	}

	children, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 10, "bytes", "desc")
	if err != nil {
		t.Fatalf("build storage children: %v", err)
	}
	if len(children.Items) != 4 {
		t.Fatalf("expected 4 child rows, got %+v", children.Items)
	}
	if children.Items[0].Path != "data/c.txt" || children.Items[1].Path != "data/nested" {
		t.Fatalf("unexpected child ordering: %+v", children.Items)
	}

	diff, err := service.BuildProjectDiffAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build project diff audit: %v", err)
	}
	if diff.Summary.TotalFindings != 3 {
		t.Fatalf("expected 3 diff findings, got %+v", diff)
	}
	assertHasDiffFinding(t, diff.Findings, "duplicate_syfon_paths", "data/nested/b.txt")
	missingFinding := assertHasDiffFinding(t, diff.Findings, "repo_missing_in_syfon", "data/e.txt")
	if missingFinding.Checksum != "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" {
		t.Fatalf("expected checksum on repo_missing_in_syfon, got %+v", missingFinding)
	}
	if missingFinding.Evidence == nil || missingFinding.Evidence.Checksum != missingFinding.Checksum || missingFinding.Evidence.BucketEvaluation != "not_checked" {
		t.Fatalf("expected evidence on repo_missing_in_syfon, got %+v", missingFinding)
	}
	orphanFinding := assertHasDiffFinding(t, diff.Findings, "syfon_missing_in_repo", "s3://bucket/orphan")
	if len(orphanFinding.SourcePaths) != 1 || orphanFinding.SourcePaths[0] != "s3://bucket/orphan" {
		t.Fatalf("expected orphan source path, got %+v", orphanFinding)
	}
	if orphanFinding.Checksum != "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" {
		t.Fatalf("expected checksum on syfon_missing_in_repo, got %+v", orphanFinding)
	}
	if orphanFinding.Evidence == nil || len(orphanFinding.Evidence.AccessURLs) != 1 || orphanFinding.Evidence.AccessURLs[0] != "s3://bucket/orphan" {
		t.Fatalf("expected orphan access URL evidence, got %+v", orphanFinding)
	}

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	if backend.probeCalls != 1 {
		t.Fatalf("expected one bulk storage probe call, got %d", backend.probeCalls)
	}
	if cleanup.Summary.TotalFindings != 3 {
		t.Fatalf("expected 3 cleanup findings, got %+v", cleanup)
	}
	assertHasCleanupFinding(t, cleanup.Findings, "stale_duplicate_record", "data/nested/b.txt")
	assertHasCleanupFinding(t, cleanup.Findings, "broken_access_url_error", "data/c.txt")
	assertHasCleanupFinding(t, cleanup.Findings, "repo_orphan_stale_record", "s3://bucket/orphan")

	applyResult, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true, true, true, false, false, true)
	if err != nil {
		t.Fatalf("apply cleanup dry run: %v", err)
	}
	if !applyResult.DryRun {
		t.Fatalf("expected dry run apply result, got %+v", applyResult)
	}
	if len(applyResult.DeletedRecordIDs) != 2 || !contains(applyResult.DeletedRecordIDs, "obj-b-old") || !contains(applyResult.DeletedRecordIDs, "obj-orphan") {
		t.Fatalf("unexpected dry run delete ids: %+v", applyResult)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("dry run should not delete objects, got %+v", backend.deletedIDs)
	}
}

func TestApplyStorageCleanupRepairsBrokenBucketMappings(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:      "obj-a",
				Checksum:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization:  "org",
				Project:       "proj",
				Size:          100,
				UpdatedAt:     &now,
				AccessURLs:    []string{"s3://legacy/a", "s3://bucket/a"},
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{URL: "s3://legacy/a"}, {URL: "s3://bucket/a"}},
			},
			{
				ObjectID:      "obj-b",
				Checksum:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Organization:  "org",
				Project:       "proj",
				Size:          200,
				UpdatedAt:     &now,
				AccessURLs:    []string{"s3://legacy/b"},
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{URL: "s3://legacy/b"}},
			},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://legacy/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:        storageProbeRequestKey("s3://legacy/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL: "s3://legacy/a",
				Status:    "error",
				ErrorKind: "credential_missing",
			},
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			storageProbeRequestKey("s3://legacy/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:        storageProbeRequestKey("s3://legacy/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL: "s3://legacy/b",
				Status:    "error",
				ErrorKind: "credential_missing",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	applyResult, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", refName, "data", []string{"data/a.txt", "data/b.txt"}, mirrorPath, repo, hash, true, false, false, false, true, false)
	if err != nil {
		t.Fatalf("apply cleanup broken bucket mapping repair: %v", err)
	}
	if len(applyResult.UpdatedRecordIDs) != 1 || applyResult.UpdatedRecordIDs[0] != "obj-a" {
		t.Fatalf("expected obj-a access methods to be updated, got %+v", applyResult.UpdatedRecordIDs)
	}
	if len(applyResult.DeletedRecordIDs) != 1 || applyResult.DeletedRecordIDs[0] != "obj-b" {
		t.Fatalf("expected obj-b to be deleted, got %+v", applyResult.DeletedRecordIDs)
	}
	updatedMethods := backend.updatedAccessMethods["obj-a"]
	if len(updatedMethods) != 1 || updatedMethods[0].URL != "s3://bucket/a" {
		t.Fatalf("expected obj-a to retain only good access method, got %+v", updatedMethods)
	}
	if len(backend.deletedIDs) != 1 || backend.deletedIDs[0] != "obj-b" {
		t.Fatalf("expected obj-b to be deleted, got %+v", backend.deletedIDs)
	}
}

func TestApplyStorageCleanupDeletesBucketOnlyObjects(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:      "obj-a",
				Checksum:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization:  "org",
				Project:       "proj",
				Size:          100,
				UpdatedAt:     &now,
				AccessURLs:    []string{"s3://bucket/a"},
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{URL: "s3://bucket/a"}},
			},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults:  map[string]gintegrationsyfon.BulkStorageProbeResult{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", SizeBytes: 100},
			{ObjectURL: "s3://bucket/orphan", Bucket: "bucket", Key: "orphan", SizeBytes: 25},
		},
	}
	service := NewStorageAnalyticsService(backend)

	applyResult, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", refName, "data", []string{"s3://bucket/orphan"}, mirrorPath, repo, hash, true, false, false, true, false, false)
	if err != nil {
		t.Fatalf("apply cleanup bucket only delete: %v", err)
	}
	if len(applyResult.DeletedBucketObjectURLs) != 1 || applyResult.DeletedBucketObjectURLs[0] != "s3://bucket/orphan" {
		t.Fatalf("expected orphan bucket object to be deleted, got %+v", applyResult.DeletedBucketObjectURLs)
	}
	if len(backend.deletedBucketObjects) != 1 || backend.deletedBucketObjects[0] != "s3://bucket/orphan" {
		t.Fatalf("expected backend bucket delete call, got %+v", backend.deletedBucketObjects)
	}
}

func TestBuildStorageCleanupAuditReportsStorageProbeFailures(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/b"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
			storageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:                   storageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:            "s3://bucket/b",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"size_mismatch"},
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	assertHasCleanupFinding(t, cleanup.Findings, "storage_object_missing", "data/a.txt")
	assertHasCleanupFinding(t, cleanup.Findings, "storage_validation_mismatch", "data/b.txt")
	var mismatchFinding GitStorageCleanupFinding
	for _, finding := range cleanup.Findings {
		if finding.Kind == "storage_validation_mismatch" && finding.NormalizedPath == "data/b.txt" {
			mismatchFinding = finding
			break
		}
	}
	if len(mismatchFinding.Records) != 1 || len(mismatchFinding.Records[0].AccessProbes) != 1 {
		t.Fatalf("expected probe details on mismatch finding, got %+v", mismatchFinding)
	}
	if mismatchFinding.Records[0].AccessProbes[0].ValidationStatus != "mismatched" {
		t.Fatalf("expected mismatched validation status, got %+v", mismatchFinding.Records[0].AccessProbes[0])
	}
	if mismatchFinding.Checksum != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("expected checksum on mismatch finding, got %+v", mismatchFinding)
	}
	if mismatchFinding.Evidence == nil ||
		len(mismatchFinding.Evidence.AccessURLs) != 1 ||
		mismatchFinding.Evidence.AccessURLs[0] != "s3://bucket/b" ||
		len(mismatchFinding.Evidence.ProbeStatuses) != 1 ||
		mismatchFinding.Evidence.ProbeStatuses[0] != "present" ||
		mismatchFinding.Evidence.BucketEvaluation != "probed" {
		t.Fatalf("expected storage evidence on mismatch finding, got %+v", mismatchFinding)
	}
}

func TestBuildStorageCleanupAuditFlagsRecordWhenAnyAccessProbeIsDead(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	missingKey := storageProbeRequestKey("s3://bucket/legacy-a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	presentKey := storageProbeRequestKey("s3://bucket/current-a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bucket/legacy-a", "s3://bucket/current-a"},
			},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			missingKey: {
				ID:               missingKey,
				ObjectURL:        "s3://bucket/legacy-a",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
			presentKey: {
				ID:               presentKey,
				ObjectURL:        "s3://bucket/current-a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	finding := assertHasCleanupFinding(t, cleanup.Findings, "storage_object_missing", "data/a.txt")
	if finding.Evidence == nil || !contains(finding.Evidence.AccessURLs, "s3://bucket/legacy-a") {
		t.Fatalf("expected dead raw access URL evidence, got %+v", finding)
	}
}

func TestBuildStorageCleanupAuditReportsBrokenBucketMappingSeparately(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	probeKey := storageProbeRequestKey("s3://bforepc-prod/path/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bforepc-prod/path/a.txt"},
			},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			probeKey: {
				ID:               probeKey,
				ObjectURL:        "s3://bforepc-prod/path/a.txt",
				Status:           "error",
				Exists:           false,
				ErrorKind:        "credential_missing",
				Error:            `no stored bucket credential found for bucket "bforepc-prod"`,
				ValidationStatus: "unverifiable",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	assertHasCleanupFinding(t, cleanup.Findings, "broken_bucket_mapping", "data/a.txt")
	for _, finding := range cleanup.Findings {
		if finding.Kind == "broken_bucket_mapping" && finding.NormalizedPath == "data/a.txt" {
			if len(finding.Records) != 1 || finding.Records[0].Error != "no Syfon bucket mapping is configured for this access URL" {
				t.Fatalf("expected broken bucket mapping detail, got %+v", finding)
			}
			if finding.Evidence == nil ||
				len(finding.Evidence.AccessURLs) != 1 ||
				finding.Evidence.AccessURLs[0] != "s3://bforepc-prod/path/a.txt" ||
				len(finding.Evidence.ErrorKinds) != 1 ||
				finding.Evidence.ErrorKinds[0] != "credential_missing" ||
				finding.Evidence.BucketEvaluation != "probed" {
				t.Fatalf("expected broken bucket mapping evidence, got %+v", finding)
			}
			return
		}
	}
	t.Fatalf("missing broken bucket mapping detail in %+v", cleanup.Findings)
}

func TestBuildStorageCleanupAuditStartsFromBucketInventory(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-orphan", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj", Size: 50, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/orphan"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
			{ObjectURL: "s3://bucket/orphan", Bucket: "bucket", Key: "orphan", Path: "orphan", SizeBytes: 50},
			{ObjectURL: "s3://bucket/loose", Bucket: "bucket", Key: "loose", Path: "loose", SizeBytes: 25},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Bucket:           "bucket",
				Key:              "a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			storageProbeRequestKey("s3://bucket/orphan", 50, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"): {
				ID:               storageProbeRequestKey("s3://bucket/orphan", 50, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
				ObjectURL:        "s3://bucket/orphan",
				Bucket:           "bucket",
				Key:              "orphan",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	assertHasCleanupFinding(t, cleanup.Findings, "repo_orphan_live_object", "s3://bucket/orphan")
	assertHasCleanupFinding(t, cleanup.Findings, "bucket_only_object", "s3://bucket/loose")
}

func TestBuildStorageChainAuditUsesBucketFirstFindingKinds(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/missing.txt":  lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"data/git-only.txt": lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 300),
		"data/bad-map.txt":  lfsPointer("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 400),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-missing", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/missing"}},
			{ObjectID: "obj-no-git", Checksum: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Organization: "org", Project: "proj", Size: 150, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/no-git"}},
			{ObjectID: "obj-bad-map", Checksum: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Organization: "org", Project: "proj", Size: 400, UpdatedAt: &now, AccessURLs: []string{"s3://legacy-bucket/bad-map"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
			{ObjectURL: "s3://bucket/no-git", Bucket: "bucket", Key: "no-git", Path: "no-git", SizeBytes: 150},
			{ObjectURL: "s3://bucket/loose", Bucket: "bucket", Key: "loose", Path: "loose", SizeBytes: 25},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Bucket:           "bucket",
				Key:              "a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			storageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               storageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:        "s3://bucket/missing",
				Bucket:           "bucket",
				Key:              "missing",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
			storageProbeRequestKey("s3://bucket/no-git", 150, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"): {
				ID:               storageProbeRequestKey("s3://bucket/no-git", 150, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
				ObjectURL:        "s3://bucket/no-git",
				Bucket:           "bucket",
				Key:              "no-git",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			storageProbeRequestKey("s3://legacy-bucket/bad-map", 400, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"): {
				ID:               storageProbeRequestKey("s3://legacy-bucket/bad-map", 400, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
				ObjectURL:        "s3://legacy-bucket/bad-map",
				Status:           "error",
				Exists:           false,
				ErrorKind:        "credential_missing",
				Error:            `no stored bucket credential found for bucket "legacy-bucket"`,
				ValidationStatus: "unverifiable",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	findings := loadAllChainFindings(t, service, "org", "proj", chain)
	assertHasChainFinding(t, findings, "bucket_only_object", "s3://bucket/loose")
	assertHasChainFinding(t, findings, "bucket_syfon_no_git", "s3://bucket/no-git")
	assertHasChainFinding(t, findings, "syfon_git_no_bucket", "data/missing.txt")
	assertHasChainFinding(t, findings, "syfon_broken_bucket_mapping", "data/bad-map.txt")
	assertHasChainFinding(t, findings, "git_only_no_syfon", "data/git-only.txt")
	if chain.Summary.BucketObjectCount != 3 || chain.Summary.SyfonRecordCount != 4 || chain.Summary.GitTrackedFileCount != 4 {
		t.Fatalf("unexpected chain summary totals: %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditUsesProjectAuditSourcesAndTargetsProbes(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/missing.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/present"}},
			{ObjectID: "obj-missing", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/missing"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present", Bucket: "bucket", Key: "present", Path: "present", SizeBytes: 100, MetaSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               storageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:        "s3://bucket/missing",
				Bucket:           "bucket",
				Key:              "missing",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	findings := loadAllChainFindings(t, service, "org", "proj", chain)
	assertHasChainFinding(t, findings, "syfon_git_no_bucket", "data/missing.txt")
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected one project audit record call, got %d", backend.listProjectAuditRecordsCalls)
	}
	if backend.listProjectScopesCalls != 1 {
		t.Fatalf("expected one project scope call, got %d", backend.listProjectScopesCalls)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected no project file usage calls, got %d", backend.listProjectFileUsageCalls)
	}
	if backend.listBucketsCalls != 0 || backend.listBucketScopesCalls != 0 {
		t.Fatalf("expected no bucket-wide scope discovery, got listBuckets=%d listBucketScopes=%d", backend.listBucketsCalls, backend.listBucketScopesCalls)
	}
	if backend.probeCalls != 1 || len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != "s3://bucket/missing" {
		t.Fatalf("expected one targeted probe for missing object, got calls=%d items=%+v", backend.probeCalls, backend.probeItems)
	}
}

func TestBuildStorageChainAuditInventoryOnlySkipsTargetedProbes(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/present"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present", Bucket: "bucket", Key: "present", Path: "present", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		refName,
		"data",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{ProbeMode: StorageChainProbeModeInventoryOnly},
	)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if backend.probeCalls != 0 || len(backend.probeItems) != 0 {
		t.Fatalf("expected inventory-only audit to skip targeted probes, got calls=%d items=%+v", backend.probeCalls, backend.probeItems)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected inventory-derived complete chain, got summary %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditValidateModeUsesListValidation(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt":  lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/mismatch.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Name: "present.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/present.txt"}},
			{ObjectID: "obj-mismatch", Name: "expected.txt", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, AccessURLs: []string{"s3://bucket/mismatch.txt"}},
			{ObjectID: "obj-syfon-only", Name: "syfon-only.txt", Checksum: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Organization: "org", Project: "proj", Size: 300, AccessURLs: []string{"s3://bucket/syfon-only.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present.txt", Bucket: "bucket", Key: "present.txt", Path: "present.txt", SizeBytes: 100},
		},
		listProbeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageListValidationRequestKey("s3://bucket/mismatch.txt", 200, "expected.txt"): {
				ID:                   storageListValidationRequestKey("s3://bucket/mismatch.txt", 200, "expected.txt"),
				ObjectURL:            "s3://bucket/mismatch.txt",
				Provider:             "s3",
				Bucket:               "bucket",
				Key:                  "mismatch.txt",
				Path:                 "mismatch.txt",
				Exists:               true,
				Status:               "present",
				ValidationStatus:     "mismatched",
				SizeBytes:            int64Ptr(200),
				SizeMatch:            ptrBool(true),
				NameMatch:            ptrBool(false),
				ValidationMismatches: []string{"name"},
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		refName,
		"data",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err != nil {
		t.Fatalf("build validate chain audit: %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 0 {
		t.Fatalf("expected validate mode to skip recursive bucket inventory, got %d calls", backend.listProjectBucketObjectsCalls)
	}
	if backend.listProjectBucketSummaryCalls != 0 {
		t.Fatalf("expected validate mode to skip bucket summary preflight, got %d calls", backend.listProjectBucketSummaryCalls)
	}
	if backend.probeCalls != 0 {
		t.Fatalf("expected validate mode to skip HEAD probes, got %d calls", backend.probeCalls)
	}
	if backend.listProbeCalls != 1 || len(backend.listProbeItems) != 3 {
		t.Fatalf("expected three bulk LIST validation items, got calls=%d items=%+v", backend.listProbeCalls, backend.listProbeItems)
	}
	if backend.listProbeItems[0].ExpectedName == "" && backend.listProbeItems[1].ExpectedName == "" {
		t.Fatalf("expected LIST validation items to include expected Syfon names, got %+v", backend.listProbeItems)
	}
	if chain.Summary.BucketPathExists != nil || chain.Summary.BucketSummaryMode != "" {
		t.Fatalf("expected validate mode not to claim bucket prefix summary, got %+v", chain.Summary)
	}
	if chain.Summary.BucketObjectCount != 3 || chain.Summary.CountsByKind["bucket_only_object"] != 0 {
		t.Fatalf("expected validate mode to count validated objects without bucket-only inventory, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["bucket_syfon_git_complete"] != 1 {
		t.Fatalf("expected validate mode to count clean Syfon/Git/bucket join, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["bucket_syfon_no_git"] != 1 {
		t.Fatalf("expected validate mode to report Syfon-backed no-Git record, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["git_syfon_metadata_mismatch"]; got != 1 {
		t.Fatalf("expected one LIST-derived metadata mismatch, got summary %+v", chain.Summary)
	}
	assertNoChainFinding(t, chain.Findings, "bucket_only_object")
	assertHasChainFinding(t, chain.Findings, "git_syfon_metadata_mismatch", "data/mismatch.txt")
	assertHasChainFinding(t, chain.Findings, "bucket_syfon_no_git", "s3://bucket/syfon-only.txt")
}

func TestExpectedStorageObjectNameForListValidationSkipsContentAddressedKeys(t *testing.T) {
	if got := expectedStorageObjectNameForListValidation("s3://cbds/0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/ec4b068cb42b52449dd44052c3bfb2a459b00336a9cd42cd29c22ca1d1b26cb0", "CONFIG/cbds-BForePC.json"); got != "" {
		t.Fatalf("expected content-addressed storage key to skip name validation, got %q", got)
	}
	if got := expectedStorageObjectNameForListValidation("s3://bucket/path/cbds-BForePC.json", "CONFIG/cbds-BForePC.json"); got != "cbds-BForePC.json" {
		t.Fatalf("expected filename-backed storage key to validate basename, got %q", got)
	}
}

func TestBuildStorageChainAuditCachesProjectChainInputs(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/present"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present", Bucket: "bucket", Key: "present", Path: "present", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)
	options := StorageChainAuditOptions{ProbeMode: StorageChainProbeModeInventoryOnly}

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build second chain audit: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected cached project records, got %d calls", backend.listProjectAuditRecordsCalls)
	}
	if backend.listProjectScopesCalls != 1 {
		t.Fatalf("expected cached project scopes, got %d calls", backend.listProjectScopesCalls)
	}
	if backend.listProjectBucketObjectsCalls != 1 {
		t.Fatalf("expected cached project bucket inventory, got %d calls", backend.listProjectBucketObjectsCalls)
	}
}

func TestBuildStorageChainAuditForwardsBucketPathPrefixForExplicitInventory(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"README.md": "fixture",
	})
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/root/CONFIG/a.json", Bucket: "bucket", Key: "root/CONFIG/a.json", Path: "CONFIG/a.json", SizeBytes: 10},
			{ObjectURL: "s3://bucket/root/CONFIG/nested/b.json", Bucket: "bucket", Key: "root/CONFIG/nested/b.json", Path: "CONFIG/nested/b.json", SizeBytes: 15},
		},
	}
	service := NewStorageAnalyticsService(backend)
	response, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeItems,
		BucketPathPrefix:    "/CONFIG/",
		ProbeMode:           StorageChainProbeModeInventoryOnly,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 1 {
		t.Fatalf("expected one recursive bucket inventory call, got %d", backend.listProjectBucketObjectsCalls)
	}
	if backend.listProjectBucketObjectsPathPrefix != "CONFIG" {
		t.Fatalf("expected bucket path prefix to be forwarded, got %q", backend.listProjectBucketObjectsPathPrefix)
	}
	if response.BucketPathPrefix != "CONFIG" {
		t.Fatalf("expected response bucket path prefix, got %q", response.BucketPathPrefix)
	}
	if response.Summary.BucketObjectCount != 2 {
		t.Fatalf("expected recursive LIST rows to drive bucket count, got %+v", response.Summary)
	}
}

func TestBuildStorageChainAuditSurfacesMetadataMismatchAndEvidence(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 999},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:                   storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:            "s3://bucket/a",
				Bucket:               "bucket",
				Key:                  "a",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"size_mismatch"},
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	findings := loadAllChainFindings(t, service, "org", "proj", chain)
	finding := assertHasChainFinding(t, findings, "git_syfon_metadata_mismatch", "data/a.txt")
	if finding.Evidence == nil ||
		len(finding.Evidence.BucketObjectURLs) != 1 ||
		finding.Evidence.BucketObjectURLs[0] != "s3://bucket/a" ||
		len(finding.Evidence.ValidationStates) != 1 ||
		finding.Evidence.ValidationStates[0] != "mismatched" {
		t.Fatalf("expected metadata mismatch evidence, got %+v", finding)
	}
}

func TestBuildStorageChainAuditUsesScopedProjectRecordsForGitJoin(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
		},
		bulkRecords:   map[string][]gintegrationsyfon.ProjectRecord{},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Bucket:           "bucket",
				Key:              "a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	for _, finding := range loadAllChainFindings(t, service, "org", "proj", chain) {
		if finding.Kind == "git_only_no_syfon" && finding.NormalizedPath == "data/a.txt" {
			t.Fatalf("did not expect scoped Syfon record to be reclassified as git-only when bulk checksum lookup is empty: %+v", finding)
		}
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected one fully connected bucket->syfon->git chain, got summary %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditCanonicalizesScopedLegacyAccessURLs(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/slide.ome.tiff": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bforepc-prod/OHSU/slide.ome.tiff"},
			},
		},
		buckets: map[string]domain.StorageBucket{
			"bforepc": {Bucket: "bforepc", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"bforepc": {{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", Bucket: "bforepc", Key: "bforepc-prod/OHSU/slide.ome.tiff", Path: "slide.ome.tiff", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bforepc-prod/OHSU/slide.ome.tiff",
				Status:           "not_found",
				Exists:           false,
				ErrorKind:        "credential_missing",
				Error:            `no stored bucket credential found for bucket "bforepc-prod"`,
				ValidationStatus: "unverifiable",
			},
			storageProbeRequestKey("s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff",
				Bucket:           "bforepc",
				Key:              "bforepc-prod/OHSU/slide.ome.tiff",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected mapped bucket match to suppress stale raw URL misclassification, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected mapped bucket match to preserve clean-chain count, got %+v", chain.Summary)
	}
	findings := loadAllChainFindings(t, service, "HTAN_INT", "BForePC", chain)
	for _, finding := range findings {
		if finding.NormalizedPath == "data/slide.ome.tiff" {
			t.Fatalf("did not expect stale raw URL to produce a chain finding once mapped object exists: %+v", finding)
		}
	}
	if len(backend.probeItems) != 2 {
		t.Fatalf("expected raw and canonical probes, got %+v", backend.probeItems)
	}
	if !containsProbeTarget(backend.probeItems, "s3://bforepc-prod/OHSU/slide.ome.tiff") || !containsProbeTarget(backend.probeItems, "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff") {
		t.Fatalf("expected probe targets to include raw and canonical URLs, got %+v", backend.probeItems)
	}
}

func TestBuildStorageChainAuditFlagsExactPathMismatchWhenHashExistsElsewhereInBucket(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"CONFIG/cbds-BForePC.json": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 23739),
	})
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	objectHash := "ec4b068cb42b52449dd44052c3bfb2a459b00336a9cd42cd29c22ca1d1b26cb0"
	rawURL := "s3://cbds/0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/" + objectHash
	canonicalURL := "s3://bforepc/bforepc-prod/0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/" + objectHash
	relocatedURL := "s3://bforepc/bforepc-prod/2532ab27-4961-57da-8bef-e9774093bf56/" + objectHash
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Name:         "CONFIG/cbds-BForePC.json",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         23739,
				UpdatedAt:    &now,
				AccessURLs:   []string{rawURL},
			},
		},
		projectScopes: []domain.StorageBucketScope{
			{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL: relocatedURL,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/2532ab27-4961-57da-8bef-e9774093bf56/" + objectHash,
				Path:      objectHash,
				SizeBytes: 23739,
			},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey(rawURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey(rawURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        rawURL,
				Bucket:           "cbds",
				Key:              "0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/" + objectHash,
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			storageProbeRequestKey(canonicalURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey(canonicalURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        canonicalURL,
				Bucket:           "bforepc",
				Key:              "bforepc-prod/0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/" + objectHash,
				Status:           "not_found",
				Exists:           false,
				ErrorKind:        "object_not_found",
				ValidationStatus: "unverifiable",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "CONFIG", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 1 {
		t.Fatalf("expected exact mapped-key miss to surface as broken bucket mapping, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_only_object"]; got != 0 {
		t.Fatalf("expected relocated hash not to be double-counted as bucket-only, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 0 {
		t.Fatalf("expected exact mapped-key miss to block clean-chain count, got %+v", chain.Summary)
	}
	findings := loadAllChainFindings(t, service, "HTAN_INT", "BForePC", chain)
	assertHasChainFinding(t, findings, "syfon_broken_bucket_mapping", "CONFIG/cbds-BForePC.json")
}

func TestBuildStorageChainAuditNormalizesChecksumJoinKeys(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bucket/a.txt"},
			},
		},
		bulkRecords: map[string][]gintegrationsyfon.ProjectRecord{
			"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA": {{
				ObjectID:     "obj-a",
				Checksum:     "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bucket/a.txt"},
			}},
		},
		buckets: map[string]domain.StorageBucket{
			"bucket": {Bucket: "bucket", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"bucket": {{
				Bucket:       "bucket",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket",
			}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a.txt", Bucket: "bucket", Key: "a.txt", Path: "a.txt", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a.txt",
				Bucket:           "bucket",
				Key:              "a.txt",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["git_only_no_syfon"]; got != 0 {
		t.Fatalf("expected normalized checksum join to avoid git-only false positive, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected one normalized complete chain, got %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditFailsWhenProjectBucketListDenied(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bucket/prefix/a.txt"},
			},
		},
		buckets: map[string]domain.StorageBucket{
			"bucket": {Bucket: "bucket", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"bucket": {{
				Bucket:       "bucket",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket/prefix",
			}},
		},
		usageByObject:               map[string]gintegrationsyfon.FileUsage{},
		listProjectBucketObjectsErr: fmt.Errorf("list syfon project bucket objects: syfon POST /data/inspect/project-bucket failed with status 409: provider rejected bucket inventory request for s3://bucket/prefix; mapped bucket target may be missing or inaccessible"),
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/prefix/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/prefix/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/prefix/a.txt",
				Bucket:           "bucket",
				Key:              "prefix/a.txt",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	_, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err == nil {
		t.Fatal("expected bucket inventory failure to hard-error chain audit")
	}
	if !strings.Contains(err.Error(), "mapped bucket target may be missing or inaccessible") {
		t.Fatalf("expected bucket inventory error detail, got %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 1 {
		t.Fatalf("expected one bucket inventory attempt, got %d", backend.listProjectBucketObjectsCalls)
	}
	if backend.probeCalls != 0 {
		t.Fatalf("expected no probe fallback after bucket inventory failure, got %d", backend.probeCalls)
	}
}

func TestBuildStorageChainAuditReturnsFullFindings(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/git-only.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 50),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
		},
		buckets: map[string]domain.StorageBucket{
			"bucket": {Bucket: "bucket", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"bucket": {{
				Bucket:       "bucket",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket",
			}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               storageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Bucket:           "bucket",
				Key:              "a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if len(chain.Findings) == 0 {
		t.Fatalf("expected chain audit findings in response, got %+v", chain)
	}
	if len(chain.Groups) == 0 {
		t.Fatalf("expected grouped summary rows, got %+v", chain)
	}
	if finding := assertHasChainFinding(t, chain.Findings, "git_only_no_syfon", "data/git-only.txt"); finding.NormalizedPath != "data/git-only.txt" {
		t.Fatalf("unexpected chain finding: %+v", finding)
	}
}

func TestBuildStorageCleanupAuditTreatsMissingProbeEvidenceAsProbeError(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://legacy-bucket/a"},
			},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/other", Bucket: "bucket", Key: "other", Path: "other", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://legacy-bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, true)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	assertHasCleanupFinding(t, cleanup.Findings, "storage_probe_error", "data/a.txt")
	for _, finding := range cleanup.Findings {
		if finding.NormalizedPath == "data/a.txt" && finding.Kind == "storage_object_missing" {
			t.Fatalf("did not expect missing-object classification without a real not_found probe, got %+v", finding)
		}
	}
}

func TestBuildStorageCleanupAuditSkipsBucketStagesWhenCheckStorageDisabled(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
		},
		buckets: map[string]domain.StorageBucket{
			"bucket": {Bucket: "bucket", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"bucket": {{
				Bucket:       "bucket",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket",
			}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)

	cleanup, _, err := service.BuildStorageCleanupAudit(context.Background(), "Bearer token", "org", "proj", refName, "data", nil, mirrorPath, repo, hash, false)
	if err != nil {
		t.Fatalf("build cleanup audit: %v", err)
	}
	if len(cleanup.Findings) != 0 {
		t.Fatalf("expected no cleanup findings without storage checks, got %+v", cleanup.Findings)
	}
	if backend.listProjectBucketObjectsCalls != 0 {
		t.Fatalf("expected bucket inventory to be skipped when check_storage=false, got %d calls", backend.listProjectBucketObjectsCalls)
	}
	if backend.probeCalls != 0 {
		t.Fatalf("expected storage probes to be skipped when check_storage=false, got %d calls", backend.probeCalls)
	}
}

func TestPersistRepoAnalyticsIndexAndLoadExistingDirectoryWithoutLFSFiles(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"plain/notes.txt":   "plain text only\n",
		"plain/nested/x.md": "still plain\n",
	})
	if err := PersistRepoAnalyticsIndex(context.Background(), mirrorPath, repo, refName, hash); err != nil {
		t.Fatalf("persist repo analytics index: %v", err)
	}
	sidecar, err := readRepoAnalyticsIndexSidecar(mirrorPath)
	if err != nil {
		t.Fatalf("read repo analytics sidecar: %v", err)
	}
	if sidecar.CommitHash != hash.String() {
		t.Fatalf("unexpected sidecar hash: %+v", sidecar)
	}
	index, err := loadOrBuildRepoAnalyticsIndex(context.Background(), mirrorPath, refName, repo, hash)
	if err != nil {
		t.Fatalf("load repo analytics index: %v", err)
	}
	directory, err := repoDirectoryAggregate(index, "plain")
	if err != nil {
		t.Fatalf("lookup plain directory aggregate: %v", err)
	}
	if directory.FileCount != 0 || directory.DirectChildCount != 0 {
		t.Fatalf("expected zero-lfs directory aggregate, got %+v", directory)
	}
	filtered, err := filterRepoInventoryFiles(index, "plain")
	if err != nil {
		t.Fatalf("filter plain directory inventory: %v", err)
	}
	if len(filtered) != 0 {
		t.Fatalf("expected no lfs files under plain directory, got %+v", filtered)
	}
}

func buildAnalyticsMirror(t *testing.T, files map[string]string) (*gogit.Repository, string, string, plumbing.Hash) {
	t.Helper()
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source")
	repo, err := gogit.PlainInit(sourcePath, false)
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("load worktree: %v", err)
	}
	for filePath, content := range files {
		fullPath := filepath.Join(sourcePath, filepath.FromSlash(filePath))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filePath, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", filePath, err)
		}
		if _, err := worktree.Add(filePath); err != nil {
			t.Fatalf("add %s: %v", filePath, err)
		}
	}
	if _, err := worktree.Commit("seed analytics repo", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit analytics repo: %v", err)
	}
	mirrorPath := filepath.Join(tempDir, "mirror.git")
	if err := SyncRepositoryMirror(context.Background(), sourcePath, mirrorPath, nil); err != nil {
		t.Fatalf("sync mirror: %v", err)
	}
	mirrorRepo, err := OpenRepository(mirrorPath)
	if err != nil {
		t.Fatalf("open mirror: %v", err)
	}
	refName, hash, err := ResolveGitReference(mirrorRepo, "", "")
	if err != nil {
		t.Fatalf("resolve ref: %v", err)
	}
	return mirrorRepo, mirrorPath, refName, hash
}

func lfsPointer(checksum string, size int64) string {
	return strings.Join([]string{
		"version https://git-lfs.github.com/spec/v1",
		"oid sha256:" + checksum,
		"size " + strconv.FormatInt(size, 10),
		"",
	}, "\n")
}

func ptrTime(value time.Time) *time.Time {
	copyValue := value
	return &copyValue
}

func ptrBool(value bool) *bool {
	copyValue := value
	return &copyValue
}

func int64Ptr(value int64) *int64 {
	copyValue := value
	return &copyValue
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsProbeTarget(values []gintegrationsyfon.BulkStorageProbeItem, target string) bool {
	for _, value := range values {
		if value.ObjectURL == target {
			return true
		}
	}
	return false
}

func assertHasDiffFinding(t *testing.T, findings []GitProjectDiffFinding, kind string, path string) GitProjectDiffFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return finding
		}
	}
	t.Fatalf("missing diff finding kind=%s path=%s in %+v", kind, path, findings)
	return GitProjectDiffFinding{}
}

func assertHasCleanupFinding(t *testing.T, findings []GitStorageCleanupFinding, kind string, path string) GitStorageCleanupFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return finding
		}
	}
	t.Fatalf("missing cleanup finding kind=%s path=%s in %+v", kind, path, findings)
	return GitStorageCleanupFinding{}
}

func loadAllChainFindings(t *testing.T, service *StorageAnalyticsService, organization string, project string, chain *GitStorageChainAuditResponse) []GitStorageChainFinding {
	t.Helper()
	_, _ = service, organization
	_ = project
	if chain == nil {
		t.Fatalf("expected chain audit response, got %+v", chain)
	}
	return append([]GitStorageChainFinding(nil), chain.Findings...)
}

func assertHasChainFinding(t *testing.T, findings []GitStorageChainFinding, kind string, path string) GitStorageChainFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return finding
		}
	}
	t.Fatalf("missing chain finding kind=%s path=%s in %+v", kind, path, findings)
	return GitStorageChainFinding{}
}

func assertNoChainFinding(t *testing.T, findings []GitStorageChainFinding, kind string) {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind {
			t.Fatalf("unexpected chain finding kind=%s in %+v", kind, findings)
		}
	}
}
