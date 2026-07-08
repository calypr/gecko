package git

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

type fakeStorageAnalyticsBackend struct {
	projectRecords                       []gintegrationsyfon.ProjectRecord
	projectMetricsSummary                *gintegrationsyfon.ProjectMetricsSummary
	bulkRecords                          map[string][]gintegrationsyfon.ProjectRecord
	buckets                              map[string]domain.StorageBucket
	bucketScopes                         map[string][]domain.StorageBucketScope
	projectScopes                        []domain.StorageBucketScope
	usageByObject                        map[string]gintegrationsyfon.FileUsage
	bulkChecksums                        []string
	probeResults                         map[string]gintegrationsyfon.BulkStorageProbeResult
	listProbeResults                     map[string]gintegrationsyfon.BulkStorageProbeResult
	bucketObjects                        []gintegrationsyfon.ProjectBucketObject
	listProjectBucketObjectsErr          error
	listBucketsCalls                     int
	listBucketScopesCalls                int
	listProjectAuditRecordsCalls         int
	listProjectAuditRecordsPathPrefix    string
	listProjectAuditRecordsDelay         time.Duration
	getProjectMetricsSummaryCalls        int
	listProjectScopesCalls               int
	listProjectFileUsageCalls            int
	listProjectFileUsageByObjectIDsCalls int
	listProjectFileUsageObjectIDs        []string
	listProjectFileUsageByObjectIDsErr   error
	listProjectBucketObjectsCalls        int
	listProjectBucketObjectsPathPrefix   string
	listProjectBucketInventoryCalls      int
	listProjectBucketInventoryPathPrefix string
	listProjectBucketSummaryCalls        int
	listProjectBucketSummaryMode         string
	bulkGetProjectRecordsCalls           int
	bulkGetDelay                         time.Duration
	probeCalls                           int
	probeItems                           []gintegrationsyfon.BulkStorageProbeItem
	listProbeCalls                       int
	listProbeItems                       []gintegrationsyfon.BulkStorageProbeItem
	deletedIDs                           []string
	deletedStorageIDs                    []string
	updatedAccessMethods                 map[string][]gintegrationsyfon.ProjectAccessMethod
	deletedBucketObjects                 []string
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

func (fake *fakeStorageAnalyticsBackend) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error) {
	fake.listProjectAuditRecordsCalls++
	fake.listProjectAuditRecordsPathPrefix = pathPrefix
	if fake.listProjectAuditRecordsDelay > 0 {
		time.Sleep(fake.listProjectAuditRecordsDelay)
	}
	return append([]gintegrationsyfon.ProjectRecord(nil), fake.projectRecords...), nil
}

func (fake *fakeStorageAnalyticsBackend) GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error) {
	fake.getProjectMetricsSummaryCalls++
	if fake.projectMetricsSummary != nil {
		summary := *fake.projectMetricsSummary
		return &summary, nil
	}
	latest := ""
	for _, record := range fake.projectRecords {
		if record.UpdatedAt == nil {
			continue
		}
		formatted := record.UpdatedAt.UTC().Format(time.RFC3339Nano)
		if formatted > latest {
			latest = formatted
		}
	}
	return &gintegrationsyfon.ProjectMetricsSummary{
		RecordCount:             len(fake.projectRecords),
		RecordLatestUpdatedTime: latest,
	}, nil
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
	fake.bulkGetProjectRecordsCalls++
	fake.bulkChecksums = append([]string(nil), checksums...)
	if fake.bulkGetDelay > 0 {
		time.Sleep(fake.bulkGetDelay)
	}
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

func (fake *fakeStorageAnalyticsBackend) ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	fake.listProjectFileUsageByObjectIDsCalls++
	fake.listProjectFileUsageObjectIDs = append(fake.listProjectFileUsageObjectIDs, objectIDs...)
	if fake.listProjectFileUsageByObjectIDsErr != nil {
		return nil, fake.listProjectFileUsageByObjectIDsErr
	}
	out := make(map[string]gintegrationsyfon.FileUsage, len(objectIDs))
	for _, objectID := range objectIDs {
		if usage, ok := fake.usageByObject[objectID]; ok {
			out[objectID] = usage
		}
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

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, error) {
	fake.listProjectBucketInventoryCalls++
	fake.listProjectBucketInventoryPathPrefix = pathPrefix
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
	if deleteStorageData {
		fake.deletedStorageIDs = append(fake.deletedStorageIDs, objectIDs...)
	}
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
	if backend.listProjectFileUsageByObjectIDsCalls != 1 {
		t.Fatalf("expected exact summary to use bulk file usage lookup, got %d", backend.listProjectFileUsageByObjectIDsCalls)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected exact summary to load shared project audit records once, got %d", backend.listProjectAuditRecordsCalls)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected exact summary to join cached project records locally, got %d checksum lookup calls", backend.bulkGetProjectRecordsCalls)
	}
	if contains(backend.listProjectFileUsageObjectIDs, "obj-orphan") {
		t.Fatalf("expected exact summary usage lookup to include only Git-matched records, got %+v", backend.listProjectFileUsageObjectIDs)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected exact summary to skip paginated project usage lookup, got %d", backend.listProjectFileUsageCalls)
	}

	usageCallsBeforeChildren := backend.listProjectFileUsageCalls
	children, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 10, "bytes", "desc", "")
	if err != nil {
		t.Fatalf("build storage children: %v", err)
	}
	if backend.listProjectFileUsageCalls != usageCallsBeforeChildren {
		t.Fatalf("expected storage children to skip project-wide file usage, got calls before=%d after=%d", usageCallsBeforeChildren, backend.listProjectFileUsageCalls)
	}
	if len(children.Items) != 4 {
		t.Fatalf("expected 4 child rows, got %+v", children.Items)
	}
	if children.HasMore || children.NextCursor != "" {
		t.Fatalf("expected unpaginated children response, got %+v", children)
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

	applyResult, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		nil,
		nil,
		[]GitStorageCleanupApplyFinding{
			{Kind: "stale_duplicate_record", NormalizedPath: "data/nested/b.txt", ObjectIDs: []string{"obj-b-old"}},
			{Kind: "repo_orphan_stale_record", NormalizedPath: "s3://bucket/orphan", ObjectIDs: []string{"obj-orphan"}, AccessURLs: []string{"s3://bucket/orphan"}},
		},
		true,
		true,
		false,
		false,
		true,
	)
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

func TestBuildStorageChildrenEnrichesOnlySelectedPage(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/big/a.txt":   lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 600),
		"data/big/b.txt":   lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 300),
		"data/other.txt":   lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 50),
		"data/small.txt":   lfsPointer("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 100),
		"outside/file.txt": lfsPointer("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 1000),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj"},
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj"},
			{ObjectID: "obj-c", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj"},
			{ObjectID: "obj-d", Checksum: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Organization: "org", Project: "proj"},
			{ObjectID: "obj-e", Checksum: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Organization: "org", Project: "proj"},
		},
	}
	service := NewStorageAnalyticsService(backend)

	children, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 1, "bytes", "desc", "")
	if err != nil {
		t.Fatalf("build storage children: %v", err)
	}
	if len(children.Items) != 1 || children.Items[0].Path != "data/big" {
		t.Fatalf("expected only biggest child page, got %+v", children.Items)
	}
	if !children.HasMore || children.NextCursor == "" {
		t.Fatalf("expected next cursor for truncated children page, got %+v", children)
	}
	expectedChecksums := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	if strings.Join(backend.bulkChecksums, ",") != strings.Join(expectedChecksums, ",") {
		t.Fatalf("expected page-scoped checksums %v, got %v", expectedChecksums, backend.bulkChecksums)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected storage children to skip project file usage, got %d calls", backend.listProjectFileUsageCalls)
	}
}

func TestBuildStorageChildrenCursorPagination(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 300),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"data/c.txt": lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 100),
	})
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})

	first, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, "bytes", "desc", "")
	if err != nil {
		t.Fatalf("build first storage children page: %v", err)
	}
	if len(first.Items) != 2 || first.Items[0].Path != "data/a.txt" || first.Items[1].Path != "data/b.txt" {
		t.Fatalf("unexpected first page: %+v", first.Items)
	}
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("expected next cursor on first page, got %+v", first)
	}

	second, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, "bytes", "desc", first.NextCursor)
	if err != nil {
		t.Fatalf("build second storage children page: %v", err)
	}
	if len(second.Items) != 1 || second.Items[0].Path != "data/c.txt" {
		t.Fatalf("unexpected second page: %+v", second.Items)
	}
	if second.HasMore || second.NextCursor != "" {
		t.Fatalf("expected final page without cursor, got %+v", second)
	}
	if second.Items[0].Path == first.Items[0].Path || second.Items[0].Path == first.Items[1].Path {
		t.Fatalf("expected cursor pages not to duplicate rows: first=%+v second=%+v", first.Items, second.Items)
	}
}

func TestBuildStorageFolderCombinesSummaryAndChildren(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 300),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"data/c.txt": lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj"},
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj"},
			{ObjectID: "obj-c", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj"},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-a": {ObjectID: "obj-a", DownloadCount: 3},
			"obj-b": {ObjectID: "obj-b", DownloadCount: 2},
			"obj-c": {ObjectID: "obj-c", DownloadCount: 1},
		},
	}
	service := NewStorageAnalyticsService(backend)

	summary, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build storage summary: %v", err)
	}
	children, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, "bytes", "desc", "")
	if err != nil {
		t.Fatalf("build storage children: %v", err)
	}
	folder, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, "bytes", "desc", "", StorageFolderSummaryModeExact, false, nil)
	if err != nil {
		t.Fatalf("build storage folder: %v", err)
	}
	expectedSummary := *summary
	expectedSummary.Source = StorageFolderSummarySourceExactJoin
	if folder.Summary != expectedSummary {
		t.Fatalf("expected folder summary to match summary endpoint\nfolder=%+v\nsummary=%+v", folder.Summary, *summary)
	}
	if len(folder.Children.Items) != len(children.Items) || folder.Children.HasMore != children.HasMore || folder.Children.NextCursor != children.NextCursor {
		t.Fatalf("expected folder children page shape to match children endpoint\nfolder=%+v\nchildren=%+v", folder.Children, *children)
	}
	for i := range children.Items {
		if folder.Children.Items[i].Path != children.Items[i].Path || folder.Children.Items[i].Type != children.Items[i].Type {
			t.Fatalf("expected folder child ordering to match children endpoint\nfolder=%+v\nchildren=%+v", folder.Children, *children)
		}
	}

	next, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, "bytes", "desc", folder.Children.NextCursor, StorageFolderSummaryModeExact, false, nil)
	if err != nil {
		t.Fatalf("build storage folder next page: %v", err)
	}
	if len(next.Children.Items) != 1 || next.Children.Items[0].Path != "data/c.txt" {
		t.Fatalf("unexpected next folder page: %+v", next.Children)
	}
	if next.Children.HasMore || next.Children.NextCursor != "" {
		t.Fatalf("expected final folder page without cursor, got %+v", next.Children)
	}
}

func TestBuildStorageFolderDefaultModeUsesGitIndexOnly(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":  lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt":  lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"other/c.txt": lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 50),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj"},
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj"},
			{ObjectID: "obj-c", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj"},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-a": {ObjectID: "obj-a", DownloadCount: 1},
			"obj-b": {ObjectID: "obj-b", DownloadCount: 1},
			"obj-c": {ObjectID: "obj-c", DownloadCount: 1},
		},
	}
	service := NewStorageAnalyticsService(backend)

	folder, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, 1, "bytes", "desc", "", "", false, nil)
	if err != nil {
		t.Fatalf("build storage folder: %v", err)
	}
	if folder.Summary.Source != StorageFolderSummarySourceGitIndex {
		t.Fatalf("expected default folder summary source %q, got %+v", StorageFolderSummarySourceGitIndex, folder.Summary)
	}
	if folder.Summary.FileCount != 3 || folder.Summary.RecordCount != 0 || folder.Summary.DownloadCount != 0 || folder.Summary.DuplicatePathCount != 0 {
		t.Fatalf("git-index folder summary should not fake exact Syfon totals, got %+v", folder.Summary)
	}
	if len(folder.Children.Items) != 1 || !folder.Children.HasMore {
		t.Fatalf("unexpected folder response: %+v", folder)
	}
	first := folder.Children.Items[0]
	if first.Path != "data" || first.Type != "directory" || first.FileCount != 2 || first.TotalBytes != 300 {
		t.Fatalf("expected first page to use Git directory aggregate, got %+v", folder.Children.Items)
	}
	if first.RecordCount != 0 || first.DownloadCount != 0 || first.LastDownloadTime != "" {
		t.Fatalf("git-index child should not include Syfon/download enrichment, got %+v", first)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected default folder mode to skip Syfon record lookup, got %d", backend.bulkGetProjectRecordsCalls)
	}
	if len(backend.bulkChecksums) != 0 {
		t.Fatalf("expected default folder mode to avoid descendant checksum expansion, got %v", backend.bulkChecksums)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected default folder mode to skip project usage lookup, got %d calls", backend.listProjectFileUsageCalls)
	}
	if backend.listProjectBucketObjectsCalls != 0 || backend.listProjectBucketSummaryCalls != 0 || backend.probeCalls != 0 {
		t.Fatalf("expected default folder mode to skip bucket APIs, got objects=%d summary=%d probes=%d", backend.listProjectBucketObjectsCalls, backend.listProjectBucketSummaryCalls, backend.probeCalls)
	}
}

func TestBuildStorageFolderDefaultModeUsesPreSortedServingIndex(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/alpha.txt":   lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 300),
		"data/bravo.txt":   lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100),
		"data/charlie.txt": lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 200),
	})
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})

	tests := map[string]struct {
		sortBy    string
		sortOrder string
		expected  []string
	}{
		"bytes desc": {sortBy: "bytes", sortOrder: "desc", expected: []string{"data/alpha.txt", "data/charlie.txt", "data/bravo.txt"}},
		"bytes asc":  {sortBy: "bytes", sortOrder: "asc", expected: []string{"data/bravo.txt", "data/charlie.txt", "data/alpha.txt"}},
		"name asc":   {sortBy: "name", sortOrder: "asc", expected: []string{"data/alpha.txt", "data/bravo.txt", "data/charlie.txt"}},
		"name desc":  {sortBy: "name", sortOrder: "desc", expected: []string{"data/charlie.txt", "data/bravo.txt", "data/alpha.txt"}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			folder, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, test.sortBy, test.sortOrder, "", "", false, nil)
			if err != nil {
				t.Fatalf("build storage folder: %v", err)
			}
			if got := storageChildResponsePaths(folder.Children.Items); strings.Join(got, ",") != strings.Join(test.expected[:2], ",") {
				t.Fatalf("unexpected first page order: got %v want %v", got, test.expected[:2])
			}
			if !folder.Children.HasMore || folder.Children.NextCursor == "" {
				t.Fatalf("expected first page cursor, got %+v", folder.Children)
			}
			next, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 2, test.sortBy, test.sortOrder, folder.Children.NextCursor, "", false, nil)
			if err != nil {
				t.Fatalf("build storage folder next page: %v", err)
			}
			if got := storageChildResponsePaths(next.Children.Items); strings.Join(got, ",") != test.expected[2] {
				t.Fatalf("unexpected next page order: got %v want %v", got, test.expected[2:])
			}
			if next.Children.HasMore || next.Children.NextCursor != "" {
				t.Fatalf("expected final page without cursor, got %+v", next.Children)
			}
		})
	}
}

func TestBuildStorageFolderDefaultModeUsesSidecarServingIndex(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	if err := PersistRepoAnalyticsIndex(context.Background(), mirrorPath, repo, refName, hash); err != nil {
		t.Fatalf("persist repo analytics index: %v", err)
	}
	repoAnalyticsIndexCache.mu.Lock()
	repoAnalyticsIndexCache.entries = map[string]*repoAnalyticsIndex{}
	repoAnalyticsIndexCache.inflight = map[string]*inflightRepoAnalyticsIndex{}
	repoAnalyticsIndexCache.mu.Unlock()

	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	folder, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 1, "bytes", "desc", "", "", false, nil)
	if err != nil {
		t.Fatalf("build storage folder from sidecar: %v", err)
	}
	if got := storageChildResponsePaths(folder.Children.Items); strings.Join(got, ",") != "data/b.txt" {
		t.Fatalf("expected sidecar serving index to preserve bytes desc order, got %v", got)
	}
	if !folder.Children.HasMore || folder.Children.NextCursor == "" {
		t.Fatalf("expected sidecar serving index cursor, got %+v", folder.Children)
	}
}

func storageChildResponsePaths(items []GitStorageChildResponseItem) []string {
	paths := make([]string, 0, len(items))
	for _, item := range items {
		paths = append(paths, item.Path)
	}
	return paths
}

func TestBuildStorageChildrenRejectsStaleCursor(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 300),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	wrongHash := plumbing.NewHash("cccccccccccccccccccccccccccccccccccccccc")
	staleCursors := map[string]string{
		"commit":     buildStorageChildrenCursor(wrongHash, "data", "bytes", "desc", 1),
		"path":       buildStorageChildrenCursor(hash, "other", "bytes", "desc", 1),
		"sort_by":    buildStorageChildrenCursor(hash, "data", "name", "desc", 1),
		"sort_order": buildStorageChildrenCursor(hash, "data", "bytes", "asc", 1),
		"invalid":    "not-base64",
	}
	for name, cursor := range staleCursors {
		t.Run(name, func(t *testing.T) {
			_, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 1, "bytes", "desc", cursor)
			if err == nil {
				t.Fatalf("expected stale cursor %q to fail", name)
			}
		})
	}
}

func TestApplyStorageCleanupMatchesSelectedRepoOrphanByAnyAccessURL(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{}
	service := NewStorageAnalyticsService(backend)

	applyResult, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"s3://bucket/orphan-selected"},
		nil,
		[]GitStorageCleanupApplyFinding{{
			Kind:           "repo_orphan_stale_record",
			NormalizedPath: "s3://bucket/orphan-primary",
			ObjectIDs:      []string{"obj-orphan"},
			AccessURLs:     []string{"s3://bucket/orphan-primary", "s3://bucket/orphan-selected"},
		}},
		true,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if !contains(applyResult.DeletedRecordIDs, "obj-orphan") {
		t.Fatalf("expected selected orphan to be deleted, got %+v", applyResult)
	}
	if len(applyResult.SkippedPaths) != 0 || len(applyResult.ManualPaths) != 0 {
		t.Fatalf("expected selected orphan not to be skipped or manual, got %+v", applyResult)
	}
	if !contains(backend.deletedIDs, "obj-orphan") {
		t.Fatalf("expected syfon delete call for selected orphan, got %+v", backend.deletedIDs)
	}
}

func TestApplyStorageCleanupRejectsUnmatchedSelectedPaths(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{}
	service := NewStorageAnalyticsService(backend)

	_, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"s3://other-bucket/missing"},
		nil,
		[]GitStorageCleanupApplyFinding{{
			Kind:           "repo_orphan_stale_record",
			NormalizedPath: "s3://bucket/a",
			ObjectIDs:      []string{"obj-a"},
			AccessURLs:     []string{"s3://bucket/a"},
		}},
		true,
		false,
		false,
		false,
		false,
	)
	if err == nil {
		t.Fatalf("expected unmatched selected path to fail")
	}
	if !strings.Contains(err.Error(), "selected cleanup paths did not match provided cleanup findings") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyStorageCleanupRepairsBrokenBucketMappings(t *testing.T) {
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

	applyResult, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"data/a.txt", "data/b.txt"},
		nil,
		[]GitStorageCleanupApplyFinding{
			{
				Kind:           "broken_bucket_mapping",
				NormalizedPath: "data/a.txt",
				ObjectIDs:      []string{"obj-a"},
				Records: []GitStorageCleanupRecordAudit{{
					ObjectID:      "obj-a",
					AccessURLs:    []string{"s3://legacy/a", "s3://bucket/a"},
					AccessMethods: []GitStorageCleanupAccessMethod{{URL: "s3://legacy/a"}, {URL: "s3://bucket/a"}},
					AccessProbes: []GitStorageCleanupAccessProbe{
						{URL: "s3://legacy/a", Status: "error", ErrorKind: "credential_missing"},
						{URL: "s3://bucket/a", Status: "present"},
					},
				}},
			},
			{
				Kind:           "broken_bucket_mapping",
				NormalizedPath: "data/b.txt",
				ObjectIDs:      []string{"obj-b"},
				Records: []GitStorageCleanupRecordAudit{{
					ObjectID:      "obj-b",
					AccessURLs:    []string{"s3://legacy/b"},
					AccessMethods: []GitStorageCleanupAccessMethod{{URL: "s3://legacy/b"}},
					AccessProbes:  []GitStorageCleanupAccessProbe{{URL: "s3://legacy/b", Status: "error", ErrorKind: "credential_missing"}},
				}},
			},
		},
		false,
		false,
		false,
		true,
		false,
	)
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

	applyResult, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"s3://bucket/orphan"},
		nil,
		[]GitStorageCleanupApplyFinding{{
			Kind:             "bucket_only_object",
			NormalizedPath:   "s3://bucket/orphan",
			BucketObjectURL:  "s3://bucket/orphan",
			BucketObjectURLs: []string{"s3://bucket/orphan"},
		}},
		false,
		false,
		true,
		false,
		false,
	)
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
	if mismatchFinding.Actionability != "manual_choice" ||
		!contains(mismatchFinding.AvailableActions, "delete_syfon_record") ||
		!contains(mismatchFinding.AvailableActions, "delete_bucket_object") ||
		!contains(mismatchFinding.AvailableActions, "delete_both") ||
		mismatchFinding.DefaultAction != "" ||
		!mismatchFinding.SupportsDryRun {
		t.Fatalf("expected mismatch action metadata, got %+v", mismatchFinding)
	}
}

func TestApplyStorageCleanupSupportsMetadataMismatchActions(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/b"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:                   storageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:            "s3://bucket/b",
				Bucket:               "bucket",
				Key:                  "b",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"size_mismatch"},
			},
		},
	}
	service := NewStorageAnalyticsService(backend)
	applyFindings := []GitStorageCleanupApplyFinding{{
		Kind:             "storage_validation_mismatch",
		NormalizedPath:   "data/b.txt",
		ObjectIDs:        []string{"obj-b"},
		BucketObjectURL:  "s3://bucket/b",
		BucketObjectURLs: []string{"s3://bucket/b"},
		AccessURLs:       []string{"s3://bucket/b"},
	}}

	deleteRecordOnly, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"data/b.txt"},
		[]GitStorageCleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_syfon_record"}},
		applyFindings,
		false,
		false,
		false,
		false,
		true,
	)
	if err != nil {
		t.Fatalf("apply cleanup delete syfon record dry run: %v", err)
	}
	if len(deleteRecordOnly.DeletedRecordIDs) != 1 || deleteRecordOnly.DeletedRecordIDs[0] != "obj-b" {
		t.Fatalf("expected mismatch delete-record action to select Syfon record, got %+v", deleteRecordOnly)
	}
	if len(deleteRecordOnly.DeletedBucketObjectURLs) != 0 {
		t.Fatalf("expected record-only action to avoid bucket delete, got %+v", deleteRecordOnly)
	}

	deleteBucketOnly, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"data/b.txt"},
		[]GitStorageCleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_bucket_object"}},
		applyFindings,
		false,
		false,
		false,
		false,
		true,
	)
	if err != nil {
		t.Fatalf("apply cleanup delete bucket object dry run: %v", err)
	}
	if len(deleteBucketOnly.DeletedRecordIDs) != 0 {
		t.Fatalf("expected bucket-only action to avoid Syfon delete, got %+v", deleteBucketOnly)
	}
	if len(deleteBucketOnly.DeletedBucketObjectURLs) != 1 || deleteBucketOnly.DeletedBucketObjectURLs[0] != "s3://bucket/b" {
		t.Fatalf("expected mismatch delete-bucket action to select bucket object, got %+v", deleteBucketOnly)
	}

	deleteBoth, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"data/b.txt"},
		[]GitStorageCleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_both"}},
		applyFindings,
		false,
		false,
		false,
		false,
		true,
	)
	if err != nil {
		t.Fatalf("apply cleanup delete both dry run: %v", err)
	}
	if len(deleteBoth.DeletedRecordIDs) != 1 || deleteBoth.DeletedRecordIDs[0] != "obj-b" {
		t.Fatalf("expected dual delete action to include Syfon record, got %+v", deleteBoth)
	}
	if len(deleteBoth.DeletedBucketObjectURLs) != 1 || deleteBoth.DeletedBucketObjectURLs[0] != "s3://bucket/b" {
		t.Fatalf("expected dual delete action to include bucket object, got %+v", deleteBoth)
	}
}

func TestStorageRepairPolicyCoversFindingKinds(t *testing.T) {
	tests := []struct {
		kind          string
		defaultAction string
		actions       []string
		actionability string
	}{
		{"bucket_only_object", storageActionDeleteBucketObject, []string{storageActionDeleteBucketObject}, storageActionabilityAutoRepair},
		{"bucket_syfon_no_git", storageActionDeleteBoth, []string{storageActionDeleteBoth, storageActionDeleteSyfonRecord, storageActionDeleteBucketObject}, storageActionabilityAutoRepair},
		{"repo_orphan_live_object", storageActionDeleteBoth, []string{storageActionDeleteBoth, storageActionDeleteSyfonRecord, storageActionDeleteBucketObject}, storageActionabilityAutoRepair},
		{"repo_orphan_stale_record", storageActionDeleteSyfonRecord, []string{storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"stale_duplicate_record", storageActionDeleteSyfonRecord, []string{storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"live_duplicate_conflict", storageActionInspectEvidence, []string{storageActionInspectEvidence}, storageActionabilityInspectOnly},
		{"broken_access_url_error", storageActionRemoveBrokenAccessURLs, []string{storageActionRemoveBrokenAccessURLs, storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"broken_bucket_mapping", storageActionRemoveBrokenAccessURLs, []string{storageActionRemoveBrokenAccessURLs, storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"syfon_broken_bucket_mapping", storageActionRemoveBrokenAccessURLs, []string{storageActionRemoveBrokenAccessURLs, storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"storage_validation_mismatch", "", []string{storageActionDeleteSyfonRecord, storageActionDeleteBucketObject, storageActionDeleteBoth}, storageActionabilityManualChoice},
		{"git_syfon_metadata_mismatch", "", []string{storageActionDeleteSyfonRecord, storageActionDeleteBucketObject, storageActionDeleteBoth}, storageActionabilityManualChoice},
		{"storage_object_missing", storageActionDeleteSyfonRecord, []string{storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"syfon_git_no_bucket", storageActionDeleteSyfonRecord, []string{storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"syfon_missing_bucket_object", storageActionDeleteSyfonRecord, []string{storageActionDeleteSyfonRecord}, storageActionabilityAutoRepair},
		{"git_only_no_syfon", storageActionInspectEvidence, []string{storageActionInspectEvidence}, storageActionabilityInspectOnly},
		{"storage_probe_error", storageActionInspectEvidence, []string{storageActionInspectEvidence}, storageActionabilityInspectOnly},
		{"probe_error", storageActionInspectEvidence, []string{storageActionInspectEvidence}, storageActionabilityInspectOnly},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			policy := storageRepairPolicyForKind(test.kind)
			if policy.defaultAction != test.defaultAction {
				t.Fatalf("expected default %q, got %q", test.defaultAction, policy.defaultAction)
			}
			if policy.actionability != test.actionability {
				t.Fatalf("expected actionability %q, got %q", test.actionability, policy.actionability)
			}
			for _, action := range test.actions {
				if !contains(policy.actions, action) {
					t.Fatalf("expected action %q in %+v", action, policy.actions)
				}
			}
		})
	}
}

func TestFilterStorageChainSummaryPreservesCleanJoinCount(t *testing.T) {
	summary := GitStorageChainAuditSummary{
		CountsByKind: map[string]int{
			"bucket_syfon_git_complete":   7,
			"git_syfon_metadata_mismatch": 3,
			"syfon_broken_bucket_mapping": 2,
		},
		BucketObjectCount:   10,
		SyfonRecordCount:    10,
		GitTrackedFileCount: 10,
	}
	findings := []GitStorageChainFinding{
		{Kind: "git_syfon_metadata_mismatch", NormalizedPath: "data/a.txt"},
	}

	filtered := filterStorageChainSummary(summary, findings)

	if got := filtered.CountsByKind["bucket_syfon_git_complete"]; got != 7 {
		t.Fatalf("expected clean-chain count to survive filtering, got %d in %+v", got, filtered)
	}
	if got := filtered.CountsByKind["git_syfon_metadata_mismatch"]; got != 1 {
		t.Fatalf("expected filtered mismatch count from findings, got %d in %+v", got, filtered)
	}
	if got := filtered.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected filtered broken-mapping count to reset, got %d in %+v", got, filtered)
	}
}

func TestApplyStorageCleanupRepairMatrix(t *testing.T) {
	tests := []struct {
		name                string
		finding             GitStorageCleanupApplyFinding
		actions             []GitStorageCleanupApplyAction
		wantDeletedIDs      []string
		wantStorageIDs      []string
		wantBucketURLs      []string
		wantUpdatedRecordID string
		wantManualPath      string
	}{
		{name: "bucket only", finding: applyFinding("bucket_only_object", "s3://bucket/only", nil, []string{"s3://bucket/only"}), wantBucketURLs: []string{"s3://bucket/only"}},
		{name: "bucket syfon no git", finding: applyFinding("bucket_syfon_no_git", "s3://bucket/no-git", []string{"obj-a"}, []string{"s3://bucket/no-git"}), wantDeletedIDs: []string{"obj-a"}, wantBucketURLs: []string{"s3://bucket/no-git"}},
		{name: "repo orphan live", finding: applyFinding("repo_orphan_live_object", "s3://bucket/live", []string{"obj-live"}, nil), wantDeletedIDs: []string{"obj-live"}, wantStorageIDs: []string{"obj-live"}},
		{name: "repo orphan stale", finding: applyFinding("repo_orphan_stale_record", "s3://bucket/stale", []string{"obj-stale"}, nil), wantDeletedIDs: []string{"obj-stale"}},
		{name: "stale duplicate", finding: applyFinding("stale_duplicate_record", "data/a.txt", []string{"obj-old"}, nil), wantDeletedIDs: []string{"obj-old"}},
		{name: "storage missing", finding: applyFinding("storage_object_missing", "data/missing.txt", []string{"obj-missing"}, nil), wantDeletedIDs: []string{"obj-missing"}},
		{name: "syfon git no bucket", finding: applyFinding("syfon_git_no_bucket", "data/missing.txt", []string{"obj-missing"}, nil), wantDeletedIDs: []string{"obj-missing"}},
		{name: "syfon missing bucket object", finding: applyFinding("syfon_missing_bucket_object", "s3://bucket/missing", []string{"obj-missing"}, nil), wantDeletedIDs: []string{"obj-missing"}},
		{name: "git syfon mismatch delete both", finding: applyFinding("git_syfon_metadata_mismatch", "data/mismatch.txt", []string{"obj-mm"}, []string{"s3://bucket/mismatch"}), actions: []GitStorageCleanupApplyAction{{Kind: "git_syfon_metadata_mismatch", NormalizedPath: "data/mismatch.txt", Action: storageActionDeleteBoth}}, wantDeletedIDs: []string{"obj-mm"}, wantBucketURLs: []string{"s3://bucket/mismatch"}},
		{name: "storage mismatch delete both", finding: applyFinding("storage_validation_mismatch", "data/mismatch.txt", []string{"obj-mm"}, []string{"s3://bucket/mismatch"}), actions: []GitStorageCleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/mismatch.txt", Action: storageActionDeleteBoth}}, wantDeletedIDs: []string{"obj-mm"}, wantBucketURLs: []string{"s3://bucket/mismatch"}},
		{name: "live duplicate conflict", finding: applyFinding("live_duplicate_conflict", "data/dup.txt", []string{"obj-a", "obj-b"}, nil), wantManualPath: "data/dup.txt"},
		{name: "git only no syfon", finding: applyFinding("git_only_no_syfon", "data/git-only.txt", nil, nil), wantManualPath: "data/git-only.txt"},
		{name: "probe error", finding: applyFinding("probe_error", "data/probe.txt", []string{"obj-probe"}, nil), wantManualPath: "data/probe.txt"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeStorageAnalyticsBackend{}
			service := NewStorageAnalyticsService(backend)
			result, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", nil, test.actions, []GitStorageCleanupApplyFinding{test.finding}, false, false, false, false, false)
			if err != nil {
				t.Fatalf("apply cleanup: %v", err)
			}
			assertStringSet(t, "deleted IDs", result.DeletedRecordIDs, test.wantDeletedIDs)
			assertStringSet(t, "storage IDs", backend.deletedStorageIDs, test.wantStorageIDs)
			assertStringSet(t, "bucket URLs", result.DeletedBucketObjectURLs, test.wantBucketURLs)
			if test.wantManualPath != "" && !contains(result.ManualPaths, test.wantManualPath) {
				t.Fatalf("expected manual path %q, got %+v", test.wantManualPath, result.ManualPaths)
			}
			if test.wantUpdatedRecordID != "" && !contains(result.UpdatedRecordIDs, test.wantUpdatedRecordID) {
				t.Fatalf("expected updated record %q, got %+v", test.wantUpdatedRecordID, result.UpdatedRecordIDs)
			}
		})
	}
}

func TestApplyStorageCleanupPrunesBrokenAccessMethods(t *testing.T) {
	tests := []struct {
		name           string
		record         GitStorageCleanupRecordAudit
		wantDeleted    []string
		wantRemaining  []string
		wantErrSnippet string
	}{
		{
			name: "keeps good access method",
			record: GitStorageCleanupRecordAudit{
				ObjectID:      "obj-a",
				AccessURLs:    []string{"s3://legacy/a", "s3://bucket/a"},
				AccessMethods: []GitStorageCleanupAccessMethod{{URL: "s3://legacy/a"}, {URL: "s3://bucket/a"}},
				AccessProbes: []GitStorageCleanupAccessProbe{
					{URL: "s3://legacy/a", Status: "error", ErrorKind: "credential_missing"},
					{URL: "s3://bucket/a", Status: "present"},
				},
			},
			wantRemaining: []string{"s3://bucket/a"},
		},
		{
			name: "deletes record when only access method is broken",
			record: GitStorageCleanupRecordAudit{
				ObjectID:      "obj-b",
				AccessURLs:    []string{"s3://legacy/b"},
				AccessMethods: []GitStorageCleanupAccessMethod{{URL: "s3://legacy/b"}},
				AccessProbes:  []GitStorageCleanupAccessProbe{{URL: "s3://legacy/b", Status: "error", ErrorKind: "credential_missing"}},
			},
			wantDeleted: []string{"obj-b"},
		},
		{
			name: "missing evidence fails",
			record: GitStorageCleanupRecordAudit{
				ObjectID:      "obj-c",
				AccessURLs:    []string{"s3://legacy/c"},
				AccessMethods: []GitStorageCleanupAccessMethod{{URL: "s3://legacy/c"}},
			},
			wantErrSnippet: "missing broken access URL evidence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeStorageAnalyticsBackend{}
			service := NewStorageAnalyticsService(backend)
			_, err := service.ApplyStorageCleanup(
				context.Background(),
				"Bearer token",
				"org",
				"proj",
				nil,
				nil,
				[]GitStorageCleanupApplyFinding{{
					Kind:           "broken_access_url_error",
					NormalizedPath: "data/a.txt",
					ObjectIDs:      []string{test.record.ObjectID},
					Records:        []GitStorageCleanupRecordAudit{test.record},
				}},
				false,
				false,
				false,
				false,
				false,
			)
			if test.wantErrSnippet != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrSnippet) {
					t.Fatalf("expected error containing %q, got %v", test.wantErrSnippet, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("apply cleanup: %v", err)
			}
			assertStringSet(t, "deleted IDs", backend.deletedIDs, test.wantDeleted)
			if len(test.wantRemaining) > 0 {
				methods := backend.updatedAccessMethods[test.record.ObjectID]
				got := make([]string, 0, len(methods))
				for _, method := range methods {
					got = append(got, method.URL)
				}
				assertStringSet(t, "remaining access methods", got, test.wantRemaining)
			}
		})
	}
}

func TestApplyStorageCleanupValidation(t *testing.T) {
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	tests := []struct {
		name    string
		finding GitStorageCleanupApplyFinding
		actions []GitStorageCleanupApplyAction
		wantErr string
	}{
		{name: "unknown kind", finding: applyFinding("unknown_kind", "data/a.txt", []string{"obj-a"}, nil), wantErr: "unsupported cleanup finding kind"},
		{name: "unsupported action", finding: applyFinding("bucket_only_object", "s3://bucket/a", nil, []string{"s3://bucket/a"}), actions: []GitStorageCleanupApplyAction{{Kind: "bucket_only_object", NormalizedPath: "s3://bucket/a", Action: storageActionDeleteSyfonRecord}}, wantErr: "not supported"},
		{name: "action not advertised", finding: func() GitStorageCleanupApplyFinding {
			finding := applyFinding("bucket_only_object", "s3://bucket/a", nil, []string{"s3://bucket/a"})
			finding.AvailableActions = []string{storageActionInspectEvidence}
			return finding
		}(), wantErr: "not advertised"},
		{name: "missing object ids", finding: applyFinding("repo_orphan_stale_record", "s3://bucket/a", nil, nil), wantErr: "missing object_ids"},
		{name: "missing bucket urls", finding: applyFinding("bucket_only_object", "data/a.txt", nil, nil), wantErr: "missing bucket object URLs"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", nil, test.actions, []GitStorageCleanupApplyFinding{test.finding}, false, false, false, false, false)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
			}
		})
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
			if len(finding.Records) != 1 || finding.Records[0].Error != "Syfon access URL did not resolve through a configured bucket mapping" {
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
			if finding.Actionability != "auto_repair" ||
				!contains(finding.AvailableActions, "remove_broken_access_urls") ||
				finding.DefaultAction != "remove_broken_access_urls" ||
				!finding.SupportsDryRun {
				t.Fatalf("expected broken mapping action metadata, got %+v", finding)
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
	if backend.listProjectAuditRecordsPathPrefix != "" {
		t.Fatalf("expected audit to load project-scoped records for cache reuse, got %q", backend.listProjectAuditRecordsPathPrefix)
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
			{ObjectURL: "s3://bucket/mismatch.txt", Bucket: "bucket", Key: "mismatch.txt", Path: "mismatch.txt", SizeBytes: 200},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		refName,
		"",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err != nil {
		t.Fatalf("build validate chain audit: %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 0 {
		t.Fatalf("expected validate mode to skip browse bucket inventory endpoint, got %d calls", backend.listProjectBucketObjectsCalls)
	}
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected validate mode to load project inventory once, got %d calls", backend.listProjectBucketInventoryCalls)
	}
	if backend.listProjectBucketSummaryCalls != 0 {
		t.Fatalf("expected validate mode to skip bucket summary preflight, got %d calls", backend.listProjectBucketSummaryCalls)
	}
	if backend.probeCalls != 0 {
		t.Fatalf("expected validate mode to skip HEAD probes, got %d calls", backend.probeCalls)
	}
	if backend.listProbeCalls != 0 || len(backend.listProbeItems) != 0 {
		t.Fatalf("expected project inventory validation to skip bulk LIST calls, got calls=%d items=%+v", backend.listProbeCalls, backend.listProbeItems)
	}
	if chain.Summary.BucketPathExists != nil || chain.Summary.BucketSummaryMode != "" {
		t.Fatalf("expected validate mode not to claim bucket prefix summary, got %+v", chain.Summary)
	}
	if chain.Summary.ValidationMode != StorageChainValidationModeList {
		t.Fatalf("expected LIST validation mode, got %+v", chain.Summary)
	}
	if chain.Summary.BucketObjectCount != 2 || chain.Summary.CountsByKind["bucket_only_object"] != 0 {
		t.Fatalf("expected validate mode to count project inventory without bucket-only findings, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["bucket_syfon_git_complete"] != 2 {
		t.Fatalf("expected validate mode to count clean Syfon/Git/bucket join, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["syfon_missing_bucket_object"] != 1 {
		t.Fatalf("expected validate mode to report missing Syfon bucket object, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected basename mismatch not to be a broken bucket mapping, got summary %+v", chain.Summary)
	}
	assertNoChainFinding(t, chain.Findings, "bucket_only_object")
	assertNoChainFinding(t, chain.Findings, "syfon_broken_bucket_mapping")
	assertHasChainFinding(t, chain.Findings, "syfon_missing_bucket_object", "s3://bucket/syfon-only.txt")
}

func TestBuildStorageChainAuditMetadataModeUsesMetadataValidation(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/a.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			storageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:                   storageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:            "s3://bucket/a.txt",
				Bucket:               "bucket",
				Key:                  "a.txt",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"sha256_mismatch"},
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
		StorageChainAuditOptions{
			BucketInventoryMode: StorageChainBucketModeValidate,
			ValidationMode:      StorageChainValidationModeMetadata,
		},
	)
	if err != nil {
		t.Fatalf("build metadata chain audit: %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 0 {
		t.Fatalf("expected metadata validation to skip recursive bucket inventory, got %d calls", backend.listProjectBucketObjectsCalls)
	}
	if backend.listProbeCalls != 0 {
		t.Fatalf("expected metadata validation to skip LIST validation, got %d calls", backend.listProbeCalls)
	}
	if backend.probeCalls != 1 || len(backend.probeItems) != 1 {
		t.Fatalf("expected one metadata probe call, got calls=%d items=%+v", backend.probeCalls, backend.probeItems)
	}
	if chain.Summary.ValidationMode != StorageChainValidationModeMetadata {
		t.Fatalf("expected metadata validation mode, got %+v", chain.Summary)
	}
	finding := assertHasChainFinding(t, chain.Findings, "git_syfon_metadata_mismatch", "data/a.txt")
	if finding.Evidence == nil || !contains(finding.Evidence.StorageOperations, StorageChainValidationModeMetadata) {
		t.Fatalf("expected metadata operation evidence, got %+v", finding.Evidence)
	}
}

func TestBuildStorageChainAuditIgnoresOneByteListSizeDrift(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 44),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Name: "a.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 44, AccessURLs: []string{"s3://bucket/data/a.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/a.txt", Bucket: "bucket", Key: "data/a.txt", Path: "data/a.txt", SizeBytes: 43},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		ValidationMode:      StorageChainValidationModeList,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["git_syfon_metadata_mismatch"]; got != 0 {
		t.Fatalf("expected one-byte size drift to be ignored, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected one-byte size drift to count as complete, got summary %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditSuggestsFixForLargeSizeDrift(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 44),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Name: "a.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 44, AccessURLs: []string{"s3://bucket/data/a.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/a.txt", Bucket: "bucket", Key: "data/a.txt", Path: "data/a.txt", SizeBytes: 40},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		ValidationMode:      StorageChainValidationModeList,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	finding := assertHasChainFinding(t, chain.Findings, "git_syfon_metadata_mismatch", "data/a.txt")
	if !strings.Contains(finding.SuggestedFix, "Syfon/Git size is 44 B, bucket inventory reports 40 B") {
		t.Fatalf("expected size-drift suggested fix, got %+v", finding)
	}
	if finding.SuggestedAction != "" {
		t.Fatalf("did not expect fake suggested action for size drift, got %+v", finding)
	}
}

func TestBuildStorageChainAuditIgnoresBucketBasenameMismatch(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/right.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Name:         "right.txt",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				AccessURLs:   []string{"s3://bucket/data/wrong.txt"},
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
					{AccessID: "s3", Type: "s3", URL: "s3://bucket/data/wrong.txt"},
				},
			},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/wrong.txt", Bucket: "bucket", Key: "data/wrong.txt", Path: "data/wrong.txt", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		ValidationMode:      StorageChainValidationModeList,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["git_syfon_metadata_mismatch"]; got != 0 {
		t.Fatalf("expected basename-only mismatch to be ignored, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected basename-only mismatch not to be a broken mapping, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected basename-only mismatch to count as complete, got summary %+v", chain.Summary)
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
	if backend.listProjectAuditRecordsPathPrefix != "" {
		t.Fatalf("expected cached project records to load the full project, got %q", backend.listProjectAuditRecordsPathPrefix)
	}
	if backend.getProjectMetricsSummaryCalls != 2 {
		t.Fatalf("expected one cheap summary check per audit, got %d", backend.getProjectMetricsSummaryCalls)
	}
	if backend.listProjectScopesCalls != 1 {
		t.Fatalf("expected cached project scopes, got %d calls", backend.listProjectScopesCalls)
	}
	if backend.listProjectBucketObjectsCalls != 1 {
		t.Fatalf("expected cached project bucket inventory, got %d calls", backend.listProjectBucketObjectsCalls)
	}
}

func TestBuildStorageChainAuditCachesFullResponseAcrossFindingViews(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Name: "present.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
			{ObjectID: "obj-syfon-only", Name: "syfon-only.txt", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, AccessURLs: []string{"s3://bucket/data/syfon-only.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/data/syfon-only.txt", Bucket: "bucket", Key: "data/syfon-only.txt", Path: "data/syfon-only.txt", SizeBytes: 200},
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainAuditResponseCache = newMemoryStorageChainAuditResponseCache()

	first, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		FindingKind:         "git_only_no_syfon",
		FindingLimit:        -1,
	})
	if err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	if first.Summary.AuditCacheHit {
		t.Fatalf("expected first response to build fresh, got %+v", first.Summary)
	}
	if first.Summary.ReturnedFindings != 0 || first.Summary.FindingsTruncated {
		t.Fatalf("expected first response to apply request finding kind, got %+v", first.Summary)
	}

	second, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		FindingKind:         "bucket_syfon_no_git",
		FindingLimit:        -1,
	})
	if err != nil {
		t.Fatalf("build second chain audit: %v", err)
	}
	if !second.Summary.AuditCacheHit {
		t.Fatalf("expected second response to come from audit response cache, got %+v", second.Summary)
	}
	if len(second.Findings) != 1 || second.Findings[0].Kind != "bucket_syfon_no_git" {
		t.Fatalf("expected cached full response to serve issue-kind view, got %+v", second.Findings)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected cached full response to skip Syfon record reload, got %d calls", backend.listProjectAuditRecordsCalls)
	}
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected cached full response to skip bucket inventory reload, got %d calls", backend.listProjectBucketInventoryCalls)
	}
}

func TestBuildStorageChainAuditProjectsGitSubpathFromRootResponseCache(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"other/clean.txt":  lfsPointer("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 300),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Name: "present.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
			{ObjectID: "obj-clean", Name: "clean.txt", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Organization: "org", Project: "proj", Size: 300, AccessURLs: []string{"s3://bucket/other/clean.txt"}},
			{ObjectID: "obj-syfon-only", Name: "syfon-only.txt", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, AccessURLs: []string{"s3://bucket/data/syfon-only.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/other/clean.txt", Bucket: "bucket", Key: "other/clean.txt", Path: "other/clean.txt", SizeBytes: 300},
			{ObjectURL: "s3://bucket/data/syfon-only.txt", Bucket: "bucket", Key: "data/syfon-only.txt", Path: "data/syfon-only.txt", SizeBytes: 200},
		},
	}
	service := NewStorageAnalyticsService(backend)
	cache := newMemoryStorageChainAuditResponseCache()
	service.chainAuditResponseCache = cache
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	root, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options)
	if err != nil {
		t.Fatalf("build root chain audit: %v", err)
	}
	if root.Summary.AuditCacheHit {
		t.Fatalf("expected root audit to build fresh, got %+v", root.Summary)
	}
	if backend.listProjectAuditRecordsCalls != 1 || backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected root audit to load Syfon records and bucket inventory once, got records=%d bucket=%d", backend.listProjectAuditRecordsCalls, backend.listProjectBucketInventoryCalls)
	}
	subpath, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options)
	if err != nil {
		t.Fatalf("build subpath chain audit: %v", err)
	}
	if !subpath.Summary.AuditCacheHit || subpath.Summary.AuditCacheSource != "memory:root" {
		t.Fatalf("expected subpath audit to project from root response cache, got %+v", subpath.Summary)
	}
	if subpath.PathPrefix != "data" || subpath.Summary.GitTrackedFileCount != 1 {
		t.Fatalf("expected data-scoped audit projection, got path=%q summary=%+v", subpath.PathPrefix, subpath.Summary)
	}
	if got := subpath.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected subpath complete count from Git inventory, got summary %+v", subpath.Summary)
	}
	if got := subpath.Summary.CountsByKind["bucket_syfon_no_git"]; got != 1 {
		t.Fatalf("expected subpath Syfon-only finding from root cache, got summary %+v findings=%+v", subpath.Summary, subpath.Findings)
	}
	if backend.listProjectAuditRecordsCalls != 1 || backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected subpath projection to skip Syfon records and bucket inventory, got records=%d bucket=%d", backend.listProjectAuditRecordsCalls, backend.listProjectBucketInventoryCalls)
	}
	cache.mu.RLock()
	cacheEntryCount := len(cache.entries)
	cache.mu.RUnlock()
	if cacheEntryCount != 1 {
		t.Fatalf("expected only root response cache entry, got %d", cacheEntryCount)
	}
}

func TestBuildStorageChainAuditForceRefreshBypassesResponseCache(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Name: "present.txt", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainAuditResponseCache = newMemoryStorageChainAuditResponseCache()
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	options.ForceAuditRefresh = true
	refreshed, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options)
	if err != nil {
		t.Fatalf("build forced chain audit: %v", err)
	}
	if refreshed.Summary.AuditCacheHit {
		t.Fatalf("expected forced response to rebuild fresh, got %+v", refreshed.Summary)
	}
	if backend.listProjectBucketInventoryCalls != 2 {
		t.Fatalf("expected forced refresh to reload bucket inventory, got %d calls", backend.listProjectBucketInventoryCalls)
	}
}

func TestStorageFolderExactReusesAuditProjectRecordCache(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-present": {ObjectID: "obj-present", DownloadCount: 3},
		},
	}
	service := NewStorageAnalyticsService(backend)

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{ProbeMode: StorageChainProbeModeInventoryOnly}); err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	summary, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
	if err != nil {
		t.Fatalf("build exact storage summary: %v", err)
	}
	if summary.RecordCount != 1 || summary.DownloadCount != 3 {
		t.Fatalf("expected exact summary to join cached record and usage, got %+v", summary)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected audit and exact summary to share one project record enumeration, got %d", backend.listProjectAuditRecordsCalls)
	}
	if backend.getProjectMetricsSummaryCalls != 2 {
		t.Fatalf("expected one cheap metrics validation per request, got %d", backend.getProjectMetricsSummaryCalls)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected exact summary to avoid checksum bulk record lookup, got %d", backend.bulkGetProjectRecordsCalls)
	}
}

func TestStorageFolderExactWarmsAuditProjectRecordCache(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)

	if _, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash); err != nil {
		t.Fatalf("build exact storage summary: %v", err)
	}
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{ProbeMode: StorageChainProbeModeInventoryOnly}); err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected exact summary and audit to share one project record enumeration, got %d", backend.listProjectAuditRecordsCalls)
	}
	if backend.getProjectMetricsSummaryCalls != 2 {
		t.Fatalf("expected one cheap metrics validation per request, got %d", backend.getProjectMetricsSummaryCalls)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected exact summary to avoid checksum bulk record lookup, got %d", backend.bulkGetProjectRecordsCalls)
	}
}

func TestStorageFolderExactAndAuditCoalesceProjectRecordRefresh(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		listProjectAuditRecordsDelay: 50 * time.Millisecond,
		projectMetricsSummary: &gintegrationsyfon.ProjectMetricsSummary{
			RecordCount:             1,
			RecordLatestUpdatedTime: "2026-07-01T00:00:00Z",
		},
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)
	errs := make(chan error, 2)

	go func() {
		_, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
		errs <- err
	}()
	go func() {
		_, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{ProbeMode: StorageChainProbeModeInventoryOnly})
		errs <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("expected concurrent exact summary and audit to succeed, got %v", err)
		}
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected concurrent exact summary and audit to coalesce one project record refresh, got %d", backend.listProjectAuditRecordsCalls)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected exact summary to avoid checksum bulk record lookup, got %d", backend.bulkGetProjectRecordsCalls)
	}
}

func TestBuildStorageChainAuditCachesProjectRecordsPerPathPrefix(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"CONFIG/a.json": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"DATA/b.json":   lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/root/CONFIG/a.json"}},
			{ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/root/DATA/b.json"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket/root"},
		},
	}
	service := NewStorageAnalyticsService(backend)
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "CONFIG", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "DATA", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build second chain audit: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected project-scoped record cache to serve both prefixes, got %d calls", backend.listProjectAuditRecordsCalls)
	}
}

func TestBuildStorageChainAuditCoalescesConcurrentProjectRecordRefresh(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		listProjectAuditRecordsDelay: 50 * time.Millisecond,
		projectMetricsSummary: &gintegrationsyfon.ProjectMetricsSummary{
			RecordCount:             1,
			RecordLatestUpdatedTime: "2026-07-01T00:00:00Z",
		},
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/a.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
	}
	service := NewStorageAnalyticsService(backend)
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("build concurrent chain audit: %v", err)
		}
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected concurrent audits to share one project record refresh, got %d calls", backend.listProjectAuditRecordsCalls)
	}
	if backend.getProjectMetricsSummaryCalls != 2 {
		t.Fatalf("expected each audit to use the cheap metrics validator, got %d calls", backend.getProjectMetricsSummaryCalls)
	}
}

func TestBuildStorageChainAuditRefreshesProjectRecordsWhenMetricsValidatorChanges(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectMetricsSummary: &gintegrationsyfon.ProjectMetricsSummary{
			RecordCount:             1,
			RecordLatestUpdatedTime: "2026-07-01T00:00:00Z",
		},
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/a.txt"}},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
	}
	service := NewStorageAnalyticsService(backend)
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	backend.projectMetricsSummary = &gintegrationsyfon.ProjectMetricsSummary{
		RecordCount:             2,
		RecordLatestUpdatedTime: "2026-07-02T00:00:00Z",
	}
	backend.projectRecords = append(backend.projectRecords, gintegrationsyfon.ProjectRecord{
		ObjectID: "obj-b", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, AccessURLs: []string{"s3://bucket/data/b.txt"},
	})
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build second chain audit: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 2 {
		t.Fatalf("expected changed metrics validator to refresh project records, got %d calls", backend.listProjectAuditRecordsCalls)
	}
}

func TestStorageChildrenSkipsSummaryJoinUsageWork(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-a": {ObjectID: "obj-a", DownloadCount: 1},
		},
		bulkGetDelay: 75 * time.Millisecond,
	}
	service := NewStorageAnalyticsService(backend)
	errCh := make(chan error, 2)
	go func() {
		_, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash)
		errCh <- err
	}()
	go func() {
		_, err := service.BuildStorageChildren(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, 1000, "bytes", "desc", "")
		errCh <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("expected concurrent storage analytics request to succeed, got %v", err)
		}
	}
	if backend.bulkGetProjectRecordsCalls != 1 {
		t.Fatalf("expected only page-scoped children to use checksum lookup, got %d", backend.bulkGetProjectRecordsCalls)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected paginated usage lookup to remain unused, got %d", backend.listProjectFileUsageCalls)
	}
	if backend.listProjectFileUsageByObjectIDsCalls != 1 {
		t.Fatalf("expected only summary to perform bulk usage lookup, got %d", backend.listProjectFileUsageByObjectIDsCalls)
	}
}

func TestListProjectFileUsageByObjectIDsChunksRequests(t *testing.T) {
	objectIDs := make([]string, 0, projectFileUsageBulkChunkSize+1)
	for i := 0; i < projectFileUsageBulkChunkSize+1; i++ {
		objectIDs = append(objectIDs, fmt.Sprintf("obj-%04d", i))
	}
	backend := &fakeStorageAnalyticsBackend{
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-0000": {ObjectID: "obj-0000", DownloadCount: 1},
			"obj-5000": {ObjectID: "obj-5000", DownloadCount: 2},
		},
	}
	service := NewStorageAnalyticsService(backend)

	usage, err := service.listProjectFileUsageByObjectIDs(context.Background(), "Bearer token", "org", "proj", objectIDs, cleanupInactiveDays)
	if err != nil {
		t.Fatalf("list bulk usage: %v", err)
	}
	if backend.listProjectFileUsageByObjectIDsCalls != 2 {
		t.Fatalf("expected two bulk usage chunks, got %d", backend.listProjectFileUsageByObjectIDsCalls)
	}
	if backend.listProjectFileUsageCalls != 0 {
		t.Fatalf("expected no paginated fallback, got %d", backend.listProjectFileUsageCalls)
	}
	if len(backend.listProjectFileUsageObjectIDs) != projectFileUsageBulkChunkSize+1 {
		t.Fatalf("expected all ids to be requested, got %d", len(backend.listProjectFileUsageObjectIDs))
	}
	if usage["obj-0000"].DownloadCount != 1 || usage["obj-5000"].DownloadCount != 2 {
		t.Fatalf("unexpected usage map: %+v", usage)
	}
}

func TestListProjectFileUsageByObjectIDsFallsBackWhenBulkEndpointMissing(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{
		listProjectFileUsageByObjectIDsErr: &gintegrationsyfon.HTTPError{
			Method: "POST",
			Path:   "/index/v1/metrics/files/bulk",
			Status: http.StatusNotFound,
		},
		usageByObject: map[string]gintegrationsyfon.FileUsage{
			"obj-a": {ObjectID: "obj-a", DownloadCount: 9},
		},
	}
	service := NewStorageAnalyticsService(backend)

	usage, err := service.listProjectFileUsageByObjectIDs(context.Background(), "Bearer token", "org", "proj", []string{"obj-a"}, cleanupInactiveDays)
	if err != nil {
		t.Fatalf("list fallback usage: %v", err)
	}
	if backend.listProjectFileUsageByObjectIDsCalls != 1 || backend.listProjectFileUsageCalls != 1 {
		t.Fatalf("expected one bulk attempt and one paginated fallback, got bulk=%d paginated=%d", backend.listProjectFileUsageByObjectIDsCalls, backend.listProjectFileUsageCalls)
	}
	if usage["obj-a"].DownloadCount != 9 {
		t.Fatalf("unexpected fallback usage: %+v", usage)
	}
}

func TestLoadProjectJoinCacheFiltersSharedRecordsByDeduplicatedGitChecksums(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/b.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-orphan", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/orphan"}},
		},
	}
	service := NewStorageAnalyticsService(backend)
	if _, err := service.BuildStorageSummary(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash); err != nil {
		t.Fatalf("build storage summary: %v", err)
	}
	if backend.bulkGetProjectRecordsCalls != 0 {
		t.Fatalf("expected exact summary to avoid checksum lookup, got %d calls", backend.bulkGetProjectRecordsCalls)
	}
	if len(backend.listProjectFileUsageObjectIDs) != 1 || backend.listProjectFileUsageObjectIDs[0] != "obj-a" {
		t.Fatalf("expected duplicate Git checksums to request one matched object usage row, got %+v", backend.listProjectFileUsageObjectIDs)
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
	if finding.Actionability != "manual_choice" ||
		!contains(finding.AvailableActions, "delete_syfon_record") ||
		!contains(finding.AvailableActions, "delete_bucket_object") ||
		!contains(finding.AvailableActions, "delete_both") ||
		finding.DefaultAction != "" ||
		!finding.SupportsDryRun {
		t.Fatalf("expected metadata mismatch chain actions, got %+v", finding)
	}
}

func TestBuildStorageChainAuditFiltersFindingsByKind(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":       lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/bad-map.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
	})
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: "obj-a", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, UpdatedAt: &now, AccessURLs: []string{"s3://bucket/a"}},
			{ObjectID: "obj-bad-map", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Organization: "org", Project: "proj", Size: 200, UpdatedAt: &now, AccessURLs: []string{"s3://legacy-bucket/bad-map.txt"}},
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
			storageProbeRequestKey("s3://legacy-bucket/bad-map.txt", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               storageProbeRequestKey("s3://legacy-bucket/bad-map.txt", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:        "s3://legacy-bucket/bad-map.txt",
				Status:           "error",
				Exists:           false,
				ErrorKind:        "credential_missing",
				Error:            `no stored bucket credential found for bucket "legacy-bucket"`,
				ValidationStatus: "unverifiable",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{
		FindingKind:  "syfon_broken_bucket_mapping",
		FindingLimit: 1,
	})
	if err != nil {
		t.Fatalf("build filtered chain audit: %v", err)
	}
	if chain.Summary.TotalFindings != 1 || chain.Summary.ReturnedFindings != 1 || chain.Summary.FindingsTruncated {
		t.Fatalf("expected one filtered finding without truncation, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 1 {
		t.Fatalf("expected filtered broken mapping count, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["git_syfon_metadata_mismatch"]; got != 0 {
		t.Fatalf("expected mismatch count to be excluded from filtered response, got %+v", chain.Summary)
	}
	if len(chain.Groups) != 1 || chain.Groups[0].Kind != "syfon_broken_bucket_mapping" {
		t.Fatalf("expected one filtered group, got %+v", chain.Groups)
	}
	if len(chain.Findings) != 1 || chain.Findings[0].Kind != "syfon_broken_bucket_mapping" || chain.Findings[0].NormalizedPath != "data/bad-map.txt" {
		t.Fatalf("expected filtered broken mapping detail row, got %+v", chain.Findings)
	}
}

func TestBuildStorageChainAuditDefaultTruncationKeepsRowsPerIssueKind(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/unrelated.txt": lfsPointer("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 1),
	})
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	records := make([]gintegrationsyfon.ProjectRecord, 0, 2)
	probeResults := make(map[string]gintegrationsyfon.BulkStorageProbeResult)
	for i := 0; i < 2; i++ {
		checksum := fmt.Sprintf("%064x", i+1)
		objectID := fmt.Sprintf("obj-broken-%03d", i)
		objectURL := fmt.Sprintf("s3://legacy-bucket/broken-%03d.txt", i)
		records = append(records, gintegrationsyfon.ProjectRecord{
			ObjectID:     objectID,
			Checksum:     checksum,
			Organization: "org",
			Project:      "proj",
			Size:         100 + int64(i),
			UpdatedAt:    &now,
			AccessURLs:   []string{objectURL},
		})
		probeResults[storageProbeRequestKey(objectURL, 100+int64(i), checksum)] = gintegrationsyfon.BulkStorageProbeResult{
			ID:               storageProbeRequestKey(objectURL, 100+int64(i), checksum),
			ObjectURL:        objectURL,
			Status:           "error",
			Exists:           false,
			ErrorKind:        "credential_missing",
			Error:            `no stored bucket credential found for bucket "legacy-bucket"`,
			ValidationStatus: "unverifiable",
		}
	}
	bucketObjects := make([]gintegrationsyfon.ProjectBucketObject, 0, 501)
	for i := 0; i < 501; i++ {
		bucketObjects = append(bucketObjects, gintegrationsyfon.ProjectBucketObject{
			ObjectURL: fmt.Sprintf("s3://bucket/loose-%03d.txt", i),
			Bucket:    "bucket",
			Key:       fmt.Sprintf("loose-%03d.txt", i),
			Path:      fmt.Sprintf("loose-%03d.txt", i),
			SizeBytes: 10,
		})
	}
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: records,
		bucketObjects:  bucketObjects,
		probeResults:   probeResults,
	}
	service := NewStorageAnalyticsService(backend)

	truncated, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		FindingLimit: 500,
	})
	if err != nil {
		t.Fatalf("build truncated chain audit: %v", err)
	}
	if !truncated.Summary.FindingsTruncated || truncated.Summary.ReturnedFindings != 503 {
		t.Fatalf("expected per-kind truncated response, got %+v", truncated.Summary)
	}
	if got := truncated.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 2 {
		t.Fatalf("expected summary to retain broken mapping count, got %+v", truncated.Summary)
	}
	assertHasChainFinding(t, truncated.Findings, "syfon_broken_bucket_mapping", "syfon/obj-broken-000")
	assertHasChainFinding(t, truncated.Findings, "syfon_broken_bucket_mapping", "syfon/obj-broken-001")

	filtered, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		FindingKind:  "syfon_broken_bucket_mapping",
		FindingLimit: -1,
	})
	if err != nil {
		t.Fatalf("build filtered chain audit: %v", err)
	}
	if filtered.Summary.FindingsTruncated || filtered.Summary.ReturnedFindings != 2 {
		t.Fatalf("expected full filtered response, got %+v", filtered.Summary)
	}
	if len(filtered.Findings) != 2 {
		t.Fatalf("expected filtered broken mapping detail rows, got %+v", filtered.Findings)
	}
	assertHasChainFinding(t, filtered.Findings, "syfon_broken_bucket_mapping", "syfon/obj-broken-000")
	assertHasChainFinding(t, filtered.Findings, "syfon_broken_bucket_mapping", "syfon/obj-broken-001")
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
	if len(backend.probeItems) != 1 {
		t.Fatalf("expected only the canonical scoped probe, got %+v", backend.probeItems)
	}
	if containsProbeTarget(backend.probeItems, "s3://bforepc-prod/OHSU/slide.ome.tiff") {
		t.Fatalf("did not expect raw alias URL to be probed once scoped URL resolves: %+v", backend.probeItems)
	}
	if !containsProbeTarget(backend.probeItems, "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff") {
		t.Fatalf("expected probe targets to include canonical scoped URL, got %+v", backend.probeItems)
	}
}

func TestBuildStorageChainAuditAcceptsMappedNameAndSourcePathInventoryObjects(t *testing.T) {
	sourceChecksum := "610cad76a473c4b0bf43f923b3bb907aac9ac2630d9c3f21aba1c531953c43e4"
	mappedChecksum := "b71c3fcf28a6d2cf6e432b21a1734786708517f4808c22f9114abb73a60f4cc7"
	sourcePath := "OHSU/koei_chin/visium_hd/1-R1/outs/binned_outputs/square_002um/spatial/tissue_positions.parquet"
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		sourcePath:             lfsPointer(sourceChecksum, 2048),
		"META/Research.ndjson": lfsPointer(mappedChecksum, 4722),
	})
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-source-path",
				Name:         "tissue_positions.parquet",
				Checksum:     sourceChecksum,
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         2048,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bforepc/" + sourceChecksum},
			},
			{
				ObjectID:     "obj-mapped-name",
				Name:         "ResearchSubject.ndjson",
				Checksum:     mappedChecksum,
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         4722,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bforepc/" + mappedChecksum},
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
				ObjectURL: "s3://bforepc/bforepc-prod/" + sourcePath,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/" + sourcePath,
				Path:      sourcePath,
				SizeBytes: 2048,
			},
			{
				ObjectURL: "s3://bforepc/" + mappedChecksum,
				Bucket:    "bforepc",
				Key:       mappedChecksum,
				Path:      mappedChecksum,
				SizeBytes: 4722,
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 2 {
		t.Fatalf("expected both source-path and mapped-name objects to complete chains, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 0 {
		t.Fatalf("expected no missing bucket findings when either candidate exists, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_missing_bucket_object"]; got != 0 {
		t.Fatalf("expected no Syfon-only missing bucket findings when either candidate exists, got summary %+v", chain.Summary)
	}
}

func TestBuildStorageChainAuditCanonicalizesStaleRecordBucketUsingAuditProjectScopes(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/file.bin": lfsPointer("dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				Organization: "Ellrott_Lab",
				Project:      "hla2vec",
				Size:         100,
				AccessURLs: []string{
					"s3://EllrottLab/calypr/ff4b54c2-fc50-5850-9fc3-9d662e1f9e44/dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				},
			},
		},
		buckets: map[string]domain.StorageBucket{
			"gdc-mirror-bucket": {Bucket: "gdc-mirror-bucket", Provider: "s3"},
		},
		bucketScopes: map[string][]domain.StorageBucketScope{
			"gdc-mirror-bucket": {{
				Bucket:       "gdc-mirror-bucket",
				Organization: "gdc_mirror",
				ProjectID:    "gdc_mirror",
				Path:         "s3://gdc-mirror-bucket/gdc_mirror",
			}},
		},
		projectScopes: []domain.StorageBucketScope{},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL:  "s3://gdc-mirror-bucket/gdc_mirror/calypr/ff4b54c2-fc50-5850-9fc3-9d662e1f9e44/dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				Bucket:     "gdc-mirror-bucket",
				Key:        "gdc_mirror/calypr/ff4b54c2-fc50-5850-9fc3-9d662e1f9e44/dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				Path:       "dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				SizeBytes:  100,
				Provider:   "s3",
				MetaSHA256: "dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"gdc_mirror",
		"gdc_mirror",
		refName,
		"data",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if backend.listBucketScopesCalls == 0 {
		t.Fatalf("expected audit to fall back to bucket scope enumeration")
	}
	if got := chain.Summary.CountsByKind["syfon_missing_bucket_object"]; got != 0 {
		t.Fatalf("expected stale EllrottLab URL to be canonicalized into the audit project bucket, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_syfon_git_complete"]; got != 1 {
		t.Fatalf("expected current project bucket object to complete the chain, got summary %+v", chain.Summary)
	}
	findings := loadAllChainFindings(t, service, "gdc_mirror", "gdc_mirror", chain)
	for _, finding := range findings {
		if strings.Contains(finding.BucketObjectURL, "EllrottLab") || strings.Contains(finding.NormalizedPath, "EllrottLab") {
			t.Fatalf("did not expect stale EllrottLab bucket to appear in findings: %+v", finding)
		}
	}
}

func TestBuildStorageChainAuditErrorsWhenRecordBucketCannotMapToAuditProjectScope(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/file.bin": lfsPointer("dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-a",
				Checksum:     "dc4e842bde2d06c161ad96dfcc58084677e9e376989f5b3567c4217b108a0935",
				Organization: "Ellrott_Lab",
				Project:      "hla2vec",
				Size:         100,
				AccessURLs:   []string{"s3://EllrottLab/calypr/file.bin"},
			},
		},
		projectScopes: []domain.StorageBucketScope{},
	}
	service := NewStorageAnalyticsService(backend)

	_, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"gdc_mirror",
		"gdc_mirror",
		refName,
		"data",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err == nil {
		t.Fatalf("expected unmapped S3 access URL to fail audit")
	}
	if !strings.Contains(err.Error(), `storage access URL "s3://EllrottLab/calypr/file.bin" could not be mapped into bucket scopes for project gdc_mirror/gdc_mirror`) {
		t.Fatalf("expected explicit unmapped URL error, got %v", err)
	}
}

func TestBuildStorageChainAuditErrorsWhenProjectScopeBucketDisagreesWithInventory(t *testing.T) {
	checksum := "ba49c9465b911b950abd60fb333e76032db677b29194ae2268a2a6797402cb4f"
	did := "0005c5a4-d3cb-57fc-ad83-78a52c950851"
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/file.tsv": lfsPointer(checksum, 4247846),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     did,
				Checksum:     checksum,
				Organization: "gdc_mirror",
				Project:      "gdc_mirror",
				Size:         4247846,
				Name:         "186b06a9-0b6c-48fe-83fa-451514703728.rna_seq.augmented_star_gene_counts.tsv",
				AccessURLs:   []string{"s3://gdcdata/" + did + "/" + checksum},
			},
		},
		projectScopes: []domain.StorageBucketScope{
			{
				Bucket:       "EllrottLab",
				Organization: "gdc_mirror",
				ProjectID:    "gdc_mirror",
				Path:         "s3://EllrottLab/calypr",
			},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL: "s3://gdcdata/" + did + "/" + checksum,
				Bucket:    "gdcdata",
				Key:       did + "/" + checksum,
				Path:      "186b06a9-0b6c-48fe-83fa-451514703728.rna_seq.augmented_star_gene_counts.tsv",
				SizeBytes: 4247846,
				Provider:  "s3",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	_, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"gdc_mirror",
		"gdc_mirror",
		refName,
		"data",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err == nil {
		t.Fatalf("expected scope bucket mismatch to fail audit")
	}
	if !strings.Contains(err.Error(), `storage access URL mapped to "s3://EllrottLab/calypr/0005c5a4-d3cb-57fc-ad83-78a52c950851/ba49c9465b911b950abd60fb333e76032db677b29194ae2268a2a6797402cb4f"`) {
		t.Fatalf("expected mapped EllrottLab URL in error, got %v", err)
	}
	if !strings.Contains(err.Error(), `bucket "EllrottLab" is not present in project bucket inventory for project gdc_mirror/gdc_mirror`) {
		t.Fatalf("expected inventory bucket mismatch error, got %v", err)
	}
}

func TestCanonicalizeScopedStorageURLMatchesSyfonDownloadScopeResolution(t *testing.T) {
	tests := []struct {
		name      string
		accessURL string
		scopes    []domain.StorageBucketScope
		org       string
		project   string
		want      string
	}{
		{
			name:      "project scope rewrites alias bucket to physical bucket",
			accessURL: "s3://bforepc-prod/OHSU/koei_chin/visium_hd/barcodes.tsv.gz",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			}},
			org:     "HTAN_INT",
			project: "BForePC",
			want:    "s3://bforepc/bforepc-prod/OHSU/koei_chin/visium_hd/barcodes.tsv.gz",
		},
		{
			name:      "root scope does not require slash in path",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://physical",
			}},
			org:     "org",
			project: "proj",
			want:    "s3://physical/path/file.txt",
		},
		{
			name:      "explicit bucket with prefix path",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "root/prefix",
			}},
			org:     "org",
			project: "proj",
			want:    "s3://physical/root/prefix/path/file.txt",
		},
		{
			name:      "already scoped url is not double prefixed",
			accessURL: "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			}},
			org:     "HTAN_INT",
			project: "BForePC",
			want:    "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff",
		},
		{
			name:      "unrelated scope is ignored",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "other",
				ProjectID:    "proj",
				Path:         "s3://physical/root",
			}},
			org:     "org",
			project: "proj",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canonicalizeScopedStorageURL(tt.accessURL, tt.scopes, tt.org, tt.project)
			if got != tt.want {
				t.Fatalf("unexpected scoped URL: got %q want %q", got, tt.want)
			}
		})
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

func TestClassifyStorageFindingSuppressesRawURLFailuresWhenScopedProbeMatches(t *testing.T) {
	record := projectRecordState{
		ProjectRecord: gintegrationsyfon.ProjectRecord{
			ObjectID:   "obj-a",
			Checksum:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Size:       100,
			AccessURLs: []string{"s3://bforepc-prod/JHU/slide.ome.tiff"},
		},
		CanonicalAccessURLs: []string{"s3://bforepc/bforepc-prod/JHU/slide.ome.tiff"},
		CanonicalAccessURLByRaw: map[string]string{
			"s3://bforepc-prod/JHU/slide.ome.tiff": "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
		},
		AccessProbes: []gintegrationsyfon.BulkStorageProbeResult{
			{
				ObjectURL:        "s3://bforepc-prod/JHU/slide.ome.tiff",
				Status:           "error",
				Exists:           false,
				ErrorKind:        "credential_missing",
				ValidationStatus: "unverifiable",
			},
			{
				ObjectURL:        "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
				Bucket:           "bforepc",
				Key:              "bforepc-prod/JHU/slide.ome.tiff",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}

	if got := classifyStorageFinding(record, nil); got != storageFindingNone {
		t.Fatalf("expected scoped matched probe to suppress raw URL credential miss, got %s", got)
	}
}

func TestClassifyStorageFindingSuppressesRawURLMismatchWhenScopedProbeMatches(t *testing.T) {
	record := projectRecordState{
		ProjectRecord: gintegrationsyfon.ProjectRecord{
			ObjectID:   "obj-a",
			Checksum:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Size:       100,
			AccessURLs: []string{"s3://bforepc-prod/JHU/slide.ome.tiff"},
		},
		CanonicalAccessURLs: []string{"s3://bforepc/bforepc-prod/JHU/slide.ome.tiff"},
		CanonicalAccessURLByRaw: map[string]string{
			"s3://bforepc-prod/JHU/slide.ome.tiff": "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
		},
		AccessProbes: []gintegrationsyfon.BulkStorageProbeResult{
			{
				ObjectURL:            "s3://bforepc-prod/JHU/slide.ome.tiff",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"size_mismatch"},
			},
			{
				ObjectURL:        "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
				Bucket:           "bforepc",
				Key:              "bforepc-prod/JHU/slide.ome.tiff",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}

	if got := classifyStorageFinding(record, nil); got != storageFindingNone {
		t.Fatalf("expected scoped matched probe to suppress raw URL mismatch, got %s", got)
	}
}

func TestClassifyStorageFindingSurfacesRepairableBrokenAccessMethod(t *testing.T) {
	record := projectRecordState{
		ProjectRecord: gintegrationsyfon.ProjectRecord{
			ObjectID: "obj-a",
			Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Size:     100,
			AccessURLs: []string{
				"s3://retired-bucket/JHU/slide.ome.tiff",
				"s3://bforepc-prod/JHU/slide.ome.tiff",
			},
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
				{AccessID: "s3", Type: "s3", URL: "s3://retired-bucket/JHU/slide.ome.tiff"},
				{AccessID: "s3", Type: "s3", URL: "s3://bforepc-prod/JHU/slide.ome.tiff"},
			},
		},
		CanonicalAccessURLs: []string{"s3://retired-bucket/JHU/slide.ome.tiff", "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff"},
		CanonicalAccessURLByRaw: map[string]string{
			"s3://retired-bucket/JHU/slide.ome.tiff": "s3://retired-bucket/JHU/slide.ome.tiff",
			"s3://bforepc-prod/JHU/slide.ome.tiff":   "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
		},
		AccessProbes: []gintegrationsyfon.BulkStorageProbeResult{
			{
				ObjectURL:        "s3://retired-bucket/JHU/slide.ome.tiff",
				Status:           "error",
				Exists:           false,
				ErrorKind:        "credential_missing",
				ValidationStatus: "unverifiable",
			},
			{
				ObjectURL:        "s3://bforepc/bforepc-prod/JHU/slide.ome.tiff",
				Bucket:           "bforepc",
				Key:              "bforepc-prod/JHU/slide.ome.tiff",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}

	if got := classifyStorageFinding(record, nil); got != storageFindingBrokenBucketMap {
		t.Fatalf("expected repairable broken access method finding, got %s", got)
	}
	broken := repairableBrokenAccessRecord(record)
	if len(broken.AccessProbes) != 1 || broken.AccessProbes[0].ObjectURL != "s3://retired-bucket/JHU/slide.ome.tiff" {
		t.Fatalf("expected only broken access probe in repair record, got %+v", broken.AccessProbes)
	}
	remaining, shouldDelete, ok := repairBrokenBucketMappingRecord(broken)
	if !ok || shouldDelete {
		t.Fatalf("expected repair update, got ok=%v shouldDelete=%v remaining=%+v", ok, shouldDelete, remaining)
	}
	if len(remaining) != 1 || remaining[0].URL != "s3://bforepc-prod/JHU/slide.ome.tiff" {
		t.Fatalf("expected working access method to remain, got %+v", remaining)
	}
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
		listProjectBucketObjectsErr: fmt.Errorf("list syfon project bucket inventory: syfon POST /data/inspect/project-bucket/inventory failed with status 409: provider rejected bucket inventory request for s3://bucket/prefix; mapped bucket target may be missing or inaccessible"),
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

	_, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate})
	if err == nil {
		t.Fatal("expected bucket inventory failure to hard-error chain audit")
	}
	if !strings.Contains(err.Error(), "mapped bucket target may be missing or inaccessible") {
		t.Fatalf("expected bucket inventory error detail, got %v", err)
	}
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected one bucket validation inventory attempt, got %d", backend.listProjectBucketInventoryCalls)
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

func TestApplyStorageCleanupDeletesSelectedBucketSyfonNoGitChainFinding(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-no-git",
				Checksum:     "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				Organization: "org",
				Project:      "proj",
				Size:         150,
				UpdatedAt:    &now,
				AccessURLs:   []string{"s3://bucket/no-git"},
			},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL:  "s3://bucket/no-git",
				Bucket:     "bucket",
				Key:        "no-git",
				Path:       "no-git",
				SizeBytes:  150,
				MetaSHA256: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	response, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"s3://bucket/no-git"},
		nil,
		[]GitStorageCleanupApplyFinding{{
			Kind:             "bucket_syfon_no_git",
			NormalizedPath:   "s3://bucket/no-git",
			ObjectIDs:        []string{"obj-no-git"},
			BucketObjectURL:  "s3://bucket/no-git",
			BucketObjectURLs: []string{"s3://bucket/no-git"},
			AccessURLs:       []string{"s3://bucket/no-git"},
		}},
		true,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if !contains(response.DeletedRecordIDs, "obj-no-git") {
		t.Fatalf("expected selected Syfon record to be deleted, got %+v", response)
	}
	if !contains(response.DeletedBucketObjectURLs, "s3://bucket/no-git") {
		t.Fatalf("expected selected bucket object to be deleted, got %+v", response)
	}
	if !contains(backend.deletedIDs, "obj-no-git") {
		t.Fatalf("expected backend Syfon delete, got %+v", backend.deletedIDs)
	}
	if !contains(backend.deletedBucketObjects, "s3://bucket/no-git") {
		t.Fatalf("expected backend bucket delete, got %+v", backend.deletedBucketObjects)
	}
}

func TestBuildStorageChainAuditMarksBucketSyfonNoGitActionable(t *testing.T) {
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupport("bucket_syfon_no_git")
	if actionability != storageActionabilityAutoRepair {
		t.Fatalf("expected auto-repair actionability, got %q", actionability)
	}
	if defaultAction != storageActionDeleteBoth {
		t.Fatalf("expected delete-both default action, got %q", defaultAction)
	}
	if !supportsDryRun {
		t.Fatal("expected bucket+Syfon/no-Git to support dry-run")
	}
	if !contains(availableActions, storageActionDeleteBoth) || !contains(availableActions, storageActionDeleteSyfonRecord) || !contains(availableActions, storageActionDeleteBucketObject) {
		t.Fatalf("expected destructive chain actions, got %+v", availableActions)
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

func BenchmarkBuildStorageFolderDefaultModeLargeDirectoryPage(b *testing.B) {
	children := make([]GitRepoAnalyticsChild, 1000)
	for index := 0; index < 1000; index++ {
		children[index] = GitRepoAnalyticsChild{
			Name:       fmt.Sprintf("file-%04d.txt", index),
			Path:       fmt.Sprintf("data/file-%04d.txt", index),
			Type:       "file",
			FileCount:  1,
			TotalBytes: int64(index + 1),
		}
	}
	hash := plumbing.NewHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	mirrorPath := filepath.Join(b.TempDir(), "mirror.git")
	index := repoAnalyticsIndexFromSidecar(GitRepoAnalyticsIndexSidecar{
		SchemaVersion: repoAnalyticsIndexSchemaVersion,
		CommitHash:    hash.String(),
		RefName:       "main",
		GeneratedAt:   time.Now().UTC(),
		Directories: []GitRepoAnalyticsDirectory{
			{
				Path:             "data",
				DirectChildCount: len(children),
				FileCount:        len(children),
				TotalBytes:       500500,
				Children:         children,
			},
		},
	})
	repoAnalyticsIndexCache.put(mirrorPath, hash, index)
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	if _, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", "main", "data", mirrorPath, nil, hash, 100, "bytes", "desc", "", "", false, nil); err != nil {
		b.Fatalf("warm storage folder index: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := service.BuildStorageFolder(context.Background(), "Bearer token", "org", "proj", "main", "data", mirrorPath, nil, hash, 100, "bytes", "desc", "", "", false, nil); err != nil {
			b.Fatalf("build storage folder: %v", err)
		}
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

func applyFinding(kind string, normalizedPath string, objectIDs []string, bucketURLs []string) GitStorageCleanupApplyFinding {
	policy := storageRepairPolicyForKind(kind)
	return GitStorageCleanupApplyFinding{
		Kind:             kind,
		NormalizedPath:   normalizedPath,
		ObjectIDs:        append([]string(nil), objectIDs...),
		BucketObjectURL:  firstString(bucketURLs),
		BucketObjectURLs: append([]string(nil), bucketURLs...),
		AccessURLs:       append([]string(nil), bucketURLs...),
		AvailableActions: append([]string(nil), policy.actions...),
		DefaultAction:    policy.defaultAction,
		Evidence: &GitAuditEvidence{
			ObjectIDs:        append([]string(nil), objectIDs...),
			AccessURLs:       append([]string(nil), bucketURLs...),
			BucketObjectURLs: append([]string(nil), bucketURLs...),
		},
	}
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func assertStringSet(t *testing.T, label string, got []string, want []string) {
	t.Helper()
	got = uniqueStrings(got)
	want = uniqueStrings(want)
	if len(got) != len(want) {
		t.Fatalf("%s: expected %v, got %v", label, want, got)
	}
	for _, value := range want {
		if !contains(got, value) {
			t.Fatalf("%s: expected %v, got %v", label, want, got)
		}
	}
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
