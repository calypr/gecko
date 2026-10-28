package storageaudit

import (
	"context"
	"strings"
	"testing"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/browse"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/go-git/go-git/v5/plumbing"
)

func TestBuildGitRepoInventoryAndStorageAnalytics(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"data/c.txt":        lfsPointer("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 300),
		"data/e.txt":        lfsPointer("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 50),
		"data/nested/b.txt": lfsPointer("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 200),
		"plain.txt":         "not lfs\n",
	})
	index, err := repoindex.BuildRepoAnalyticsIndex(refName, repo, hash)
	if err != nil {
		t.Fatalf("build repo analytics index: %v", err)
	}
	inventory, err := repoindex.FilterRepoInventoryFiles(index, "data")
	if err != nil {
		t.Fatalf("filter repo inventory: %v", err)
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
		[]CleanupApplyFinding{
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
	repoindex.Cache.Mu.Lock()
	repoindex.Cache.Entries = map[string]*repoindex.Index{}
	repoindex.Cache.Inflight = map[string]*repoindex.Inflight{}
	repoindex.Cache.Mu.Unlock()

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

func storageChildResponsePaths(items []StorageChildResponseItem) []string {
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
		"commit":     browse.BuildStorageChildrenCursor(wrongHash, "data", "bytes", "desc", 1),
		"path":       browse.BuildStorageChildrenCursor(hash, "other", "bytes", "desc", 1),
		"sort_by":    browse.BuildStorageChildrenCursor(hash, "data", "name", "desc", 1),
		"sort_order": browse.BuildStorageChildrenCursor(hash, "data", "bytes", "asc", 1),
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
