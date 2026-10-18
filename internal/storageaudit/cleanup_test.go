package storageaudit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	"github.com/calypr/gecko/internal/storageaudit/cleanup"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

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
		[]CleanupApplyFinding{{
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
		[]CleanupApplyFinding{{
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
			sarecord.StorageProbeRequestKey("s3://legacy/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:        sarecord.StorageProbeRequestKey("s3://legacy/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL: "s3://legacy/a",
				Status:    "error",
				ErrorKind: "credential_missing",
			},
			sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			sarecord.StorageProbeRequestKey("s3://legacy/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:        sarecord.StorageProbeRequestKey("s3://legacy/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL: "s3://legacy/b",
				Status:    "error",
				ErrorKind: "credential_missing",
			},
		},
	}
	markBrokenAccessURLs(backend, "s3://legacy/a", "s3://legacy/b")
	service := NewStorageAnalyticsService(backend)

	applyResult, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"data/a.txt", "data/b.txt"},
		nil,
		[]CleanupApplyFinding{
			{
				Kind:           "broken_bucket_mapping",
				NormalizedPath: "data/a.txt",
				ObjectIDs:      []string{"obj-a"},
				Records: []CleanupRecordAudit{{
					ObjectID:      "obj-a",
					AccessURLs:    []string{"s3://legacy/a", "s3://bucket/a"},
					AccessMethods: []CleanupAccessMethod{{URL: "s3://legacy/a"}, {URL: "s3://bucket/a"}},
					AccessProbes: []CleanupAccessProbe{
						{URL: "s3://legacy/a", Status: "error", ErrorKind: "credential_missing"},
						{URL: "s3://bucket/a", Status: "present"},
					},
				}},
			},
			{
				Kind:           "broken_bucket_mapping",
				NormalizedPath: "data/b.txt",
				ObjectIDs:      []string{"obj-b"},
				Records: []CleanupRecordAudit{{
					ObjectID:      "obj-b",
					AccessURLs:    []string{"s3://legacy/b"},
					AccessMethods: []CleanupAccessMethod{{URL: "s3://legacy/b"}},
					AccessProbes:  []CleanupAccessProbe{{URL: "s3://legacy/b", Status: "error", ErrorKind: "credential_missing"}},
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		[]CleanupApplyFinding{{
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
			sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
			sarecord.StorageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:                   sarecord.StorageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
	var mismatchFinding CleanupFinding
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
			sarecord.StorageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:                   sarecord.StorageProbeRequestKey("s3://bucket/b", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
	applyFindings := []CleanupApplyFinding{{
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
		[]CleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_syfon_record"}},
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
		[]CleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_bucket_object"}},
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
		[]CleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/b.txt", Action: "delete_both"}},
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

func TestApplyStorageCleanupRepairMatrix(t *testing.T) {
	tests := []struct {
		name                string
		finding             CleanupApplyFinding
		actions             []CleanupApplyAction
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
		{name: "storage missing", finding: applyFinding("storage_object_missing", "data/missing.txt", []string{"obj-missing"}, []string{"s3://bucket/missing"}), wantDeletedIDs: []string{"obj-missing"}},
		{name: "syfon git no bucket", finding: applyFinding("syfon_git_no_bucket", "data/missing.txt", []string{"obj-missing"}, []string{"s3://bucket/missing"}), wantDeletedIDs: []string{"obj-missing"}},
		{name: "syfon missing bucket object", finding: applyFinding("syfon_missing_bucket_object", "s3://bucket/missing", []string{"obj-missing"}, []string{"s3://bucket/missing"}), wantDeletedIDs: []string{"obj-missing"}},
		{name: "git syfon mismatch requires manual review", finding: applyFinding("git_syfon_metadata_mismatch", "data/mismatch.txt", []string{"obj-mm"}, []string{"s3://bucket/mismatch"}), wantManualPath: "data/mismatch.txt"},
		{name: "storage mismatch delete both", finding: applyFinding("storage_validation_mismatch", "data/mismatch.txt", []string{"obj-mm"}, []string{"s3://bucket/mismatch"}), actions: []CleanupApplyAction{{Kind: "storage_validation_mismatch", NormalizedPath: "data/mismatch.txt", Action: sarecord.ActionDeleteBoth}}, wantDeletedIDs: []string{"obj-mm"}, wantBucketURLs: []string{"s3://bucket/mismatch"}},
		{name: "live duplicate conflict", finding: applyFinding("live_duplicate_conflict", "data/dup.txt", []string{"obj-a", "obj-b"}, nil), wantManualPath: "data/dup.txt"},
		{name: "git only no syfon", finding: applyFinding("git_only_no_syfon", "data/git-only.txt", nil, nil), wantManualPath: "data/git-only.txt"},
		{name: "probe error", finding: applyFinding("probe_error", "data/probe.txt", []string{"obj-probe"}, nil), wantManualPath: "data/probe.txt"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeStorageAnalyticsBackend{probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{}}
			action := test.finding.DefaultAction
			if len(test.actions) > 0 {
				action = test.actions[0].Action
			}
			if cleanup.VerificationExpectation(test.finding.Kind, action) == cleanup.ExpectationMissing {
				backend.probeResults["cleanup-revalidate-0"] = gintegrationsyfon.BulkStorageProbeResult{
					ID:                 "cleanup-revalidate-0",
					Status:             "not_found",
					ErrorKind:          "object_not_found",
					VerificationMethod: "ranged_get",
				}
			}
			service := NewStorageAnalyticsService(backend)
			result, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", nil, test.actions, []CleanupApplyFinding{test.finding}, false, false, false, false, false)
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

func TestApplyStorageCleanupBucketOnlyDeletionEvictsChainInputCache(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
	}
	service := NewStorageAnalyticsService(backend)
	cacheKey := service.chainSvc.ProjectChainInputCacheKey("org", "proj")
	service.chainSvc.ChainInputCache[cacheKey] = chain.ChainInputCacheEntry{
		ExpiresAt:     time.Now().Add(time.Minute),
		ProjectScopes: backend.projectScopes,
	}
	service.chainSvc.ChainInputCache[cacheKey+"::bucket-validation-inventory::"] = chain.ChainInputCacheEntry{
		ExpiresAt: time.Now().Add(time.Minute),
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: "s3://bucket/only",
			Bucket:    "bucket",
			Key:       "only",
		}},
		BucketObjectsByURL: map[string]gintegrationsyfon.ProjectBucketObject{
			"s3://bucket/only": {ObjectURL: "s3://bucket/only", Bucket: "bucket", Key: "only"},
		},
	}
	service.chainSvc.ChainInputCache["org/other::bucket-validation-inventory::"] = chain.ChainInputCacheEntry{
		ExpiresAt: time.Now().Add(time.Minute),
	}

	_, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		nil,
		nil,
		[]CleanupApplyFinding{applyFinding("bucket_only_object", "s3://bucket/only", nil, []string{"s3://bucket/only"})},
		false,
		false,
		true,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply bucket-only cleanup: %v", err)
	}
	if _, ok := service.chainSvc.ChainInputCache[cacheKey]; ok {
		t.Fatalf("expected project chain input cache to be evicted, got %+v", service.chainSvc.ChainInputCache)
	}
	if _, ok := service.chainSvc.ChainInputCache[cacheKey+"::bucket-validation-inventory::"]; ok {
		t.Fatalf("expected project bucket inventory cache to be evicted, got %+v", service.chainSvc.ChainInputCache)
	}
	if _, ok := service.chainSvc.ChainInputCache["org/other::bucket-validation-inventory::"]; !ok {
		t.Fatalf("expected another project's cache entry to remain, got %+v", service.chainSvc.ChainInputCache)
	}
}

func TestApplyStorageCleanupPrunesBrokenAccessMethods(t *testing.T) {
	tests := []struct {
		name           string
		record         CleanupRecordAudit
		wantDeleted    []string
		wantRemaining  []string
		wantErrSnippet string
	}{
		{
			name: "keeps good access method",
			record: CleanupRecordAudit{
				ObjectID:      "obj-a",
				AccessURLs:    []string{"s3://legacy/a", "s3://bucket/a"},
				AccessMethods: []CleanupAccessMethod{{URL: "s3://legacy/a"}, {URL: "s3://bucket/a"}},
				AccessProbes: []CleanupAccessProbe{
					{URL: "s3://legacy/a", Status: "error", ErrorKind: "credential_missing"},
					{URL: "s3://bucket/a", Status: "present"},
				},
			},
			wantRemaining: []string{"s3://bucket/a"},
		},
		{
			name: "deletes record when only access method is broken",
			record: CleanupRecordAudit{
				ObjectID:      "obj-b",
				AccessURLs:    []string{"s3://legacy/b"},
				AccessMethods: []CleanupAccessMethod{{URL: "s3://legacy/b"}},
				AccessProbes:  []CleanupAccessProbe{{URL: "s3://legacy/b", Status: "error", ErrorKind: "credential_missing"}},
			},
			wantDeleted: []string{"obj-b"},
		},
		{
			name: "missing evidence fails",
			record: CleanupRecordAudit{
				ObjectID:      "obj-c",
				AccessURLs:    []string{"s3://legacy/c"},
				AccessMethods: []CleanupAccessMethod{{URL: "s3://legacy/c"}},
			},
			wantErrSnippet: "missing broken access URL evidence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeStorageAnalyticsBackend{}
			if test.wantErrSnippet == "" {
				current := gintegrationsyfon.ProjectRecord{ObjectID: test.record.ObjectID, Checksum: test.record.Checksum}
				for _, method := range test.record.AccessMethods {
					current.AccessMethods = append(current.AccessMethods, gintegrationsyfon.ProjectAccessMethod{
						AccessID: method.AccessID,
						Type:     method.Type,
						URL:      method.URL,
						Headers:  append([]string(nil), method.Headers...),
					})
				}
				backend.projectRecords = []gintegrationsyfon.ProjectRecord{current}
				for _, probe := range test.record.AccessProbes {
					if probe.Status != "present" {
						markBrokenAccessURLs(backend, probe.URL)
					}
				}
			}
			service := NewStorageAnalyticsService(backend)
			_, err := service.ApplyStorageCleanup(
				context.Background(),
				"Bearer token",
				"org",
				"proj",
				nil,
				nil,
				[]CleanupApplyFinding{{
					Kind:           "broken_access_url_error",
					NormalizedPath: "data/a.txt",
					ObjectIDs:      []string{test.record.ObjectID},
					Records:        []CleanupRecordAudit{test.record},
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

func TestApplyStorageCleanupReconcilesBrokenAccessURLsToFreshPhysicalRows(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const canonicalID = "104aeb2b-cc45-5952-9ef7-123b1dcea9e6"
	const siblingID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const brokenURL = "s3://legacy/broken"
	const goodURL = "s3://bucket/good"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     canonicalID,
				Checksum:     checksum,
				Organization: "org",
				Project:      "proj",
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{
					AccessID: "good-canonical",
					Type:     "s3",
					URL:      goodURL,
					Headers:  []string{"x-canonical: true"},
				}},
			},
			{
				ObjectID:     siblingID,
				Checksum:     checksum,
				Organization: "org",
				Project:      "proj",
				AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
					{AccessID: "broken-sibling", Type: "s3", URL: brokenURL, Headers: []string{"x-broken: true"}},
					{AccessID: "good-sibling", Type: "s3", URL: goodURL, Headers: []string{"x-good: true"}},
				},
			},
		},
	}
	markBrokenAccessURLs(backend, brokenURL)
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	finding := CleanupApplyFinding{
		Kind:             "broken_access_url_error",
		NormalizedPath:   "data/file.bin",
		ObjectIDs:        []string{canonicalID},
		AccessURLs:       []string{brokenURL, goodURL},
		AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
		DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
		Records: []CleanupRecordAudit{{
			ObjectID:   canonicalID,
			Checksum:   checksum,
			AccessURLs: []string{brokenURL, goodURL},
			AccessMethods: []CleanupAccessMethod{
				{AccessID: "broken-canonical", Type: "s3", URL: brokenURL},
				{AccessID: "good-canonical", Type: "s3", URL: goodURL},
			},
			AccessProbes: []CleanupAccessProbe{
				{URL: brokenURL, Status: "error", ErrorKind: "credential_missing", Exists: &brokenExists},
				{URL: goodURL, Status: "present"},
			},
		}},
	}

	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{finding}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if backend.listProjectAuditRecordsCalls != 1 {
		t.Fatalf("expected apply to reload current physical records once, got %d calls", backend.listProjectAuditRecordsCalls)
	}
	if _, ok := backend.updatedAccessMethods[canonicalID]; ok {
		t.Fatalf("stale canonical row %q must not be updated: %+v", canonicalID, backend.updatedAccessMethods)
	}
	methods, ok := backend.updatedAccessMethods[siblingID]
	if !ok {
		t.Fatalf("expected broken physical sibling %q to be updated, got %+v", siblingID, backend.updatedAccessMethods)
	}
	if len(methods) != 1 || methods[0].URL != goodURL || methods[0].AccessID != "good-sibling" || len(methods[0].Headers) != 1 || methods[0].Headers[0] != "x-good: true" {
		t.Fatalf("expected fresh sibling metadata with broken URL removed, got %+v", methods)
	}
	if len(result.UpdatedRecordIDs) != 1 || result.UpdatedRecordIDs[0] != siblingID {
		t.Fatalf("expected only sibling update in result, got %+v", result.UpdatedRecordIDs)
	}
}

func TestApplyStorageCleanupUnionsBrokenAccessURLsAcrossSelectedFindings(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const canonicalID = "104aeb2b-cc45-5952-9ef7-123b1dcea9e6"
	const siblingID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const brokenURLA = "s3://legacy/broken-a"
	const brokenURLB = "s3://legacy/broken-b"
	const goodURL = "s3://bucket/good"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: canonicalID, Checksum: checksum, AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{AccessID: "canonical-good", URL: goodURL}}},
			{ObjectID: siblingID, Checksum: checksum, AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
				{AccessID: "sibling-a", URL: brokenURLA},
				{AccessID: "sibling-b", URL: brokenURLB},
				{AccessID: "sibling-good", URL: goodURL},
			}},
		},
	}
	markBrokenAccessURLs(backend, brokenURLA, brokenURLB)
	service := NewStorageAnalyticsService(backend)
	findings := []CleanupApplyFinding{
		brokenAccessRepairFindingForTest(canonicalID, checksum, brokenURLA, goodURL),
		brokenAccessRepairFindingForTest(canonicalID, checksum, brokenURLB, goodURL),
	}
	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil, findings,
		false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	methods := backend.updatedAccessMethods[siblingID]
	if len(methods) != 1 || methods[0].URL != goodURL || methods[0].AccessID != "sibling-good" {
		t.Fatalf("expected both broken methods removed from the sibling, got %+v", methods)
	}
	if _, ok := backend.updatedAccessMethods[canonicalID]; ok {
		t.Fatalf("stale canonical row %q must not be updated: %+v", canonicalID, backend.updatedAccessMethods)
	}
	if len(result.UpdatedRecordIDs) != 1 || result.UpdatedRecordIDs[0] != siblingID {
		t.Fatalf("expected one physical sibling update, got %+v", result.UpdatedRecordIDs)
	}
	if len(result.AccessMethodChanges) != 1 || len(result.AccessMethodChanges[0].Removed) != 2 {
		t.Fatalf("expected one consolidated preview for the physical sibling, got %+v", result.AccessMethodChanges)
	}
}

func TestApplyStorageCleanupMatchesCanonicalizedLegacyAccessURLToFreshRawMethod(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const canonicalID = "104aeb2b-cc45-5952-9ef7-123b1dcea9e6"
	const siblingID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const brokenRawURL = "s3://bforepc-prod/OHSU/file.bin"
	const brokenCanonicalURL = "s3://bforepc/bforepc-prod/OHSU/file.bin"
	const goodURL = "s3://bforepc/bforepc-prod/good.bin"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{ObjectID: canonicalID, Checksum: checksum, AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{AccessID: "canonical-good", URL: goodURL}}},
			{ObjectID: siblingID, Checksum: checksum, AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
				{AccessID: "sibling-broken", URL: brokenRawURL},
				{AccessID: "sibling-good", URL: goodURL},
			}},
		},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "org",
			ProjectID:    "proj",
			Path:         "s3://bforepc/bforepc-prod",
		}},
	}
	markBrokenAccessURLs(backend, brokenRawURL)
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	finding := CleanupApplyFinding{
		Kind:             "broken_access_url_error",
		NormalizedPath:   "data/file.bin",
		ObjectIDs:        []string{canonicalID},
		AccessURLs:       []string{brokenRawURL, goodURL},
		AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
		DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
		Records: []CleanupRecordAudit{{
			ObjectID:   canonicalID,
			Checksum:   checksum,
			AccessURLs: []string{brokenCanonicalURL, goodURL},
			AccessMethods: []CleanupAccessMethod{
				{AccessID: "stale-broken", URL: brokenCanonicalURL},
				{AccessID: "stale-good", URL: goodURL},
			},
			AccessProbes: []CleanupAccessProbe{
				{URL: brokenCanonicalURL, Status: "error", ErrorKind: "credential_missing", Exists: &brokenExists},
				{URL: goodURL, Status: "present"},
			},
		}},
	}

	_, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{finding}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if backend.listProjectScopesCalls != 1 {
		t.Fatalf("expected canonicalization to load project scopes once, got %d calls", backend.listProjectScopesCalls)
	}
	methods := backend.updatedAccessMethods[siblingID]
	if len(methods) != 1 || methods[0].URL != goodURL || methods[0].AccessID != "sibling-good" {
		t.Fatalf("expected raw legacy method removed while fresh metadata remains, got %+v", methods)
	}
	if _, ok := backend.updatedAccessMethods[canonicalID]; ok {
		t.Fatalf("stale canonical row %q must not be updated: %+v", canonicalID, backend.updatedAccessMethods)
	}
}

func TestApplyStorageCleanupDoesNotMapUnrelatedBucketAccessURL(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const objectID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const unrelatedURL = "s3://other-bucket/OHSU/file.bin"
	const physicalURL = "s3://bforepc/bforepc-prod/OHSU/file.bin"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: objectID,
			Checksum: checksum,
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{
				AccessID: "valid-physical",
				URL:      physicalURL,
			}},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "org",
			ProjectID:    "proj",
			Path:         "s3://bforepc/bforepc-prod",
		}},
	}
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{{
			Kind:             "broken_access_url_error",
			NormalizedPath:   "data/file.bin",
			ObjectIDs:        []string{objectID},
			AccessURLs:       []string{unrelatedURL},
			AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
			DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
			Records: []CleanupRecordAudit{{
				ObjectID:   objectID,
				Checksum:   checksum,
				AccessURLs: []string{unrelatedURL},
				AccessMethods: []CleanupAccessMethod{{
					AccessID: "stale-unrelated",
					URL:      unrelatedURL,
				}},
				AccessProbes: []CleanupAccessProbe{{
					URL:    unrelatedURL,
					Status: "error",
					Exists: &brokenExists,
				}},
			}},
		}}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if _, ok := backend.updatedAccessMethods[objectID]; ok {
		t.Fatalf("valid physical method must not be removed for unrelated bucket evidence: %+v", backend.updatedAccessMethods)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("valid physical record must not be deleted for unrelated bucket evidence: %v", backend.deletedIDs)
	}
	if len(result.SkippedPaths) != 1 || result.SkippedPaths[0] != "data/file.bin" {
		t.Fatalf("expected idempotent skip for unrelated bucket evidence, got %+v", result.SkippedPaths)
	}
}

func TestApplyStorageCleanupDoesNotPrefixPhysicalBucketAccessURL(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const objectID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const unprefixedURL = "s3://bforepc/OHSU/file.bin"
	const prefixedURL = "s3://bforepc/bforepc-prod/OHSU/file.bin"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: objectID,
			Checksum: checksum,
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{
				AccessID: "valid-prefixed-physical",
				URL:      prefixedURL,
			}},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "org",
			ProjectID:    "proj",
			Path:         "s3://bforepc/bforepc-prod",
		}},
	}
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{{
			Kind:             "broken_access_url_error",
			NormalizedPath:   "data/file.bin",
			ObjectIDs:        []string{objectID},
			AccessURLs:       []string{unprefixedURL},
			AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
			DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
			Records: []CleanupRecordAudit{{
				ObjectID:   objectID,
				Checksum:   checksum,
				AccessURLs: []string{unprefixedURL},
				AccessMethods: []CleanupAccessMethod{{
					AccessID: "stale-unprefixed-physical",
					URL:      unprefixedURL,
				}},
				AccessProbes: []CleanupAccessProbe{{
					URL:    unprefixedURL,
					Status: "error",
					Exists: &brokenExists,
				}},
			}},
		}}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if _, ok := backend.updatedAccessMethods[objectID]; ok {
		t.Fatalf("prefixed physical method must not be removed for unprefixed physical evidence: %+v", backend.updatedAccessMethods)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("valid prefixed physical record must not be deleted: %v", backend.deletedIDs)
	}
	if len(result.SkippedPaths) != 1 || result.SkippedPaths[0] != "data/file.bin" {
		t.Fatalf("expected idempotent skip for unprefixed physical evidence, got %+v", result.SkippedPaths)
	}
}

func TestApplyStorageCleanupPhysicalBucketWinsAcrossScopes(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const objectID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const legacyURL = "s3://legacy/OHSU/file.bin"
	const incorrectlyMappedURL = "s3://bforepc/legacy/OHSU/file.bin"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: objectID,
			Checksum: checksum,
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{
				AccessID: "valid-other-scope-physical",
				URL:      incorrectlyMappedURL,
			}},
		}},
		projectScopes: []domain.StorageBucketScope{
			{
				Bucket:       "bforepc",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bforepc/legacy",
			},
			{
				Bucket:       "legacy",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://legacy",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{{
			Kind:             "broken_access_url_error",
			NormalizedPath:   "data/file.bin",
			ObjectIDs:        []string{objectID},
			AccessURLs:       []string{legacyURL},
			AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
			DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
			Records: []CleanupRecordAudit{{
				ObjectID:   objectID,
				Checksum:   checksum,
				AccessURLs: []string{legacyURL},
				AccessMethods: []CleanupAccessMethod{{
					AccessID: "stale-legacy",
					URL:      legacyURL,
				}},
				AccessProbes: []CleanupAccessProbe{{
					URL:    legacyURL,
					Status: "error",
					Exists: &brokenExists,
				}},
			}},
		}}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if _, ok := backend.updatedAccessMethods[objectID]; ok {
		t.Fatalf("physical target in another scope must not be removed by legacy mapping: %+v", backend.updatedAccessMethods)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("valid physical record must not be deleted across scopes: %v", backend.deletedIDs)
	}
	if len(result.SkippedPaths) != 1 || result.SkippedPaths[0] != "data/file.bin" {
		t.Fatalf("expected idempotent skip across scope collision, got %+v", result.SkippedPaths)
	}
}

func TestApplyStorageCleanupRejectsAmbiguousLegacyPrefix(t *testing.T) {
	const checksum = "d64811e3625fff5c0846eb000e54d03d71d87aec4e8646d80808622468590cf"
	const objectID = "7a3852ae-5dc8-5721-8e24-437203fa63a6"
	const legacyURL = "s3://legacy/OHSU/file.bin"
	const physicalURL = "s3://bucket-a/legacy/OHSU/file.bin"

	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: objectID,
			Checksum: checksum,
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{{
				AccessID: "valid-physical",
				URL:      physicalURL,
			}},
		}},
		projectScopes: []domain.StorageBucketScope{
			{
				Bucket:       "bucket-a",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket-a/legacy",
			},
			{
				Bucket:       "bucket-b",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://bucket-b/legacy",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)
	brokenExists := false
	result, err := service.ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{{
			Kind:             "broken_access_url_error",
			NormalizedPath:   "data/file.bin",
			ObjectIDs:        []string{objectID},
			AccessURLs:       []string{legacyURL},
			AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
			DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
			Records: []CleanupRecordAudit{{
				ObjectID:   objectID,
				Checksum:   checksum,
				AccessURLs: []string{legacyURL},
				AccessMethods: []CleanupAccessMethod{{
					AccessID: "stale-legacy",
					URL:      legacyURL,
				}},
				AccessProbes: []CleanupAccessProbe{{
					URL:    legacyURL,
					Status: "error",
					Exists: &brokenExists,
				}},
			}},
		}}, false, false, false, false, false,
	)
	if err != nil {
		t.Fatalf("apply cleanup: %v", err)
	}
	if _, ok := backend.updatedAccessMethods[objectID]; ok {
		t.Fatalf("ambiguous legacy prefix must not remove a method from bucket-a: %+v", backend.updatedAccessMethods)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("ambiguous legacy prefix must not delete a physical record: %v", backend.deletedIDs)
	}
	if len(result.SkippedPaths) != 1 || result.SkippedPaths[0] != "data/file.bin" {
		t.Fatalf("expected idempotent skip for ambiguous legacy prefix, got %+v", result.SkippedPaths)
	}
}

func brokenAccessRepairFindingForTest(objectID string, checksum string, brokenURL string, goodURL string) CleanupApplyFinding {
	brokenExists := false
	return CleanupApplyFinding{
		Kind:             "broken_access_url_error",
		NormalizedPath:   "data/file.bin",
		ObjectIDs:        []string{objectID},
		AvailableActions: []string{sarecord.ActionRemoveBrokenAccessURLs},
		DefaultAction:    sarecord.ActionRemoveBrokenAccessURLs,
		Records: []CleanupRecordAudit{{
			ObjectID:   objectID,
			Checksum:   checksum,
			AccessURLs: []string{brokenURL, goodURL},
			AccessMethods: []CleanupAccessMethod{
				{AccessID: "stale-broken", URL: brokenURL},
				{AccessID: "stale-good", URL: goodURL},
			},
			AccessProbes: []CleanupAccessProbe{
				{URL: brokenURL, Status: "error", ErrorKind: "credential_missing", Exists: &brokenExists},
				{URL: goodURL, Status: "present"},
			},
		}},
	}
}

func TestApplyStorageCleanupValidation(t *testing.T) {
	service := NewStorageAnalyticsService(&fakeStorageAnalyticsBackend{})
	tests := []struct {
		name    string
		finding CleanupApplyFinding
		actions []CleanupApplyAction
		wantErr string
	}{
		{name: "unknown kind", finding: applyFinding("unknown_kind", "data/a.txt", []string{"obj-a"}, nil), wantErr: "unsupported cleanup finding kind"},
		{name: "unsupported action", finding: applyFinding("bucket_only_object", "s3://bucket/a", nil, []string{"s3://bucket/a"}), actions: []CleanupApplyAction{{Kind: "bucket_only_object", NormalizedPath: "s3://bucket/a", Action: sarecord.ActionDeleteSyfonRecord}}, wantErr: "not supported"},
		{name: "missing object ids", finding: applyFinding("repo_orphan_stale_record", "s3://bucket/a", nil, nil), wantErr: "missing object_ids"},
		{name: "metadata mismatch disallows deletion", finding: applyFinding("git_syfon_metadata_mismatch", "data/a.txt", []string{"obj-a"}, nil), actions: []CleanupApplyAction{{Kind: "git_syfon_metadata_mismatch", NormalizedPath: "data/a.txt", Action: sarecord.ActionDeleteBucketObject}}, wantErr: "not supported"},
		{name: "missing revalidation urls", finding: applyFinding("syfon_git_no_bucket", "data/a.txt", []string{"obj-a"}, nil), wantErr: "required for exact revalidation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "proj", nil, test.actions, []CleanupApplyFinding{test.finding}, false, false, false, false, false)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
			}
		})
	}
}

func TestApplyStorageCleanupUsesServerPolicyWhenAuditActionsAreStale(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{}}
	service := NewStorageAnalyticsService(backend)
	finding := applyFinding("bucket_syfon_no_git", "s3://bucket/no-git", []string{"obj-no-git"}, []string{"s3://bucket/no-git"})
	// An audit may have been rendered before Gecko advertised this manual
	// repair. The server policy, rather than this stale client snapshot, must
	// decide whether the requested action is allowed.
	finding.AvailableActions = []string{sarecord.ActionInspectEvidence}
	response, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{finding.NormalizedPath},
		[]CleanupApplyAction{{
			Kind:           finding.Kind,
			NormalizedPath: finding.NormalizedPath,
			Action:         sarecord.ActionDeleteBoth,
		}},
		[]CleanupApplyFinding{finding},
		false,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply stale bucket + Syfon audit finding: %v", err)
	}
	assertStringSet(t, "deleted IDs", response.DeletedRecordIDs, []string{"obj-no-git"})
	assertStringSet(t, "deleted bucket URLs", response.DeletedBucketObjectURLs, []string{"s3://bucket/no-git"})
}

func TestBuildStorageCleanupAuditAcceptsRecordWhenAnyAccessProbeIsLive(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	missingKey := sarecord.StorageProbeRequestKey("s3://bucket/legacy-a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	presentKey := sarecord.StorageProbeRequestKey("s3://bucket/current-a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
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
	for _, finding := range cleanup.Findings {
		if finding.NormalizedPath == "data/a.txt" && finding.Kind == "storage_object_missing" {
			t.Fatalf("a live canonical locator must suppress a missing-record finding: %+v", finding)
		}
	}
}

func TestBuildStorageCleanupAuditReportsBrokenBucketMappingSeparately(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	probeKey := sarecord.StorageProbeRequestKey("s3://bforepc-prod/path/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
			{ObjectURL: "s3://bucket/orphan", Bucket: "bucket", Key: "orphan", Path: "orphan", SizeBytes: 50},
			{ObjectURL: "s3://bucket/loose", Bucket: "bucket", Key: "loose", Path: "loose", SizeBytes: 25},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bucket/a",
				Bucket:           "bucket",
				Key:              "a",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			sarecord.StorageProbeRequestKey("s3://bucket/orphan", 50, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/orphan", 50, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		[]CleanupApplyFinding{{
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
	if !contains(response.DeletedRecordIDs, "obj-no-git") || !contains(response.DeletedBucketObjectURLs, "s3://bucket/no-git") {
		t.Fatalf("expected selected record and bucket object deletion, got %+v", response)
	}
	if !contains(backend.deletedIDs, "obj-no-git") || !contains(backend.deletedBucketObjects, "s3://bucket/no-git") {
		t.Fatalf("expected backend deletion, IDs=%v bucket=%v", backend.deletedIDs, backend.deletedBucketObjects)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/other", Bucket: "bucket", Key: "other", Path: "other", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://legacy-bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {},
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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

func TestApplyStorageCleanupRejectsStaleMissingObjectEvidence(t *testing.T) {
	finding := applyFinding(
		"syfon_git_no_bucket",
		"data/file.bin",
		[]string{"object-id"},
		[]string{"s3://bucket/data/file.bin"},
	)
	backend := &fakeStorageAnalyticsBackend{}
	service := NewStorageAnalyticsService(backend)

	_, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"project",
		nil,
		nil,
		[]CleanupApplyFinding{finding},
		false,
		false,
		false,
		false,
		false,
	)
	if err == nil || !strings.Contains(err.Error(), "expected missing, got status=\"present\"") {
		t.Fatalf("expected stale missing evidence to abort cleanup, got %v", err)
	}
	if len(backend.deletedIDs) != 0 || len(backend.deletedBucketObjects) != 0 {
		t.Fatalf("revalidation failure must not mutate storage, deleted IDs=%v bucket objects=%v", backend.deletedIDs, backend.deletedBucketObjects)
	}
}

func TestApplyStorageCleanupAcceptsFreshExactMissingEvidence(t *testing.T) {
	finding := applyFinding(
		"syfon_git_no_bucket",
		"data/file.bin",
		[]string{"object-id"},
		[]string{"s3://bucket/data/file.bin"},
	)
	backend := &fakeStorageAnalyticsBackend{
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			"cleanup-revalidate-0": {
				ID:                 "cleanup-revalidate-0",
				Status:             "not_found",
				ErrorKind:          "object_not_found",
				VerificationMethod: "ranged_get",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	result, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"project",
		nil,
		nil,
		[]CleanupApplyFinding{finding},
		false,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply cleanup with fresh exact evidence: %v", err)
	}
	if len(result.DeletedRecordIDs) != 1 || result.DeletedRecordIDs[0] != "object-id" {
		t.Fatalf("expected exact-missing record deletion, got %+v", result)
	}
}

func TestApplyStorageCleanupRejectsUnconfirmedHeadMiss(t *testing.T) {
	finding := applyFinding("syfon_git_no_bucket", "data/file.bin", []string{"object-id"}, []string{"s3://bucket/data/file.bin"})
	backend := &fakeStorageAnalyticsBackend{probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
		"cleanup-revalidate-0": {ID: "cleanup-revalidate-0", Status: "not_found", ErrorKind: "object_not_found"},
	}}
	service := NewStorageAnalyticsService(backend)
	_, err := service.ApplyStorageCleanup(context.Background(), "Bearer token", "org", "project", nil, nil, []CleanupApplyFinding{finding}, false, false, false, false, false)
	if err == nil || !strings.Contains(err.Error(), "verification_method=\"\"") {
		t.Fatalf("expected unconfirmed HEAD miss to block deletion, got %v", err)
	}
	if len(backend.deletedIDs) != 0 {
		t.Fatalf("unconfirmed HEAD miss deleted records: %v", backend.deletedIDs)
	}
}

func TestApplyStorageCleanupCanonicalizesScopedBucketURLsBeforeRevalidation(t *testing.T) {
	rawURL := "s3://bforepc-prod/02bd1708-9071-5950-aed4-38a7f3090900/file.bin"
	canonicalURL := "s3://bforepc/bforepc-prod/02bd1708-9071-5950-aed4-38a7f3090900/file.bin"
	finding := applyFinding(
		"bucket_syfon_no_git",
		rawURL,
		[]string{"object-id"},
		[]string{rawURL},
	)
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "HTAN_INT",
			ProjectID:    "BForePC",
			Path:         "s3://bforepc/bforepc-prod",
		}},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			"cleanup-revalidate-0": {
				ID:        "cleanup-revalidate-0",
				ObjectURL: canonicalURL,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/02bd1708-9071-5950-aed4-38a7f3090900/file.bin",
				Exists:    true,
				Status:    "present",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	result, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"HTAN_INT",
		"BForePC",
		[]string{rawURL},
		[]CleanupApplyAction{{
			Kind:           finding.Kind,
			NormalizedPath: finding.NormalizedPath,
			Action:         sarecord.ActionDeleteBoth,
		}},
		[]CleanupApplyFinding{finding},
		false,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply canonicalized cleanup: %v", err)
	}
	if len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != canonicalURL {
		t.Fatalf("expected canonical revalidation target %q, got %+v", canonicalURL, backend.probeItems)
	}
	if len(backend.deletedBucketObjects) != 1 || backend.deletedBucketObjects[0] != canonicalURL {
		t.Fatalf("expected canonical bucket delete target %q, got %+v", canonicalURL, backend.deletedBucketObjects)
	}
	if len(result.DeletedRecordIDs) != 1 || result.DeletedRecordIDs[0] != "object-id" {
		t.Fatalf("expected Syfon record deletion, got %+v", result)
	}
}

func TestApplyStorageCleanupContinuesWhenBucketOrphanVanishesAfterAudit(t *testing.T) {
	missingURL := "s3://bucket/orphan-gone"
	presentURL := "s3://bucket/orphan-present"
	missingFinding := applyFinding("bucket_syfon_no_git", missingURL, []string{"object-gone"}, []string{missingURL})
	presentFinding := applyFinding("bucket_syfon_no_git", presentURL, []string{"object-present"}, []string{presentURL})
	backend := &fakeStorageAnalyticsBackend{
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			"cleanup-revalidate-0": {
				ID:                 "cleanup-revalidate-0",
				ObjectURL:          missingURL,
				Status:             "not_found",
				ErrorKind:          "object_not_found",
				VerificationMethod: "ranged_get",
			},
			"cleanup-revalidate-1": {
				ID:        "cleanup-revalidate-1",
				ObjectURL: presentURL,
				Exists:    true,
				Status:    "present",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	result, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"project",
		[]string{missingURL, presentURL},
		[]CleanupApplyAction{
			{Kind: missingFinding.Kind, NormalizedPath: missingFinding.NormalizedPath, Action: sarecord.ActionDeleteBoth},
			{Kind: presentFinding.Kind, NormalizedPath: presentFinding.NormalizedPath, Action: sarecord.ActionDeleteBoth},
		},
		[]CleanupApplyFinding{missingFinding, presentFinding},
		false,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("apply cleanup after one bucket object vanished: %v", err)
	}
	assertStringSet(t, "deleted Syfon records", result.DeletedRecordIDs, []string{"object-gone", "object-present"})
	assertStringSet(t, "deleted bucket objects", result.DeletedBucketObjectURLs, []string{presentURL})
	assertStringSet(t, "skipped paths", result.SkippedPaths, []string{missingURL})
}

func TestAccessRepairDryRunShowsExactChangeAndFreshEvidence(t *testing.T) {
	const brokenURL = "s3://bucket/missing"
	const goodURL = "s3://bucket/present"
	backend := accessRepairBackendForTest(brokenURL, goodURL)
	markBrokenAccessURLs(backend, brokenURL)
	result, err := applyAccessRepairForTest(backend, true, brokenURL, goodURL)
	if err != nil {
		t.Fatalf("dry run access repair: %v", err)
	}
	if !result.DryRun || len(result.AccessMethodChanges) != 1 {
		t.Fatalf("expected one dry-run access method change, got %+v", result)
	}
	change := result.AccessMethodChanges[0]
	if change.ObjectID != "record-1" || len(change.Before) != 2 || len(change.After) != 1 || change.After[0].URL != goodURL || len(change.Removed) != 1 || change.Removed[0] != brokenURL || len(change.Retained) != 1 || change.Retained[0] != goodURL || len(change.Added) != 0 {
		t.Fatalf("unexpected access method preview: %+v", change)
	}
	if len(change.FreshProbes) != 2 || change.FreshProbes[0].Status != "not_found" || change.FreshProbes[1].Status != "present" {
		t.Fatalf("expected fresh missing and present probes, got %+v", change.FreshProbes)
	}
	if len(backend.updatedAccessMethods) != 0 || len(backend.deletedIDs) != 0 {
		t.Fatalf("dry run changed Syfon records: updates=%+v deletes=%+v", backend.updatedAccessMethods, backend.deletedIDs)
	}
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode dry-run response: %v", err)
	}
	if !strings.Contains(string(wire), `"access_method_changes"`) || strings.Contains(string(wire), "secret-header") {
		t.Fatalf("unexpected dry-run wire response: %s", wire)
	}
}

func TestAccessRepairApplyRejectsChangedStorageEvidence(t *testing.T) {
	const brokenURL = "s3://bucket/missing"
	const goodURL = "s3://bucket/present"
	tests := []struct {
		name       string
		brokenURLs []string
		wantError  string
	}{
		{name: "removed URL now exists", wantError: "not confirmed broken"},
		{name: "retained URL also missing", brokenURLs: []string{brokenURL, goodURL}, wantError: "no retained or replacement access URL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := accessRepairBackendForTest(brokenURL, goodURL)
			markBrokenAccessURLs(backend, test.brokenURLs...)
			_, err := applyAccessRepairForTest(backend, false, brokenURL, goodURL)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected %q, got %v", test.wantError, err)
			}
			if len(backend.updatedAccessMethods) != 0 || len(backend.deletedIDs) != 0 {
				t.Fatalf("rejected repair changed Syfon records: updates=%+v deletes=%+v", backend.updatedAccessMethods, backend.deletedIDs)
			}
		})
	}
}

func TestAccessRepairRejectsHeadOnlyMissingEvidence(t *testing.T) {
	const brokenURL = "s3://bucket/missing"
	const goodURL = "s3://bucket/present"
	backend := accessRepairBackendForTest(brokenURL, goodURL)
	markBrokenAccessURLs(backend, brokenURL)
	backend.probeResultsByURL[brokenURL] = gintegrationsyfon.BulkStorageProbeResult{Status: "not_found", ErrorKind: "object_not_found"}
	_, err := applyAccessRepairForTest(backend, false, brokenURL, goodURL)
	if err == nil || !strings.Contains(err.Error(), "not confirmed broken by an exact storage check") {
		t.Fatalf("expected HEAD-only evidence rejection, got %v", err)
	}
	if len(backend.updatedAccessMethods) != 0 {
		t.Fatalf("HEAD-only evidence updated access methods: %v", backend.updatedAccessMethods)
	}
}

func accessRepairBackendForTest(brokenURL string, goodURL string) *fakeStorageAnalyticsBackend {
	return &fakeStorageAnalyticsBackend{projectRecords: []gintegrationsyfon.ProjectRecord{{
		ObjectID: "record-1",
		AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
			{AccessID: "broken", Type: "s3", URL: brokenURL, Headers: []string{"secret-header"}},
			{AccessID: "good", Type: "s3", URL: goodURL},
		},
	}}}
}

func applyAccessRepairForTest(backend *fakeStorageAnalyticsBackend, dryRun bool, brokenURL string, goodURL string) (*CleanupApplyResponse, error) {
	finding := CleanupApplyFinding{
		Kind:           "syfon_broken_bucket_mapping",
		NormalizedPath: "data/file.bin",
		ObjectIDs:      []string{"record-1"},
		Records: []CleanupRecordAudit{{
			ObjectID: "record-1",
			AccessMethods: []CleanupAccessMethod{
				{AccessID: "broken", Type: "s3", URL: brokenURL},
				{AccessID: "good", Type: "s3", URL: goodURL},
			},
			AccessProbes: []CleanupAccessProbe{{URL: brokenURL, Operation: "metadata", Status: "not_found", ErrorKind: "object_not_found"}},
		}},
	}
	return NewStorageAnalyticsService(backend).ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]CleanupApplyFinding{finding}, false, false, false, false, dryRun,
	)
}

func TestApplyStorageCleanupDeletesSelectedProbeErrorRecords(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{}
	service := NewStorageAnalyticsService(backend)
	response, err := service.ApplyStorageCleanup(
		context.Background(),
		"Bearer token",
		"org",
		"project",
		[]string{"probe-row"},
		[]CleanupApplyAction{{
			Kind:           "probe_error",
			NormalizedPath: "probe-row",
			Action:         sarecord.ActionDeleteSyfonRecord,
		}},
		[]CleanupApplyFinding{{
			Kind:           "probe_error",
			NormalizedPath: "probe-row",
			ObjectIDs:      []string{"probe-object"},
		}},
		false,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("delete selected probe-error record: %v", err)
	}
	if len(response.DeletedRecordIDs) != 1 || response.DeletedRecordIDs[0] != "probe-object" {
		t.Fatalf("unexpected cleanup response: %+v", response)
	}
	if len(backend.deletedIDs) != 1 || backend.deletedIDs[0] != "probe-object" {
		t.Fatalf("expected Syfon deletion only, got %+v", backend)
	}
}
