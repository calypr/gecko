package git

import (
	"context"
	"strings"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

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
		[]GitStorageCleanupApplyFinding{finding},
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
				ID:        "cleanup-revalidate-0",
				Status:    "not_found",
				ErrorKind: "object_not_found",
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
		[]GitStorageCleanupApplyFinding{finding},
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
		[]GitStorageCleanupApplyAction{{
			Kind:           finding.Kind,
			NormalizedPath: finding.NormalizedPath,
			Action:         storageActionDeleteBoth,
		}},
		[]GitStorageCleanupApplyFinding{finding},
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
				ID:        "cleanup-revalidate-0",
				ObjectURL: missingURL,
				Status:    "not_found",
				ErrorKind: "object_not_found",
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
		[]GitStorageCleanupApplyAction{
			{Kind: missingFinding.Kind, NormalizedPath: missingFinding.NormalizedPath, Action: storageActionDeleteBoth},
			{Kind: presentFinding.Kind, NormalizedPath: presentFinding.NormalizedPath, Action: storageActionDeleteBoth},
		},
		[]GitStorageCleanupApplyFinding{missingFinding, presentFinding},
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
