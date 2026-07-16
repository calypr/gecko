package git

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func TestBuildStorageChainAuditListMissDoesNotTriggerMetadataConfirmation(t *testing.T) {
	const (
		repoPath     = "data/file.bin"
		checksum     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		canonicalURL = "s3://bucket/root/data/file.bin"
	)
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(checksum, 100),
	})
	listKey := storageListValidationRequestKey(canonicalURL, 100, "file.bin")
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID:     "object-id",
			Name:         "file.bin",
			Checksum:     checksum,
			Organization: "org",
			Project:      "project",
			Size:         100,
			AccessURLs:   []string{canonicalURL},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bucket",
			Organization: "org",
			ProjectID:    "project",
			Path:         "s3://bucket/root",
		}},
		listProbeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			listKey: {
				ID:        listKey,
				ObjectURL: canonicalURL,
				Status:    "not_found",
				ErrorKind: "object_not_found",
			},
		},
	}

	chain, err := NewStorageAnalyticsService(backend).BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"org",
		"project",
		refName,
		"",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err != nil {
		t.Fatalf("build storage chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 0 {
		t.Fatalf("LIST misses must not become missing-object findings, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["probe_error"]; got != 1 {
		t.Fatalf("expected a LIST miss to remain a probe error, got summary %+v", chain.Summary)
	}
	if backend.listProbeCalls != 1 || backend.probeCalls != 0 {
		t.Fatalf("expected one exact LIST and no metadata confirmation, got list=%d metadata=%d", backend.listProbeCalls, backend.probeCalls)
	}
}
