package git

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func TestBuildStorageChainAuditListMissUsesMetadataConfirmation(t *testing.T) {
	const (
		repoPath     = "data/file.bin"
		checksum     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		canonicalURL = "s3://bucket/root/data/file.bin"
	)
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(checksum, 100),
	})
	probeKey := storageProbeRequestKey(canonicalURL, 100, checksum)
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
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			probeKey: {
				ID:        probeKey,
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
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 1 {
		t.Fatalf("expected HEAD-confirmed missing-object finding, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["probe_error"]; got != 0 {
		t.Fatalf("expected HEAD confirmation to resolve the LIST miss, got summary %+v", chain.Summary)
	}
	if backend.listProbeCalls != 0 || backend.probeCalls != 1 {
		t.Fatalf("expected one metadata confirmation and no exact LIST, got list=%d metadata=%d", backend.listProbeCalls, backend.probeCalls)
	}
}
