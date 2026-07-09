package git

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func TestBuildStorageChainAuditRequiresMetadataToConfirmListMiss(t *testing.T) {
	const (
		repoPath     = "data/file.bin"
		checksum     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		canonicalURL = "s3://bucket/root/data/file.bin"
	)
	tests := []struct {
		name           string
		metadataResult gintegrationsyfon.BulkStorageProbeResult
		wantMissing    int
		wantProbeError int
	}{
		{
			name: "metadata confirms missing",
			metadataResult: gintegrationsyfon.BulkStorageProbeResult{
				Status:    "not_found",
				ErrorKind: "object_not_found",
			},
			wantMissing: 1,
		},
		{
			name: "metadata failure leaves absence unknown",
			metadataResult: gintegrationsyfon.BulkStorageProbeResult{
				Status:    "error",
				ErrorKind: "storage_error",
				Error:     "metadata lookup failed",
			},
			wantProbeError: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
				repoPath: lfsPointer(checksum, 100),
			})
			listKey := storageListValidationRequestKey(canonicalURL, 100, "file.bin")
			metadataKey := storageProbeRequestKey(canonicalURL, 100, checksum)
			metadataResult := test.metadataResult
			metadataResult.ID = metadataKey
			metadataResult.ObjectURL = canonicalURL
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
				probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
					metadataKey: metadataResult,
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
			if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != test.wantMissing {
				t.Fatalf("unexpected missing count %d in summary %+v", got, chain.Summary)
			}
			if got := chain.Summary.CountsByKind["probe_error"]; got != test.wantProbeError {
				t.Fatalf("unexpected probe-error count %d in summary %+v", got, chain.Summary)
			}
			if backend.listProbeCalls != 1 || backend.probeCalls != 1 {
				t.Fatalf("expected one LIST candidate check and one metadata confirmation, got list=%d metadata=%d", backend.listProbeCalls, backend.probeCalls)
			}
		})
	}
}
