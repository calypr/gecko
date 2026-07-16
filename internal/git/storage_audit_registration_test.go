package git

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func TestRegisterGitOnlySyfonRecordsUsesScopedBucketEvidence(t *testing.T) {
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"OHSU/slide.ome.tiff": lfsPointer(checksum, 100),
	})
	objectURL := "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff"
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "HTAN_INT",
			ProjectID:    "BForePC",
			Path:         "s3://bforepc/bforepc-prod",
		}},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: objectURL,
			Bucket:    "bforepc",
			Key:       "bforepc-prod/OHSU/slide.ome.tiff",
			SizeBytes: 100,
		}},
	}
	service := NewStorageAnalyticsService(backend)

	audit, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{})
	if err != nil {
		t.Fatalf("build audit: %v", err)
	}
	finding := assertHasChainFinding(t, audit.Findings, "git_only_no_syfon", "OHSU/slide.ome.tiff")
	if finding.BucketObjectURL != objectURL || finding.BucketSizeBytes != 100 || finding.Evidence == nil || finding.Evidence.BucketEvaluation != "present" {
		t.Fatalf("expected audit to expose mapped bucket evidence, got %+v", finding)
	}

	response, err := service.RegisterGitOnlySyfonRecords(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, mirrorPath, repo, hash, GitOnlySyfonRegistrationRequest{
		ExpectedGitRevision: hash.String(),
		RepoPaths:           []string{"OHSU/slide.ome.tiff"},
	})
	if err != nil {
		t.Fatalf("register missing Syfon record: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Status != "created" || response.Results[0].BucketObjectURL != objectURL {
		t.Fatalf("unexpected registration response: %+v", response)
	}
	if len(backend.registeredObjects) != 1 {
		t.Fatalf("expected one Syfon registration, got %+v", backend.registeredObjects)
	}
	registered := backend.registeredObjects[0]
	if registered.Checksum != checksum || registered.Size != 100 || registered.Name != "slide.ome.tiff" {
		t.Fatalf("unexpected Syfon candidate: %+v", registered)
	}
	if len(registered.AccessURLs) != 1 || registered.AccessURLs[0] != objectURL {
		t.Fatalf("expected canonical scoped access URL, got %+v", registered.AccessURLs)
	}
}

func TestRegisterGitOnlySyfonRecordsRefusesBucketSizeMismatch(t *testing.T) {
	checksum := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.bin": lfsPointer(checksum, 100),
	})
	objectURL := "s3://bucket/root/data/a.bin"
	actualSize := int64(99)
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket/root"}},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: objectURL,
			Bucket:    "bucket",
			Key:       "root/data/a.bin",
			Path:      "data/a.bin",
			SizeBytes: 99,
		}},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			"git-only-register-0-0": {ID: "git-only-register-0-0", ObjectURL: objectURL, Exists: true, Status: "present", SizeBytes: &actualSize},
		},
	}
	service := NewStorageAnalyticsService(backend)
	response, err := service.RegisterGitOnlySyfonRecords(context.Background(), "Bearer token", "org", "proj", refName, mirrorPath, repo, hash, GitOnlySyfonRegistrationRequest{
		ExpectedGitRevision: hash.String(),
		RepoPaths:           []string{"data/a.bin"},
	})
	if err != nil {
		t.Fatalf("register missing Syfon record: %v", err)
	}
	if len(backend.registeredObjects) != 0 || len(response.Results) != 1 || response.Results[0].Status != "skipped" {
		t.Fatalf("expected size mismatch to block registration, got response=%+v registrations=%+v", response, backend.registeredObjects)
	}
}

func TestRegisterGitOnlySyfonRecordsFindsChecksumKeyedBucketObjectFromInventory(t *testing.T) {
	checksum := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.bin": lfsPointer(checksum, 100),
	})
	objectURL := "s3://bucket/root/" + checksum
	backend := &fakeStorageAnalyticsBackend{
		projectScopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket/root"}},
		bucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL:  objectURL,
			Bucket:     "bucket",
			Key:        "root/" + checksum,
			Path:       checksum,
			SizeBytes:  100,
			MetaSHA256: checksum,
		}},
	}
	service := NewStorageAnalyticsService(backend)

	response, err := service.RegisterGitOnlySyfonRecords(context.Background(), "Bearer token", "org", "proj", refName, mirrorPath, repo, hash, GitOnlySyfonRegistrationRequest{
		ExpectedGitRevision: hash.String(),
		RepoPaths:           []string{"data/a.bin"},
	})
	if err != nil {
		t.Fatalf("register missing Syfon record: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Status != "created" || response.Results[0].BucketObjectURL != objectURL {
		t.Fatalf("expected checksum-keyed inventory object to be registered, got %+v", response)
	}
	if len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != objectURL {
		t.Fatalf("expected registration to probe the inventory-resolved checksum object, got %+v", backend.probeItems)
	}
}
