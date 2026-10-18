package record

import (
	"testing"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func TestClassifyStorageFindingDoesNotTreatRepeatedBasenameAsBrokenMapping(t *testing.T) {
	record := State{
		ProjectRecord: gintegrationsyfon.ProjectRecord{
			ObjectID:   "obj-a",
			Checksum:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Size:       100,
			AccessURLs: []string{"s3://bucket-alias/path/features.tsv.gz"},
		},
		CanonicalAccessURLs: []string{"s3://bucket/root/path/features.tsv.gz"},
		AccessProbes: []gintegrationsyfon.BulkStorageProbeResult{
			{
				ObjectURL:        "s3://bucket/root/path/features.tsv.gz",
				Operation:        model.StorageChainValidationModeList,
				Bucket:           "bucket",
				Key:              "root/path/features.tsv.gz",
				Status:           "not_found",
				Exists:           false,
				ErrorKind:        "object_not_found",
				ValidationStatus: "unverifiable",
			},
		},
	}
	bucketObjects := map[string]gintegrationsyfon.ProjectBucketObject{
		"s3://bucket/root/unrelated/features.tsv.gz": {
			ObjectURL: "s3://bucket/root/unrelated/features.tsv.gz",
			Bucket:    "bucket",
			Key:       "root/unrelated/features.tsv.gz",
			SizeBytes: 100,
		},
	}

	if got := ClassifyStorageFinding(record, bucketObjects); got != FindingObjectMissing {
		t.Fatalf("expected a repeated basename without a verified raw object to remain missing, got %s", got)
	}
}

func TestClassifyStorageFindingSuppressesRawURLFailuresWhenScopedProbeMatches(t *testing.T) {
	record := State{
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

	if got := ClassifyStorageFinding(record, nil); got != FindingNone {
		t.Fatalf("expected scoped matched probe to suppress raw URL credential miss, got %s", got)
	}
}

func TestClassifyStorageFindingSuppressesRawURLMismatchWhenScopedProbeMatches(t *testing.T) {
	record := State{
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

	if got := ClassifyStorageFinding(record, nil); got != FindingNone {
		t.Fatalf("expected scoped matched probe to suppress raw URL mismatch, got %s", got)
	}
}

func TestClassifyStorageFindingSurfacesRepairableBrokenAccessMethod(t *testing.T) {
	record := State{
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

	if got := ClassifyStorageFinding(record, nil); got != FindingBrokenBucketMap {
		t.Fatalf("expected the broken independent access method to remain repairable, got %s", got)
	}
	broken := RepairableBrokenAccessRecord(record)
	if len(broken.AccessProbes) != 1 || broken.AccessProbes[0].ObjectURL != "s3://retired-bucket/JHU/slide.ome.tiff" {
		t.Fatalf("expected only broken access probe in repair record, got %+v", broken.AccessProbes)
	}
	remaining, shouldDelete, ok := RepairBrokenBucketMappingRecord(broken)
	if !ok || shouldDelete {
		t.Fatalf("expected repair update, got ok=%v shouldDelete=%v remaining=%+v", ok, shouldDelete, remaining)
	}
	if len(remaining) != 1 || remaining[0].URL != "s3://bforepc-prod/JHU/slide.ome.tiff" {
		t.Fatalf("expected working access method to remain, got %+v", remaining)
	}
}
