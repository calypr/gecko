package storageaudit

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/cleanup"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	"github.com/go-git/go-git/v5/plumbing"
)

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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
			{ObjectURL: "s3://bucket/no-git", Bucket: "bucket", Key: "no-git", Path: "no-git", SizeBytes: 150},
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
			sarecord.StorageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ObjectURL:        "s3://bucket/missing",
				Bucket:           "bucket",
				Key:              "missing",
				Status:           "not_found",
				Exists:           false,
				ValidationStatus: "unverifiable",
			},
			sarecord.StorageProbeRequestKey("s3://bucket/no-git", 150, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/no-git", 150, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
				ObjectURL:        "s3://bucket/no-git",
				Bucket:           "bucket",
				Key:              "no-git",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			sarecord.StorageProbeRequestKey("s3://legacy-bucket/bad-map", 400, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"): {
				ID:               sarecord.StorageProbeRequestKey("s3://legacy-bucket/bad-map", 400, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present", Bucket: "bucket", Key: "present", Path: "present", SizeBytes: 100, MetaSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/missing", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present.txt", Bucket: "bucket", Key: "present.txt", Path: "present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/mismatch.txt", Bucket: "bucket", Key: "mismatch.txt", Path: "mismatch.txt", SizeBytes: 200},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/syfon-only.txt", 300, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/syfon-only.txt", 300, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
				ObjectURL:        "s3://bucket/syfon-only.txt",
				Status:           "not_found",
				ErrorKind:        "object_not_found",
				ValidationStatus: "unverifiable",
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
	if backend.probeCalls != 1 || len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != "s3://bucket/syfon-only.txt" {
		t.Fatalf("expected validate mode to HEAD the unresolved URL, got calls=%d items=%+v", backend.probeCalls, backend.probeItems)
	}
	if backend.listProbeCalls != 0 {
		t.Fatalf("expected complete Syfon inventory to skip exact LIST fallback, got calls=%d items=%+v", backend.listProbeCalls, backend.listProbeItems)
	}
	if chain.Summary.BucketPathExists != nil || chain.Summary.BucketSummaryMode != "" {
		t.Fatalf("expected validate mode not to claim bucket prefix summary, got %+v", chain.Summary)
	}
	if chain.Summary.ValidationMode != StorageChainValidationModeList {
		t.Fatalf("expected LIST validation mode, got %+v", chain.Summary)
	}
	if chain.Summary.BucketObjectCount != 2 || chain.Summary.CountsByKind["bucket_only_object"] != 0 {
		t.Fatalf("expected validate mode to preserve the complete inventory, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["bucket_syfon_git_complete"] != 2 {
		t.Fatalf("expected validate mode to count clean Syfon/Git/bucket join, got summary %+v", chain.Summary)
	}
	if chain.Summary.CountsByKind["probe_error"] != 0 || chain.Summary.CountsByKind["syfon_missing_bucket_object"] != 1 {
		t.Fatalf("expected HEAD-confirmed inventory miss, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected basename mismatch not to be a broken bucket mapping, got summary %+v", chain.Summary)
	}
	assertNoChainFinding(t, chain.Findings, "bucket_only_object")
	assertNoChainFinding(t, chain.Findings, "syfon_broken_bucket_mapping")
	finding := assertHasChainFinding(t, chain.Findings, "syfon_missing_bucket_object", "s3://bucket/syfon-only.txt")
	if finding.EvidenceStatus != "unknown" || finding.ProbeStatus != "not_found" || finding.Actionability != sarecord.ActionabilityInspectOnly || finding.DefaultAction != sarecord.ActionInspectEvidence {
		t.Fatalf("expected HEAD-classified missing object without automatic deletion authority, got %+v", finding)
	}
}

func TestBuildStorageChainAuditDefaultListExposesBucketOnlyObjects(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-present",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Organization: "org",
				Project:      "proj",
				Size:         100,
				AccessURLs:   []string{"s3://bucket/present.txt"},
			},
		},
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/present.txt", Bucket: "bucket", Key: "present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/loose.txt", Bucket: "bucket", Key: "loose.txt", SizeBytes: 25},
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
		t.Fatalf("build default chain audit: %v", err)
	}
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected default list audit to load bucket inventory, got %d calls", backend.listProjectBucketInventoryCalls)
	}
	if got := chain.Summary.CountsByKind["bucket_only_object"]; got != 1 {
		t.Fatalf("expected one bucket-only finding, got summary %+v", chain.Summary)
	}
	finding := assertHasChainFinding(t, chain.Findings, "bucket_only_object", "s3://bucket/loose.txt")
	if finding.DefaultAction != sarecord.ActionDeleteBucketObject || len(finding.ObjectIDs) != 0 {
		t.Fatalf("expected bucket-only deletion evidence without Syfon IDs, got %+v", finding)
	}
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
			sarecord.StorageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:                   sarecord.StorageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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
	if got := chain.Summary.CountsByKind["bucket_only_object"]; got != 0 {
		t.Fatalf("metadata validation must not claim bucket-only coverage, got summary %+v", chain.Summary)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
	if !strings.Contains(finding.SuggestedFix, "Syfon record size is 44 B, bucket inventory reports 40 B") ||
		!strings.Contains(finding.SuggestedFix, "bucket object cannot have the current Git/Syfon SHA-256") {
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/data/syfon-only.txt", Bucket: "bucket", Key: "data/syfon-only.txt", Path: "data/syfon-only.txt", SizeBytes: 200},
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainSvc.ChainAuditResponseCache = storecache.NewMemoryStorageChainAuditResponseCache()

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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
			{ObjectURL: "s3://bucket/other/clean.txt", Bucket: "bucket", Key: "other/clean.txt", Path: "other/clean.txt", SizeBytes: 300},
			{ObjectURL: "s3://bucket/data/syfon-only.txt", Bucket: "bucket", Key: "data/syfon-only.txt", Path: "data/syfon-only.txt", SizeBytes: 200},
		},
	}
	service := NewStorageAnalyticsService(backend)
	cache := storecache.NewMemoryStorageChainAuditResponseCache()
	service.chainSvc.ChainAuditResponseCache = cache
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
	cache.Mu.Lock()
	for key, entry := range cache.Entries {
		entry.Value.RefreshDurationMillis = 123
		cache.Entries[key] = entry
	}
	cache.Mu.Unlock()
	cachedRoot, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options)
	if err != nil {
		t.Fatalf("build cached root chain audit: %v", err)
	}
	if !cachedRoot.Summary.AuditCacheHit || cachedRoot.Summary.AuditRefreshDurationMs != 123 {
		t.Fatalf("expected cached root audit to include refresh duration, got %+v", cachedRoot.Summary)
	}
	subpath, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, options)
	if err != nil {
		t.Fatalf("build subpath chain audit: %v", err)
	}
	if !subpath.Summary.AuditCacheHit || subpath.Summary.AuditCacheSource != "memory:root" {
		t.Fatalf("expected subpath audit to project from root response cache, got %+v", subpath.Summary)
	}
	if subpath.Summary.AuditRefreshDurationMs != 123 {
		t.Fatalf("expected subpath audit to include root refresh duration, got %+v", subpath.Summary)
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
	cache.Mu.RLock()
	cacheEntryCount := len(cache.Entries)
	cache.Mu.RUnlock()
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100},
		},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainSvc.ChainAuditResponseCache = storecache.NewMemoryStorageChainAuditResponseCache()
	service.chainSvc.ProjectBucketCache = storecache.NewMemoryProjectBucketInventoryCache()
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
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected forced refresh to reuse the bucket inventory during its cooldown, got %d calls", backend.listProjectBucketInventoryCalls)
	}
}

func TestBuildStorageChainAuditForceBucketRefreshBypassesInventoryCache(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"},
		}},
		projectScopes: []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100}},
	}
	service := NewStorageAnalyticsService(backend)
	service.chainSvc.ChainAuditResponseCache = storecache.NewMemoryStorageChainAuditResponseCache()
	service.chainSvc.ProjectBucketCache = storecache.NewMemoryProjectBucketInventoryCache()
	options := StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate}

	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build first chain audit: %v", err)
	}
	options.ForceAuditRefresh = true
	options.ForceBucketRefresh = true
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, options); err != nil {
		t.Fatalf("build forced bucket refresh: %v", err)
	}
	if backend.listProjectBucketInventoryCalls != 2 {
		t.Fatalf("expected forced bucket refresh to issue a new inventory scan, got %d calls", backend.listProjectBucketInventoryCalls)
	}
}

func TestBuildStorageChainAuditForceBucketRefreshWaitsForOrdinaryRefresh(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/present.txt": lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
	})
	started := make(chan struct{})
	release := make(chan struct{})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID: "obj-present", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Organization: "org", Project: "proj", Size: 100, AccessURLs: []string{"s3://bucket/data/present.txt"},
		}},
		projectScopes:                     []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}},
		BucketObjects:                     []gintegrationsyfon.ProjectBucketObject{{ObjectURL: "s3://bucket/data/present.txt", Bucket: "bucket", Key: "data/present.txt", Path: "data/present.txt", SizeBytes: 100}},
		listProjectBucketInventoryStarted: started,
		listProjectBucketInventoryRelease: release,
	}
	service := NewStorageAnalyticsService(backend)
	service.chainSvc.ChainAuditResponseCache = storecache.NewMemoryStorageChainAuditResponseCache()
	service.chainSvc.ProjectBucketCache = storecache.NewMemoryProjectBucketInventoryCache()

	ordinaryDone := make(chan error, 1)
	go func() {
		_, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate, ForceAuditRefresh: true})
		ordinaryDone <- err
	}()
	<-started
	forcedDone := make(chan error, 1)
	go func() {
		_, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate, ForceAuditRefresh: true, ForceBucketRefresh: true})
		forcedDone <- err
	}()
	close(release)
	if err := <-ordinaryDone; err != nil {
		t.Fatalf("ordinary refresh: %v", err)
	}
	if err := <-forcedDone; err != nil {
		t.Fatalf("forced bucket refresh: %v", err)
	}
	if backend.listProjectBucketInventoryCalls != 2 {
		t.Fatalf("expected forced request to run after the ordinary refresh, got %d inventory calls", backend.listProjectBucketInventoryCalls)
	}
}

func TestProjectBucketInventoryUsesStaleCacheWhenRefreshFails(t *testing.T) {
	backend := &fakeStorageAnalyticsBackend{
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: "s3://bucket/data/present.txt",
			Bucket:    "bucket",
			Key:       "data/present.txt",
			Path:      "data/present.txt",
			SizeBytes: 100,
		}},
	}
	service := NewStorageAnalyticsService(backend)
	cache := storecache.NewMemoryProjectBucketInventoryCache()
	service.chainSvc.ProjectBucketCache = cache

	objects, _, _, err := service.LoadCachedProjectBucketValidationInventory(context.Background(), "Bearer token", "org", "proj", "", false)
	if err != nil || len(objects) != 1 {
		t.Fatalf("seed cached bucket inventory: objects=%d err=%v", len(objects), err)
	}
	key := storecache.ProjectBucketInventoryCacheKey("org", "proj", "")
	cache.Mu.Lock()
	entry := cache.Entries[key]
	entry.Value.CachedAt = time.Now().Add(-storecache.ProjectBucketInventoryRefreshInterval - time.Second)
	cache.Entries[key] = entry
	cache.Mu.Unlock()
	chainKey := service.chainSvc.ProjectChainInputCacheKey("org", "proj") + "::bucket-validation-inventory::"
	service.chainSvc.ExpireChainInputCacheEntry(chainKey)
	backend.listProjectBucketObjectsErr = fmt.Errorf("provider returned an incomplete listing")

	objects, lookup, metadata, err := service.LoadCachedProjectBucketValidationInventory(context.Background(), "Bearer token", "org", "proj", "", false)
	if err != nil {
		t.Fatalf("expected stale inventory fallback, got %v", err)
	}
	if len(objects) != 1 || len(lookup) != 1 {
		t.Fatalf("expected cached inventory after failed refresh, got objects=%d lookup=%d", len(objects), len(lookup))
	}
	if metadata.Complete || !metadata.Stale || !strings.Contains(metadata.Warning, "provider returned an incomplete listing") {
		t.Fatalf("expected stale degraded inventory metadata, got %+v", metadata)
	}
	if backend.listProjectBucketInventoryCalls != 2 {
		t.Fatalf("expected one seed scan and one failed refresh, got %d calls", backend.listProjectBucketInventoryCalls)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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

	usage, err := service.join.listProjectFileUsageByObjectIDs(context.Background(), "Bearer token", "org", "proj", objectIDs, cleanupInactiveDays)
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

	usage, err := service.join.listProjectFileUsageByObjectIDs(context.Background(), "Bearer token", "org", "proj", []string{"obj-a"}, cleanupInactiveDays)
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
		bucketInventoryIncomplete: true,
		BucketInventoryWarning:    "listing incomplete",
		bucketInventoryObservedAt: "2026-09-03T21:20:22Z",
		projectScopes: []domain.StorageBucketScope{
			{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"},
		},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
	if response.Summary.BucketInventoryComplete || response.Summary.BucketInventoryWarning != "listing incomplete" || response.Summary.BucketInventoryObserved != "2026-09-03T21:20:22Z" {
		t.Fatalf("expected items mode to preserve incomplete inventory metadata, got %+v", response.Summary)
	}
	if _, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeItems,
		BucketPathPrefix:    "/CONFIG/",
		ProbeMode:           StorageChainProbeModeInventoryOnly,
		ForceAuditRefresh:   true,
		ForceBucketRefresh:  true,
	}); err != nil {
		t.Fatalf("force items inventory refresh: %v", err)
	}
	if backend.listProjectBucketObjectsCalls != 2 {
		t.Fatalf("expected force bucket refresh to bypass the items cache, got %d calls", backend.listProjectBucketObjectsCalls)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 999},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:                   sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 999},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:                   sarecord.StorageProbeRequestKey("s3://bucket/a", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:            "s3://bucket/a",
				Bucket:               "bucket",
				Key:                  "a",
				Status:               "present",
				Exists:               true,
				ValidationStatus:     "mismatched",
				ValidationMismatches: []string{"size_mismatch"},
			},
			sarecord.StorageProbeRequestKey("s3://legacy-bucket/bad-map.txt", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"): {
				ID:               sarecord.StorageProbeRequestKey("s3://legacy-bucket/bad-map.txt", 200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
		probeResults[sarecord.StorageProbeRequestKey(objectURL, 100+int64(i), checksum)] = gintegrationsyfon.BulkStorageProbeResult{
			ID:               sarecord.StorageProbeRequestKey(objectURL, 100+int64(i), checksum),
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
		BucketObjects:  bucketObjects,
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
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
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", Bucket: "bforepc", Key: "bforepc-prod/OHSU/slide.ome.tiff", Path: "slide.ome.tiff", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        "s3://bforepc-prod/OHSU/slide.ome.tiff",
				Status:           "not_found",
				Exists:           false,
				ErrorKind:        "credential_missing",
				Error:            `no stored bucket credential found for bucket "bforepc-prod"`,
				ValidationStatus: "unverifiable",
			},
			sarecord.StorageProbeRequestKey("s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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

func TestBuildStorageChainAuditClassifiesSameBucketPathChecksumConflictAsMetadataMismatch(t *testing.T) {
	const repoPath = "JHU/ashley_kiemen/hematoxylin_eosin_stain/Level_2/HTA201_3/HTA201_3_ometif/HTA201_3_1_0022.offsets.json"
	const gitChecksum = "99ec07eaecf97dc1cd7b7c4fb717c0661062eb57013d51b81780cbd3a302e323"
	const syfonChecksum = "6b33300205aa4730ace8a04147c9223d547b1bf4f54001602d4ec147b7841a4f"
	const size = int64(43)
	const canonicalURL = "s3://bforepc/bforepc-prod/" + repoPath
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(gitChecksum, size),
	})
	now := time.Date(2026, 7, 6, 18, 27, 23, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID:     "93fd2da3-7b66-5232-bb77-002b78cb8ade",
			Name:         "HTA201_3_1_0022.offsets.json",
			Checksum:     syfonChecksum,
			Organization: "HTAN_INT",
			Project:      "BForePC",
			Size:         size,
			UpdatedAt:    &now,
			AccessURLs:   []string{"s3://bforepc-prod/" + repoPath},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "HTAN_INT",
			ProjectID:    "BForePC",
			Path:         "s3://bforepc/bforepc-prod",
		}},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: canonicalURL,
			Bucket:    "bforepc",
			Key:       "bforepc-prod/" + repoPath,
			Path:      repoPath,
			SizeBytes: size,
		}},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	findings := loadAllChainFindings(t, service, "HTAN_INT", "BForePC", chain)
	mismatch := assertHasChainFinding(t, findings, "git_syfon_metadata_mismatch", repoPath)
	if mismatch.Actionability != sarecord.ActionabilityInspectOnly || mismatch.DefaultAction != sarecord.ActionInspectEvidence || !contains(mismatch.AvailableActions, sarecord.ActionInspectEvidence) {
		t.Fatalf("expected checksum conflict to be inspect-only, got %+v", mismatch)
	}
	if contains(mismatch.AvailableActions, sarecord.ActionDeleteBoth) || contains(mismatch.AvailableActions, sarecord.ActionDeleteBucketObject) {
		t.Fatalf("checksum conflict must not advertise bucket deletion, got %+v", mismatch.AvailableActions)
	}
	if !strings.Contains(mismatch.SuggestedFix, gitChecksum) || !strings.Contains(mismatch.SuggestedFix, syfonChecksum) {
		t.Fatalf("expected both conflicting checksums in the suggested fix, got %q", mismatch.SuggestedFix)
	}
	for _, kind := range []string{"bucket_only_object", "bucket_syfon_no_git", "git_only_no_syfon"} {
		if got := chain.Summary.CountsByKind[kind]; got != 0 {
			t.Fatalf("expected same-path checksum conflict to suppress %s, got summary %+v", kind, chain.Summary)
		}
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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

func TestBuildStorageChainAuditUsesCanonicalInventoryCandidatesBeforeMissingBucket(t *testing.T) {
	sourceChecksum := "db458f223d03fddc541e2ad3c93ff3c152a20ecaa636cdfcb031cff6fe0c6b3a"
	mappedChecksum := "b8639eca6eed65224f3af4c70425f578b949ee7202051388c331404cf35073f6"
	sourcePath := "OHSU/koei_chin/visium_hd/4-R2/outs/segmented_outputs/analysis/diffexp/gene_expression_kmeans_10_clusters/differential_expression.csv"
	mappedRepoPath := "OHSU/koei_chin/visium_hd/4-R2/outs/segmented_outputs/analysis/diffexp/gene_expression_kmeans_2_clusters/differential_expression.csv"
	mappedRawURL := "s3://bforepc-prod/97a32a3a-1a30-5e14-bd94-7bf65ac98f27/" + mappedChecksum
	sourceRawURL := "s3://bforepc-prod/" + sourcePath
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		sourcePath:     lfsPointer(sourceChecksum, 1200),
		mappedRepoPath: lfsPointer(mappedChecksum, 1300),
	})
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{
			{
				ObjectID:     "obj-source",
				Name:         "differential_expression.csv",
				Checksum:     sourceChecksum,
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         1200,
				UpdatedAt:    &now,
				AccessURLs:   []string{sourceRawURL},
			},
			{
				ObjectID:     "obj-mapped",
				Name:         "differential_expression.csv",
				Checksum:     mappedChecksum,
				Organization: "HTAN_INT",
				Project:      "BForePC",
				Size:         1300,
				UpdatedAt:    &now,
				AccessURLs:   []string{mappedRawURL},
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL: "s3://bforepc/bforepc-prod/" + sourcePath,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/" + sourcePath,
				Path:      sourcePath,
				SizeBytes: 1200,
			},
			{
				ObjectURL: "s3://bforepc/bforepc-prod/" + mappedRepoPath,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/" + mappedRepoPath,
				Path:      mappedRepoPath,
				SizeBytes: 1300,
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
		t.Fatalf("expected canonical inventory candidates to complete both chains, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 0 {
		t.Fatalf("expected no Git+Syfon missing bucket findings after canonical inventory matching, got summary %+v", chain.Summary)
	}
	if backend.listProbeCalls != 0 {
		t.Fatalf("expected no Syfon bulk-list fallback call, got %d calls with items %+v", backend.listProbeCalls, backend.listProbeItems)
	}
}

func TestBuildStorageChainAuditBForePCListMissUsesMetadataConfirmation(t *testing.T) {
	const repoPath = "OHSU/koei_chin/visium_hd/4-R2/outs/binned_outputs/square_002um/filtered_feature_bc_matrix/barcodes.tsv.gz"
	const checksum = "a9809ffd53130272f775ad1bbea3166dbb4853bc6fa73bfa7a51535c4bc4f599"
	const size = int64(46063992)
	const canonicalURL = "s3://bforepc/bforepc-prod/" + repoPath
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(checksum, size),
	})
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID:     "b5162717-bf49-528a-a2a8-47fbd3b0f090",
			Name:         "barcodes.tsv.gz",
			Checksum:     checksum,
			Organization: "HTAN_INT",
			Project:      "BForePC",
			Size:         size,
			AccessURLs:   []string{"s3://bforepc-prod/" + repoPath},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "HTAN_INT",
			ProjectID:    "BForePC",
			Path:         "s3://bforepc/bforepc-prod",
		}},
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey(canonicalURL, size, checksum): {
				ID:        sarecord.StorageProbeRequestKey(canonicalURL, size, checksum),
				ObjectURL: canonicalURL,
				Status:    "not_found",
				ErrorKind: "object_not_found",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(
		context.Background(),
		"Bearer token",
		"HTAN_INT",
		"BForePC",
		refName,
		"",
		mirrorPath,
		repo,
		hash,
		StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate},
	)
	if err != nil {
		t.Fatalf("build BForePC chain audit: %v", err)
	}
	if backend.listProbeCalls != 0 {
		t.Fatalf("expected no exact LIST fallback for %q, got calls=%d items=%+v", canonicalURL, backend.listProbeCalls, backend.listProbeItems)
	}
	if backend.probeCalls != 1 || len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != canonicalURL {
		t.Fatalf("expected metadata confirmation for the LIST miss at %q, got calls=%d items=%+v", canonicalURL, backend.probeCalls, backend.probeItems)
	}
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 1 {
		t.Fatalf("expected HEAD-confirmed missing-bucket finding, got summary %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["probe_error"]; got != 0 {
		t.Fatalf("expected HEAD confirmation to resolve the LIST miss, got summary %+v", chain.Summary)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
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

func TestBuildStorageChainAuditDoesNotInferMappingMismatchFromRelocatedBasename(t *testing.T) {
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{
				ObjectURL: relocatedURL,
				Bucket:    "bforepc",
				Key:       "bforepc-prod/2532ab27-4961-57da-8bef-e9774093bf56/" + objectHash,
				Path:      objectHash,
				SizeBytes: 23739,
			},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey(rawURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey(rawURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				ObjectURL:        rawURL,
				Bucket:           "cbds",
				Key:              "0b76f9ee-3c82-58e5-8ae2-47addb5d6d79/" + objectHash,
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
			sarecord.StorageProbeRequestKey(canonicalURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey(canonicalURL, 23739, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "CONFIG", mirrorPath, repo, hash, StorageChainAuditOptions{})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if got := chain.Summary.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected relocated basename not to imply a broken bucket mapping, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["bucket_only_object"]; got != 0 {
		t.Fatalf("expected relocated hash not to be double-counted as bucket-only, got %+v", chain.Summary)
	}
	if got := chain.Summary.CountsByKind["syfon_git_no_bucket"]; got != 1 {
		t.Fatalf("expected exact mapped-key miss to remain a missing-object finding, got %+v", chain.Summary)
	}
	findings := loadAllChainFindings(t, service, "HTAN_INT", "BForePC", chain)
	assertHasChainFinding(t, findings, "syfon_git_no_bucket", "CONFIG/cbds-BForePC.json")
}

func TestBuildStorageChainAuditReportsMissingAccessMethodWhenAnotherMethodExists(t *testing.T) {
	const checksum = "13b9f3b94f83dda05ceac0c95ca7168d7db5c7cd79f28f0b4c448be3373387ef"
	const missingURL = "s3://bforepc-prod/6595b01b-64a2-5b68-a607-d4bbb489bf17/" + checksum
	const presentURL = "s3://bforepc-prod/OHSU/koei_chin/visium_hd/2-R1/outs/feature_slice.h5"
	const canonicalMissingURL = "s3://bforepc/bforepc-prod/6595b01b-64a2-5b68-a607-d4bbb489bf17/" + checksum
	const canonicalPresentURL = "s3://bforepc/bforepc-prod/OHSU/koei_chin/visium_hd/2-R1/outs/feature_slice.h5"
	const repoPath = "OHSU/koei_chin/visium_hd/2-R1/outs/feature_slice.h5"
	const size = int64(529952905)

	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(checksum, size),
	})
	now := time.Date(2026, 3, 13, 20, 1, 49, 0, time.UTC)
	backend := &fakeStorageAnalyticsBackend{
		projectRecords: []gintegrationsyfon.ProjectRecord{{
			ObjectID:     "c0dfb5f8-650e-5b5e-ba23-a43afb070fbf",
			Name:         "feature_slice.h5",
			Checksum:     checksum,
			Organization: "HTAN_INT",
			Project:      "BForePC",
			Size:         size,
			UpdatedAt:    &now,
			AccessURLs:   []string{missingURL, presentURL},
			AccessMethods: []gintegrationsyfon.ProjectAccessMethod{
				{AccessID: "s3", Type: "s3", URL: missingURL},
				{AccessID: "s3", Type: "s3", URL: presentURL},
			},
		}},
		projectScopes: []domain.StorageBucketScope{{
			Bucket:       "bforepc",
			Organization: "HTAN_INT",
			ProjectID:    "BForePC",
			Path:         "s3://bforepc/bforepc-prod",
		}},
		usageByObject:             map[string]gintegrationsyfon.FileUsage{},
		bucketInventoryIncomplete: true,
		BucketInventoryWarning:    "terminal replay returned different page content",
		bucketInventoryObservedAt: "2026-09-03T21:20:22Z",
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{{
			ObjectURL: canonicalPresentURL,
			Bucket:    "bforepc",
			Key:       "bforepc-prod/OHSU/koei_chin/visium_hd/2-R1/outs/feature_slice.h5",
			Path:      repoPath,
			SizeBytes: size,
		}},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey(canonicalMissingURL, size, checksum): {
				ID:               sarecord.StorageProbeRequestKey(canonicalMissingURL, size, checksum),
				ObjectURL:        canonicalMissingURL,
				Bucket:           "bforepc",
				Key:              "bforepc-prod/6595b01b-64a2-5b68-a607-d4bbb489bf17/" + checksum,
				Status:           "not_found",
				Exists:           false,
				ErrorKind:        "object_not_found",
				ValidationStatus: "unverifiable",
			},
			sarecord.StorageProbeRequestKey(canonicalPresentURL, size, checksum): {
				ID:               sarecord.StorageProbeRequestKey(canonicalPresentURL, size, checksum),
				ObjectURL:        canonicalPresentURL,
				Bucket:           "bforepc",
				Key:              "bforepc-prod/OHSU/koei_chin/visium_hd/2-R1/outs/feature_slice.h5",
				Status:           "present",
				Exists:           true,
				ValidationStatus: "matched",
			},
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "HTAN_INT", "BForePC", refName, "", mirrorPath, repo, hash, StorageChainAuditOptions{
		BucketInventoryMode: StorageChainBucketModeValidate,
		ValidationMode:      StorageChainValidationModeList,
	})
	if err != nil {
		t.Fatalf("build chain audit: %v", err)
	}
	if !containsProbeTarget(backend.probeItems, canonicalMissingURL) || containsProbeTarget(backend.probeItems, canonicalPresentURL) {
		t.Fatalf("expected only the unresolved access method to be probed, got %+v", backend.probeItems)
	}
	if chain.Summary.BucketInventoryComplete || !strings.Contains(chain.Summary.BucketInventoryWarning, "terminal replay") || chain.Summary.BucketInventoryObserved != "2026-09-03T21:20:22Z" || chain.Summary.BucketInventorySource != "live" {
		t.Fatalf("expected degraded live inventory metadata, got %+v", chain.Summary)
	}
	findings := loadAllChainFindings(t, service, "HTAN_INT", "BForePC", chain)
	finding := assertHasChainFinding(t, findings, "syfon_broken_bucket_mapping", repoPath)
	if finding.Actionability != sarecord.ActionabilityManualChoice || finding.DefaultAction != sarecord.ActionInspectEvidence || !contains(finding.AvailableActions, sarecord.ActionRemoveBrokenAccessURLs) {
		t.Fatalf("expected broken access URL removal action, got %+v", finding)
	}
	if len(finding.Records) != 1 || len(finding.Records[0].AccessProbes) != 1 || finding.Records[0].AccessProbes[0].URL != missingURL {
		t.Fatalf("expected only the missing access URL in repair evidence, got %+v", finding.Records)
	}
	remaining := cleanup.RemainingAccessMethodsAfterBrokenRemoval(finding.Records[0])
	if len(remaining) != 1 || remaining[0].URL != presentURL {
		t.Fatalf("expected repair to retain only the working access method, got %+v", remaining)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a.txt", Bucket: "bucket", Key: "a.txt", Path: "a.txt", SizeBytes: 100},
		},
		probeResults: map[string]gintegrationsyfon.BulkStorageProbeResult{
			sarecord.StorageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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

func TestBuildStorageChainAuditFallsBackToHeadWhenProjectBucketListFails(t *testing.T) {
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
			sarecord.StorageProbeRequestKey("s3://bucket/prefix/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"): {
				ID:               sarecord.StorageProbeRequestKey("s3://bucket/prefix/a.txt", 100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{BucketInventoryMode: StorageChainBucketModeValidate})
	if err != nil {
		t.Fatalf("expected HEAD fallback instead of a hard error, got %v", err)
	}
	if backend.listProjectBucketInventoryCalls != 1 {
		t.Fatalf("expected one bucket validation inventory attempt, got %d", backend.listProjectBucketInventoryCalls)
	}
	if backend.probeCalls != 1 || len(backend.probeItems) != 1 || backend.probeItems[0].ObjectURL != "s3://bucket/prefix/a.txt" {
		t.Fatalf("expected one HEAD fallback after bucket inventory failure, got calls=%d items=%+v", backend.probeCalls, backend.probeItems)
	}
	if chain.Summary.BucketInventoryAvailable || !strings.Contains(chain.Summary.BucketInventoryError, "mapped bucket target may be missing or inaccessible") {
		t.Fatalf("expected degraded inventory details in successful audit, got %+v", chain.Summary)
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
		BucketObjects: []gintegrationsyfon.ProjectBucketObject{
			{ObjectURL: "s3://bucket/a", Bucket: "bucket", Key: "a", Path: "a", SizeBytes: 100},
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
		},
	}
	service := NewStorageAnalyticsService(backend)

	chain, err := service.BuildStorageChainAuditWithOptions(context.Background(), "Bearer token", "org", "proj", refName, "data", mirrorPath, repo, hash, StorageChainAuditOptions{})
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

func BenchmarkBuildStorageFolderDefaultModeLargeDirectoryPage(b *testing.B) {
	children := make([]RepoAnalyticsChild, 1000)
	for index := 0; index < 1000; index++ {
		children[index] = RepoAnalyticsChild{
			Name:       fmt.Sprintf("file-%04d.txt", index),
			Path:       fmt.Sprintf("data/file-%04d.txt", index),
			Type:       "file",
			FileCount:  1,
			TotalBytes: int64(index + 1),
		}
	}
	hash := plumbing.NewHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	mirrorPath := filepath.Join(b.TempDir(), "mirror.git")
	index := repoindex.RepoAnalyticsIndexFromSidecar(RepoAnalyticsIndexSidecar{
		SchemaVersion: repoindex.SchemaVersion,
		CommitHash:    hash.String(),
		RefName:       "main",
		GeneratedAt:   time.Now().UTC(),
		Directories: []RepoAnalyticsDirectory{
			{
				Path:             "data",
				DirectChildCount: len(children),
				FileCount:        len(children),
				TotalBytes:       500500,
				Children:         children,
			},
		},
	})
	repoindex.Cache.Put(mirrorPath, hash, index)
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

func TestBuildStorageChainAuditListMissUsesMetadataConfirmation(t *testing.T) {
	const (
		repoPath     = "data/file.bin"
		checksum     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		canonicalURL = "s3://bucket/root/data/file.bin"
	)
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		repoPath: lfsPointer(checksum, 100),
	})
	probeKey := sarecord.StorageProbeRequestKey(canonicalURL, 100, checksum)
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
