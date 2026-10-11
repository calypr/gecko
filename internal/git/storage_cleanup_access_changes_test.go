package git

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

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

func applyAccessRepairForTest(backend *fakeStorageAnalyticsBackend, dryRun bool, brokenURL string, goodURL string) (*GitStorageCleanupApplyResponse, error) {
	finding := GitStorageCleanupApplyFinding{
		Kind:           "syfon_broken_bucket_mapping",
		NormalizedPath: "data/file.bin",
		ObjectIDs:      []string{"record-1"},
		Records: []GitStorageCleanupRecordAudit{{
			ObjectID: "record-1",
			AccessMethods: []GitStorageCleanupAccessMethod{
				{AccessID: "broken", Type: "s3", URL: brokenURL},
				{AccessID: "good", Type: "s3", URL: goodURL},
			},
			AccessProbes: []GitStorageCleanupAccessProbe{{URL: brokenURL, Operation: "metadata", Status: "not_found", ErrorKind: "object_not_found"}},
		}},
	}
	return NewStorageAnalyticsService(backend).ApplyStorageCleanup(
		context.Background(), "Bearer token", "org", "proj", nil, nil,
		[]GitStorageCleanupApplyFinding{finding}, false, false, false, false, dryRun,
	)
}
