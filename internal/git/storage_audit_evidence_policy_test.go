package git

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/storageaudit"
)

func TestStorageChainActionSupportAllowsManualProbeErrorDeletion(t *testing.T) {
	actionability, actions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(
		"probe_error",
		string(storageaudit.EvidenceUnknown),
	)

	if actionability != storageActionabilityManualChoice {
		t.Fatalf("expected manual probe-error deletion, got %q", actionability)
	}
	if defaultAction != storageActionDeleteSyfonRecord {
		t.Fatalf("expected delete-Syfon default, got %q", defaultAction)
	}
	if !supportsDryRun {
		t.Fatal("expected probe-error deletion to support a dry run")
	}
	if len(actions) != 2 || actions[0] != storageActionDeleteSyfonRecord || actions[1] != storageActionInspectEvidence {
		t.Fatalf("unexpected probe-error actions: %+v", actions)
	}
}

func TestStorageChainActionSupportKeepsUnverifiedMissingFindingInspectOnly(t *testing.T) {
	actionability, actions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(
		"syfon_git_no_bucket",
		string(storageaudit.EvidenceUnknown),
	)

	if actionability != storageActionabilityInspectOnly {
		t.Fatalf("expected unverified missing finding to remain inspect-only, got %q", actionability)
	}
	if defaultAction != storageActionInspectEvidence || supportsDryRun {
		t.Fatalf("unexpected unverified finding defaults: action=%q dry_run=%t", defaultAction, supportsDryRun)
	}
	if len(actions) != 1 || actions[0] != storageActionInspectEvidence {
		t.Fatalf("unexpected unverified finding actions: %+v", actions)
	}
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
		[]GitStorageCleanupApplyAction{{
			Kind:           "probe_error",
			NormalizedPath: "probe-row",
			Action:         storageActionDeleteSyfonRecord,
		}},
		[]GitStorageCleanupApplyFinding{{
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
