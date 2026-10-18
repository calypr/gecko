package chain

import (
	"slices"
	"testing"

	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func TestFilterStorageChainSummaryPreservesCleanJoinCount(t *testing.T) {
	summary := model.ChainSummary{
		CountsByKind: map[string]int{
			"bucket_syfon_git_complete":   7,
			"git_syfon_metadata_mismatch": 3,
			"syfon_broken_bucket_mapping": 2,
		},
		BucketObjectCount:   10,
		SyfonRecordCount:    10,
		GitTrackedFileCount: 10,
	}
	findings := []model.ChainFinding{
		{Kind: "git_syfon_metadata_mismatch", NormalizedPath: "data/a.txt"},
	}

	filtered := filterStorageChainSummary(summary, findings)

	if got := filtered.CountsByKind["bucket_syfon_git_complete"]; got != 7 {
		t.Fatalf("expected clean-chain count to survive filtering, got %d in %+v", got, filtered)
	}
	if got := filtered.CountsByKind["git_syfon_metadata_mismatch"]; got != 1 {
		t.Fatalf("expected filtered mismatch count from findings, got %d in %+v", got, filtered)
	}
	if got := filtered.CountsByKind["syfon_broken_bucket_mapping"]; got != 0 {
		t.Fatalf("expected filtered broken-mapping count to reset, got %d in %+v", got, filtered)
	}
}

func TestStorageChainActionSupportAllowsManualProbeErrorDeletion(t *testing.T) {
	actionability, actions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(
		"probe_error",
		string(sarecord.EvidenceUnknown),
	)

	if actionability != sarecord.ActionabilityManualChoice {
		t.Fatalf("expected manual probe-error deletion, got %q", actionability)
	}
	if defaultAction != sarecord.ActionDeleteSyfonRecord {
		t.Fatalf("expected delete-Syfon default, got %q", defaultAction)
	}
	if !supportsDryRun {
		t.Fatal("expected probe-error deletion to support a dry run")
	}
	if len(actions) != 2 || actions[0] != sarecord.ActionDeleteSyfonRecord || actions[1] != sarecord.ActionInspectEvidence {
		t.Fatalf("unexpected probe-error actions: %+v", actions)
	}
}

func TestStorageChainActionSupportKeepsUnverifiedMissingFindingInspectOnly(t *testing.T) {
	actionability, actions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(
		"syfon_git_no_bucket",
		string(sarecord.EvidenceUnknown),
	)

	if actionability != sarecord.ActionabilityInspectOnly {
		t.Fatalf("expected unverified missing finding to remain inspect-only, got %q", actionability)
	}
	if defaultAction != sarecord.ActionInspectEvidence || supportsDryRun {
		t.Fatalf("unexpected unverified finding defaults: action=%q dry_run=%t", defaultAction, supportsDryRun)
	}
	if len(actions) != 1 || actions[0] != sarecord.ActionInspectEvidence {
		t.Fatalf("unexpected unverified finding actions: %+v", actions)
	}
}

func TestStorageChainActionSupportMarksBucketSyfonNoGitManualRepair(t *testing.T) {
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("bucket_syfon_no_git", "verified")
	if actionability != sarecord.ActionabilityManualChoice {
		t.Fatalf("expected manual-choice actionability, got %q", actionability)
	}
	if defaultAction != sarecord.ActionDeleteBoth {
		t.Fatalf("expected delete-both default action, got %q", defaultAction)
	}
	if !supportsDryRun {
		t.Fatal("manual repair must advertise a dry-run")
	}
	for _, action := range []string{sarecord.ActionDeleteBoth, sarecord.ActionDeleteSyfonRecord, sarecord.ActionDeleteBucketObject} {
		if !slices.Contains(availableActions, action) {
			t.Fatalf("expected action %q, got %+v", action, availableActions)
		}
	}
}
