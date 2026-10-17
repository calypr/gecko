package record

import (
	"slices"
	"testing"
)

func TestStorageRepairPolicyCoversFindingKinds(t *testing.T) {
	tests := []struct {
		kind          string
		defaultAction string
		actions       []string
		actionability string
	}{
		{"bucket_only_object", ActionDeleteBucketObject, []string{ActionDeleteBucketObject}, ActionabilityAutoRepair},
		{"bucket_syfon_no_git", ActionDeleteBoth, []string{ActionDeleteBoth, ActionDeleteSyfonRecord, ActionDeleteBucketObject}, ActionabilityManualChoice},
		{"repo_orphan_live_object", ActionDeleteBoth, []string{ActionDeleteBoth, ActionDeleteSyfonRecord, ActionDeleteBucketObject}, ActionabilityAutoRepair},
		{"repo_orphan_stale_record", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"stale_duplicate_record", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"live_duplicate_conflict", ActionInspectEvidence, []string{ActionInspectEvidence}, ActionabilityInspectOnly},
		{"broken_access_url_error", ActionRemoveBrokenAccessURLs, []string{ActionRemoveBrokenAccessURLs, ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"broken_bucket_mapping", ActionRemoveBrokenAccessURLs, []string{ActionRemoveBrokenAccessURLs, ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"syfon_broken_bucket_mapping", ActionRemoveBrokenAccessURLs, []string{ActionRemoveBrokenAccessURLs, ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"storage_validation_mismatch", "", []string{ActionDeleteSyfonRecord, ActionDeleteBucketObject, ActionDeleteBoth}, ActionabilityManualChoice},
		{"git_syfon_metadata_mismatch", ActionInspectEvidence, []string{ActionInspectEvidence}, ActionabilityInspectOnly},
		{"storage_object_missing", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"syfon_git_no_bucket", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"syfon_missing_bucket_object", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityAutoRepair},
		{"git_only_no_syfon", ActionInspectEvidence, []string{ActionInspectEvidence}, ActionabilityInspectOnly},
		{"storage_probe_error", ActionInspectEvidence, []string{ActionInspectEvidence}, ActionabilityInspectOnly},
		{"probe_error", ActionDeleteSyfonRecord, []string{ActionDeleteSyfonRecord}, ActionabilityManualChoice},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			policy := RepairPolicyForKind(test.kind)
			if policy.DefaultAction != test.defaultAction {
				t.Fatalf("expected default %q, got %q", test.defaultAction, policy.DefaultAction)
			}
			if policy.Actionability != test.actionability {
				t.Fatalf("expected actionability %q, got %q", test.actionability, policy.Actionability)
			}
			for _, action := range test.actions {
				if !slices.Contains(policy.Actions, action) {
					t.Fatalf("expected action %q in %+v", action, policy.Actions)
				}
			}
		})
	}
}

func TestAutomaticRepairRequiresVerifiedMissingEvidence(t *testing.T) {
	if !AllowsAutomaticRepair("bucket_only_object", EvidenceVerified) {
		t.Fatal("verified bucket-only inventory evidence should allow bucket deletion")
	}
	if AllowsAutomaticRepair("syfon_git_no_bucket", EvidenceUnknown) {
		t.Fatal("unknown evidence must not allow automatic repair")
	}
	if !AllowsAutomaticRepair("syfon_git_no_bucket", EvidenceVerified) {
		t.Fatal("verified missing evidence should allow the missing-record repair")
	}
	if AllowsAutomaticRepair("bucket_syfon_no_git", EvidenceVerified) {
		t.Fatal("absence from Git is informational and must not auto-delete storage")
	}
}
