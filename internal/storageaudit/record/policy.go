package record

import "strings"

func AllowsAutomaticRepair(kind string, evidenceStatus EvidenceStatus) bool {
	if evidenceStatus != EvidenceVerified {
		return false
	}
	switch strings.TrimSpace(kind) {
	case "bucket_only_object", "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
		return true
	default:
		return false
	}
}

const (
	ActionabilityAutoRepair   = "auto_repair"
	ActionabilityManualChoice = "manual_choice"
	ActionabilityInspectOnly  = "inspect_only"

	ActionRemoveBrokenAccessURLs = "remove_broken_access_urls"
	ActionDeleteSyfonRecord      = "delete_syfon_record"
	ActionDeleteBucketObject     = "delete_bucket_object"
	ActionDeleteBoth             = "delete_both"
	ActionInspectEvidence        = "inspect_evidence"
	ActionCreateSyfonRecord      = "create_syfon_record"
)

type RepairPolicy struct {
	Actionability  string
	Actions        []string
	DefaultAction  string
	SupportsDryRun bool
}

func RepairPolicyForKind(kind string) RepairPolicy {
	switch strings.TrimSpace(kind) {
	case "bucket_only_object":
		return autoRepair(ActionDeleteBucketObject, ActionInspectEvidence)
	case "bucket_syfon_no_git":
		return RepairPolicy{
			Actionability: ActionabilityManualChoice,
			Actions: []string{
				ActionDeleteBoth,
				ActionDeleteSyfonRecord,
				ActionDeleteBucketObject,
				ActionInspectEvidence,
			},
			DefaultAction:  ActionDeleteBoth,
			SupportsDryRun: true,
		}
	case "repo_orphan_live_object":
		return autoRepair(ActionDeleteBoth, ActionDeleteSyfonRecord, ActionDeleteBucketObject, ActionInspectEvidence)
	case "repo_orphan_stale_record", "stale_duplicate_record", "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
		return autoRepair(ActionDeleteSyfonRecord, ActionInspectEvidence)
	case "broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping":
		return autoRepair(ActionRemoveBrokenAccessURLs, ActionDeleteSyfonRecord, ActionInspectEvidence)
	case "storage_validation_mismatch":
		return RepairPolicy{
			Actionability: ActionabilityManualChoice,
			Actions: []string{
				ActionRemoveBrokenAccessURLs,
				ActionDeleteSyfonRecord,
				ActionDeleteBucketObject,
				ActionDeleteBoth,
				ActionInspectEvidence,
			},
			SupportsDryRun: true,
		}
	case "git_syfon_metadata_mismatch":
		return InspectOnlyRepairPolicy()
	case "probe_error":
		return RepairPolicy{
			Actionability: ActionabilityManualChoice,
			Actions: []string{
				ActionDeleteSyfonRecord,
				ActionInspectEvidence,
			},
			DefaultAction:  ActionDeleteSyfonRecord,
			SupportsDryRun: true,
		}
	default:
		return InspectOnlyRepairPolicy()
	}
}

func InspectOnlyRepairPolicy() RepairPolicy {
	return RepairPolicy{
		Actionability:  ActionabilityInspectOnly,
		Actions:        []string{ActionInspectEvidence},
		DefaultAction:  ActionInspectEvidence,
		SupportsDryRun: false,
	}
}

func autoRepair(defaultAction string, extraActions ...string) RepairPolicy {
	actions := append([]string{defaultAction}, extraActions...)
	return RepairPolicy{
		Actionability:  ActionabilityAutoRepair,
		Actions:        unique(actions),
		DefaultAction:  defaultAction,
		SupportsDryRun: true,
	}
}

func unique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
