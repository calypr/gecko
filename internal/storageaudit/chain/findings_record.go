package chain

import (
	"fmt"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/storageaudit/model"
)

func buildChainBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) model.ChainFinding {
	objectURL := sarecord.CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("bucket_only_object", "verified")
	return model.ChainFinding{
		Kind:              "bucket_only_object",
		EvidenceStatus:    "verified",
		NormalizedPath:    objectURL,
		ObjectIDs:         []string{},
		AccessURLs:        []string{objectURL},
		BucketObjectURL:   objectURL,
		ResolvedBucket:    strings.TrimSpace(item.Bucket),
		ResolvedKey:       strings.TrimSpace(item.Key),
		ProbeStatus:       "enumerated",
		RecordCount:       0,
		SizeBytes:         item.SizeBytes,
		RecommendedAction: "Bucket object exists, but no Syfon record matched it.",
		Actionability:     actionability,
		AvailableActions:  availableActions,
		DefaultAction:     defaultAction,
		SupportsDryRun:    supportsDryRun,
		Evidence: &model.Evidence{
			AccessURLs:        []string{objectURL},
			BucketObjectURLs:  []string{objectURL},
			Buckets:           sarecord.UniqueStrings([]string{item.Bucket}),
			Keys:              sarecord.UniqueStrings([]string{item.Key}),
			StorageOperations: []string{model.StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}

func storageChainActionSupportForEvidence(kind string, evidenceStatus string) (string, []string, string, bool) {
	if strings.TrimSpace(kind) == "syfon_broken_bucket_mapping" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionRemoveBrokenAccessURLs, sarecord.ActionInspectEvidence},
			sarecord.ActionInspectEvidence,
			true
	}
	if strings.TrimSpace(kind) == "git_only_no_syfon" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionCreateSyfonRecord, sarecord.ActionInspectEvidence},
			sarecord.ActionCreateSyfonRecord,
			false
	}
	if strings.TrimSpace(kind) == "git_syfon_metadata_mismatch" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionDeleteSyfonRecord, sarecord.ActionDeleteBucketObject, sarecord.ActionDeleteBoth, sarecord.ActionInspectEvidence},
			"",
			true
	}
	repair := sarecord.RepairPolicyForKind(kind)
	if repair.Actionability == sarecord.ActionabilityAutoRepair && !sarecord.AllowsAutomaticRepair(kind, sarecord.EvidenceStatus(evidenceStatus)) {
		repair = sarecord.InspectOnlyRepairPolicy()
	}
	return repair.Actionability, append([]string(nil), repair.Actions...), repair.DefaultAction, repair.SupportsDryRun
}

func buildChainRecordFindings(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string) []model.ChainFinding {
	return buildChainRecordFindingsWithOptions(kind, record, gitPaths, bucketObjectURLs, action, false)
}

func buildChainRecordFindingsWithOptions(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string, preferSyfonRecordPath bool) []model.ChainFinding {
	paths := sarecord.UniqueStrings(gitPaths)
	if len(paths) == 0 {
		displayPath := sarecord.OrphanDisplayPath(strings.TrimSpace(record.Checksum), append(bucketObjectURLs, record.AccessURLs...))
		if preferSyfonRecordPath {
			displayPath = sarecord.SyfonRecordDisplayPath(record)
		}
		if displayPath == "" {
			displayPath = strings.TrimSpace(record.Checksum)
		}
		paths = []string{displayPath}
	}
	objectIDs := sarecord.UniqueStrings([]string{record.ObjectID})
	accessURLs := sarecord.UniqueStrings(record.AccessURLs)
	evidence := sarecord.BuildFindingEvidence(strings.TrimSpace(record.Checksum), gitPaths, []sarecord.State{record}, "enumerated_and_probed")
	if evidence != nil {
		evidence.BucketObjectURLs = sarecord.UniqueStrings(append(evidence.BucketObjectURLs, bucketObjectURLs...))
	}
	evidenceStatus := storageEvidenceStatus(record, len(bucketObjectURLs) > 0)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(kind, evidenceStatus)
	primaryProbe := selectChainProbe(record, bucketObjectURLs)
	suggestedFix := suggestedFixForChainFinding(kind, record)
	suggestedAction := suggestedActionForChainFinding(kind, record)
	if suggestedAction != "" && !stringSliceContains(availableActions, suggestedAction) {
		availableActions = append([]string(nil), availableActions...)
		availableActions = append(availableActions, suggestedAction)
	}
	findings := make([]model.ChainFinding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, model.ChainFinding{
			Kind:              kind,
			EvidenceStatus:    evidenceStatus,
			NormalizedPath:    path,
			Checksum:          strings.TrimSpace(record.Checksum),
			SourcePaths:       sarecord.UniqueStrings(gitPaths),
			ObjectIDs:         objectIDs,
			Records:           []model.CleanupRecordAudit{sarecord.CleanupRecordAuditForRecord(path, "access_url", record)},
			AccessURLs:        accessURLs,
			BucketObjectURL:   primaryProbe.bucketObjectURL,
			ResolvedBucket:    primaryProbe.probe.Bucket,
			ResolvedKey:       primaryProbe.probe.Key,
			ProbeStatus:       primaryProbe.probe.Status,
			ErrorKind:         primaryProbe.probe.ErrorKind,
			Error:             chainFindingError(kind, record, primaryProbe.probe),
			RecordCount:       1,
			SizeBytes:         record.Size,
			RecommendedAction: action,
			SuggestedFix:      suggestedFix,
			SuggestedAction:   suggestedAction,
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence:          evidence,
		})
	}
	return findings
}

func suggestedActionForChainFinding(kind string, record sarecord.State) string {
	return ""
}

func suggestedFixForChainFinding(kind string, record sarecord.State) string {
	normalizedKind := strings.TrimSpace(kind)
	switch normalizedKind {
	case "git_syfon_metadata_mismatch":
	case "syfon_broken_bucket_mapping":
		return ""
	default:
		return ""
	}
	mismatches := make([]string, 0, 2)
	if probe, ok := firstAccessProbeMismatch(record, "size_mismatch"); ok {
		bucketSize := int64(0)
		if probe.SizeBytes != nil {
			bucketSize = *probe.SizeBytes
		}
		mismatches = append(mismatches, fmt.Sprintf("Syfon record size is %s, bucket inventory reports %s", formatAuditSize(record.Size), formatAuditSize(bucketSize)))
	}
	if len(mismatches) == 0 {
		return "Bucket object exists, but its storage evidence does not match the Syfon/Git record. Review the record and bucket object before applying a destructive fix."
	}
	return strings.Join(mismatches, ". ") + ". Because the byte lengths differ, the bucket object cannot have the current Git/Syfon SHA-256. Recompute the bucket object's SHA-256 before manually reconciling Git and Syfon."
}

func firstAccessProbeMismatch(record sarecord.State, mismatch string) (model.CleanupAccessProbe, bool) {
	for _, probe := range sarecord.AccessProbesForRecord(record) {
		if accessProbeHasMismatch(probe, mismatch) {
			return probe, true
		}
	}
	return model.CleanupAccessProbe{}, false
}

func accessProbeHasMismatch(probe model.CleanupAccessProbe, mismatch string) bool {
	for _, item := range probe.ValidationMismatches {
		if strings.TrimSpace(item) == mismatch {
			return true
		}
	}
	return false
}

func formatAuditSize(size int64) string {
	if size <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%d B", size)
}

type chainProbeSelection struct {
	bucketObjectURL string
	probe           model.CleanupAccessProbe
}

func selectChainProbe(record sarecord.State, bucketObjectURLs []string) chainProbeSelection {
	probes := sarecord.AccessProbesForRecord(record)
	if len(bucketObjectURLs) > 0 {
		targets := map[string]struct{}{}
		for _, bucketURL := range bucketObjectURLs {
			targets[strings.TrimSpace(bucketURL)] = struct{}{}
		}
		for _, probe := range probes {
			objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if _, ok := targets[objectURL]; ok {
				return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
			}
		}
	}
	for _, probe := range probes {
		operation := strings.TrimSpace(probe.Operation)
		if operation != model.StorageChainValidationModeMetadata && operation != model.StorageChainValidationModeList {
			continue
		}
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	for _, probe := range probes {
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	return chainProbeSelection{}
}

func chainFindingError(kind string, record sarecord.State, probe model.CleanupAccessProbe) string {
	if trimmed := strings.TrimSpace(probe.Error); trimmed != "" {
		return trimmed
	}
	switch kind {
	case "syfon_broken_bucket_mapping":
		return "Syfon access URL did not resolve through a configured bucket mapping"
	case "syfon_missing_bucket_object", "syfon_git_no_bucket":
		return "mapped bucket object was not found"
	case "git_syfon_metadata_mismatch":
		return "mapped bucket object exists, but object evidence disagrees with the Syfon/Git record"
	case "probe_error":
		return sarecord.AccessErrorForRecord(record)
	default:
		return ""
	}
}

func storageEvidenceStatus(record sarecord.State, bucketObserved bool) string {
	return string(sarecord.AssessStorageRecordEvidence(record, bucketObserved).Status)
}
