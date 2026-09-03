package git

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/gecko/internal/storageaudit"
)

func (service *StorageAnalyticsService) loadStorageAuditSyfonRevision(ctx context.Context, authorizationHeader string, organization string, project string) (string, error) {
	summary, err := service.storage.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return "", fmt.Errorf("get Syfon revision for storage audit: %w", err)
	}
	if summary == nil {
		return "", fmt.Errorf("get Syfon revision for storage audit: response summary is missing")
	}
	return fmt.Sprintf(
		"%d:%s:%s",
		summary.RecordCount,
		strings.TrimSpace(summary.RecordLatestUpdatedTime),
		strings.TrimSpace(summary.RecordRevision),
	), nil
}

func assessStorageRecordEvidence(record projectRecordState, bucketObserved bool) storageaudit.Assessment {
	probes := make([]storageaudit.Probe, 0, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probes = append(probes, storageaudit.Probe{
			Operation:        strings.TrimSpace(probe.Operation),
			Status:           strings.TrimSpace(probe.Status),
			ErrorKind:        strings.TrimSpace(probe.ErrorKind),
			ValidationStatus: strings.TrimSpace(probe.ValidationStatus),
		})
	}
	return storageaudit.Assess(probes, bucketObserved)
}

func storageEvidenceStatus(record projectRecordState, bucketObserved bool) string {
	return string(assessStorageRecordEvidence(record, bucketObserved).Status)
}

func storageChainActionSupportForEvidence(kind string, evidenceStatus string) (string, []string, string, bool) {
	if strings.TrimSpace(kind) == "syfon_broken_bucket_mapping" {
		return storageActionabilityManualChoice,
			[]string{storageActionRemoveBrokenAccessURLs, storageActionInspectEvidence},
			storageActionInspectEvidence,
			true
	}
	if strings.TrimSpace(kind) == "git_only_no_syfon" && strings.TrimSpace(evidenceStatus) == string(storageaudit.EvidenceVerified) {
		return storageActionabilityManualChoice,
			[]string{storageActionCreateSyfonRecord, storageActionInspectEvidence},
			storageActionCreateSyfonRecord,
			false
	}
	if strings.TrimSpace(kind) == "git_syfon_metadata_mismatch" && strings.TrimSpace(evidenceStatus) == string(storageaudit.EvidenceVerified) {
		return storageActionabilityManualChoice,
			[]string{storageActionDeleteSyfonRecord, storageActionDeleteBucketObject, storageActionDeleteBoth, storageActionInspectEvidence},
			"",
			true
	}
	policy := storageRepairPolicyForKind(kind)
	if policy.actionability == storageActionabilityAutoRepair && !storageaudit.AllowsAutomaticRepair(kind, storageaudit.EvidenceStatus(evidenceStatus)) {
		policy = inspectOnlyStorageRepairPolicy()
	}
	return policy.actionability, append([]string(nil), policy.actions...), policy.defaultAction, policy.supportsDryRun
}
