package record

import (
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func CompareRecordState(left State, right State) int {
	if left.Usage.DownloadCount != right.Usage.DownloadCount {
		if left.Usage.DownloadCount > right.Usage.DownloadCount {
			return 1
		}
		return -1
	}
	if CompareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime) != 0 {
		return CompareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime)
	}
	return CompareOptionalTime(left.UpdatedAt, right.UpdatedAt)
}

func CompareOptionalTime(left *time.Time, right *time.Time) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return -1
	case right == nil:
		return 1
	case left.After(*right):
		return 1
	case left.Before(*right):
		return -1
	default:
		return 0
	}
}

func RecordHasBrokenAccess(record State) bool {
	if len(record.AccessURLs) == 0 {
		return true
	}
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) != "" {
			return false
		}
	}
	return true
}

func RecordsContainLiveUsage(matches []State) bool {
	for _, match := range matches {
		if match.Usage.DownloadCount > 0 || match.Usage.UploadCount > 0 || match.Usage.LastAccessTime != nil || match.Usage.LastDownloadTime != nil || match.Usage.LastUploadTime != nil {
			return true
		}
	}
	return false
}

func ClassifyStorageFinding(record State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) FindingKind {
	if RecordHasBrokenAccess(record) {
		return FindingBrokenAccessURL
	}
	resolution := ResolveRecordStorage(record, bucketObjectsByURL)
	assessment := AssessStorageRecordEvidence(record, len(resolution.MatchedBucketObjectURLs) > 0)
	if len(RepairableBrokenAccessProbes(record)) > 0 && RecordHasUsableAccessProbe(record) {
		return FindingBrokenBucketMap
	}
	if resolution.HasAcceptedCanonicalProbe {
		return FindingNone
	}
	if InventoryHasValidationMismatch(record, resolution.MatchedBucketObjectURLs, bucketObjectsByURL) {
		return FindingValidationMismatch
	}
	if resolution.HasValidationMismatch || assessment.MetadataMismatch {
		return FindingValidationMismatch
	}
	if resolution.HasAcceptedCanonicalProbe || assessment.Present {
		return FindingNone
	}
	if assessment.MappingBroken {
		return FindingBrokenBucketMap
	}
	if len(record.AccessProbes) == 0 {
		if len(bucketObjectsByURL) > 0 && len(resolution.CandidateURLs) > 0 {
			return FindingProbeError
		}
		return FindingNone
	}
	if assessment.Missing {
		return FindingObjectMissing
	}
	// A metadata miss is definitive for cleanup and chain classification when
	// the backend did not attach a more specific error kind.
	if HasNotFoundMetadataProbe(record) {
		return FindingObjectMissing
	}
	if HasNotFoundListProbe(record) && len(bucketObjectsByURL) > 0 {
		return FindingObjectMissing
	}
	if resolution.HasProbeError || assessment.Status == "unknown" {
		return FindingProbeError
	}
	return FindingNone
}

func HasNotFoundMetadataProbe(record State) bool {
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) == model.StorageChainValidationModeMetadata && strings.TrimSpace(probe.Status) == "not_found" {
			return true
		}
	}
	return false
}

func HasNotFoundListProbe(record State) bool {
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) == model.StorageChainValidationModeList && strings.TrimSpace(probe.Status) == "not_found" {
			return true
		}
	}
	return false
}

func StorageProbeValidationMismatchIsSignificant(record State, probe gintegrationsyfon.BulkStorageProbeResult) bool {
	if strings.TrimSpace(probe.ValidationStatus) != "mismatched" {
		return false
	}
	significantMismatches := 0
	for _, mismatch := range probe.ValidationMismatches {
		switch strings.TrimSpace(mismatch) {
		case "", "name_mismatch":
			continue
		default:
			significantMismatches++
		}
	}
	if significantMismatches == 0 {
		return false
	}
	if !SyfonProbeHasMismatch(probe, "size_mismatch") || significantMismatches != 1 {
		return true
	}
	if probe.SizeBytes == nil {
		return true
	}
	return !StorageSizesMatchForAudit(record.Size, *probe.SizeBytes)
}

func InventoryHasValidationMismatch(record State, bucketObjectURLs []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(bucketObjectURLs) == 0 {
		return false
	}
	for _, objectURL := range bucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if !StorageSizesMatchForAudit(record.Size, item.SizeBytes) {
			return true
		}
	}
	return false
}

func StorageSizesMatchForAudit(expectedSize int64, observedSize int64) bool {
	if expectedSize <= 0 || observedSize <= 0 {
		return true
	}
	diff := expectedSize - observedSize
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1
}

func AccessProbesForRecord(record State) []model.CleanupAccessProbe {
	if len(record.AccessProbes) > 0 {
		probes := make([]model.CleanupAccessProbe, 0, len(record.AccessProbes))
		for _, probe := range record.AccessProbes {
			exists := probe.Exists
			probes = append(probes, model.CleanupAccessProbe{
				URL:                  probe.ObjectURL,
				Operation:            probe.Operation,
				Provider:             probe.Provider,
				Bucket:               probe.Bucket,
				Key:                  probe.Key,
				Path:                 probe.Path,
				Exists:               &exists,
				Status:               probe.Status,
				Error:                probe.Error,
				ErrorKind:            probe.ErrorKind,
				SizeBytes:            probe.SizeBytes,
				MetaSHA256:           probe.MetaSHA256,
				ETag:                 probe.ETag,
				LastModified:         probe.LastModified,
				ValidationStatus:     probe.ValidationStatus,
				SizeMatch:            probe.SizeMatch,
				NameMatch:            probe.NameMatch,
				SHA256Match:          probe.SHA256Match,
				ValidationMismatches: append([]string(nil), probe.ValidationMismatches...),
			})
		}
		return probes
	}
	if len(record.AccessURLs) == 0 {
		return []model.CleanupAccessProbe{{
			URL:       "",
			Status:    "missing",
			Error:     "no access URLs present",
			ErrorKind: "missing_access_url",
		}}
	}
	probes := make([]model.CleanupAccessProbe, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) == "" {
			probes = append(probes, model.CleanupAccessProbe{
				URL:       accessURL,
				Status:    "missing",
				Error:     "blank access URL",
				ErrorKind: "missing_access_url",
			})
			continue
		}
		probes = append(probes, model.CleanupAccessProbe{
			URL:    accessURL,
			Status: "present",
		})
	}
	return probes
}

func AccessStatusForRecord(record State) string {
	switch ClassifyStorageFinding(record, nil) {
	case FindingBrokenAccessURL, FindingObjectMissing:
		return "missing"
	case FindingBrokenBucketMap, FindingProbeError:
		return "error"
	case FindingValidationMismatch:
		return "mismatched"
	}
	return "present"
}

func AccessErrorForRecord(record State) string {
	switch ClassifyStorageFinding(record, nil) {
	case FindingBrokenAccessURL:
		return "no usable access URL present"
	case FindingBrokenBucketMap:
		return "Syfon access URL did not resolve through a configured bucket mapping"
	case FindingObjectMissing:
		return "storage object not found"
	case FindingValidationMismatch:
		return "mapped bucket object exists, but object evidence disagrees with the Syfon/Git record"
	case FindingProbeError:
		return "storage probe failed"
	}
	return ""
}

func BuildBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) model.CleanupFinding {
	objectURL := CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := StorageCleanupActionSupport("bucket_only_object")
	return model.CleanupFinding{
		Kind:                "bucket_only_object",
		NormalizedPath:      objectURL,
		ObjectIDs:           []string{},
		Records:             []model.CleanupRecordAudit{},
		RecommendedAction:   "Review and delete bucket object that has no Syfon record",
		RepoDeleteCandidate: false,
		CleanupScope:        "bucket_object",
		SizeBytes:           item.SizeBytes,
		LastUpdated:         strings.TrimSpace(item.LastModified),
		Actionability:       actionability,
		AvailableActions:    availableActions,
		DefaultAction:       defaultAction,
		SupportsDryRun:      supportsDryRun,
		Evidence: &model.Evidence{
			AccessURLs:        []string{objectURL},
			Buckets:           UniqueStrings([]string{item.Bucket}),
			Keys:              UniqueStrings([]string{item.Key}),
			StorageOperations: []string{model.StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}
