package git

import (
	"fmt"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func normalizeCleanupSelectionKey(raw string) string {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return ""
	}
	if objectURL := canonicalStorageURL("", "", trimmed); objectURL != "" {
		return objectURL
	}
	return trimmed
}

func cleanupSelectionCandidatesForRecords(checksum string, displayPath string, matches []projectRecordState) []string {
	candidates := []string{displayPath}
	normalizedChecksum := normalizeAnalyticsChecksum(checksum)
	if normalizedChecksum != "" {
		candidates = append(candidates, normalizedChecksum, "sha256/"+normalizedChecksum)
	}
	for _, match := range matches {
		candidates = append(candidates, match.ObjectID)
		candidates = append(candidates, match.AccessURLs...)
		candidates = append(candidates, match.CanonicalAccessURLs...)
		candidates = append(candidates, recordStorageCandidateURLs(match)...)
		for _, probe := range match.AccessProbes {
			candidates = append(candidates, probe.ObjectURL)
			candidates = append(candidates, canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL))
		}
	}
	return uniqueCleanupSelectionCandidates(candidates)
}

func cleanupSelectionCandidatesForBucketObject(objectURL string, item gintegrationsyfon.ProjectBucketObject) []string {
	return uniqueCleanupSelectionCandidates([]string{
		objectURL,
		item.ObjectURL,
		canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL),
		item.Key,
	})
}

func uniqueCleanupSelectionCandidates(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeCleanupSelectionKey(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func bucketObjectHasCompleteChain(matches []projectRecordState, repoPathsByChecksum map[string][]string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if classifyStorageFinding(match, bucketObjectsByURL) != storageFindingNone {
			continue
		}
		if len(uniqueStrings(repoPathsByChecksum[normalizeAnalyticsChecksum(match.Checksum)])) > 0 {
			return true
		}
	}
	return false
}

func buildCleanupFinding(kind string, normalizedPath string, matches []projectRecordState, repoDeleteCandidate bool, cleanupScope string, action string) GitStorageCleanupFinding {
	records := make([]GitStorageCleanupRecordAudit, 0, len(matches))
	var latestUpdate *time.Time
	var latestDownload *time.Time
	var totalBytes int64
	var totalDownloads int64
	checksum := ""
	for _, match := range matches {
		if checksum == "" {
			checksum = strings.TrimSpace(match.Checksum)
		}
		records = append(records, cleanupRecordAuditForRecord(normalizedPath, cleanupScope, match))
		totalBytes += match.Size
		totalDownloads += match.Usage.DownloadCount
		latestUpdate = laterTime(latestUpdate, match.UpdatedAt)
		latestDownload = laterTime(latestDownload, match.Usage.LastDownloadTime)
	}
	bucketEvaluation := "not_checked"
	if cleanupScope == "access_url" {
		bucketEvaluation = "probed"
	}
	actionability, availableActions, defaultAction, supportsDryRun := storageCleanupActionSupport(kind)
	return GitStorageCleanupFinding{
		Kind:                kind,
		NormalizedPath:      normalizedPath,
		Checksum:            checksum,
		ObjectIDs:           recordObjectIDs(matches),
		Records:             records,
		RecommendedAction:   action,
		RepoDeleteCandidate: repoDeleteCandidate,
		CleanupScope:        cleanupScope,
		SizeBytes:           totalBytes,
		LastUpdated:         formatOptionalTime(latestUpdate),
		DownloadCount:       totalDownloads,
		LastDownload:        formatOptionalTime(latestDownload),
		Actionability:       actionability,
		AvailableActions:    availableActions,
		DefaultAction:       defaultAction,
		SupportsDryRun:      supportsDryRun,
		Evidence:            buildFindingEvidence(checksum, nil, matches, bucketEvaluation),
	}
}

func cleanupRecordAuditForRecord(normalizedPath string, cleanupScope string, record projectRecordState) GitStorageCleanupRecordAudit {
	return GitStorageCleanupRecordAudit{
		ObjectID:       record.ObjectID,
		Checksum:       strings.TrimSpace(record.Checksum),
		NormalizedPath: normalizedPath,
		CleanupScope:   cleanupScope,
		AccessURLs:     uniqueStrings(record.AccessURLs),
		AccessMethods:  cleanupAccessMethodsFromProjectMethods(record.AccessMethods),
		AccessProbes:   accessProbesForRecord(record),
		Status:         accessStatusForRecord(record),
		Error:          accessErrorForRecord(record),
		SizeBytes:      record.Size,
		LastUpdated:    formatOptionalTime(record.UpdatedAt),
		DownloadCount:  record.Usage.DownloadCount,
		LastDownload:   formatOptionalTime(record.Usage.LastDownloadTime),
	}
}

func cleanupAccessMethodsFromProjectMethods(methods []gintegrationsyfon.ProjectAccessMethod) []GitStorageCleanupAccessMethod {
	out := make([]GitStorageCleanupAccessMethod, 0, len(methods))
	for _, method := range methods {
		out = append(out, GitStorageCleanupAccessMethod{
			AccessID: strings.TrimSpace(method.AccessID),
			Type:     strings.TrimSpace(method.Type),
			URL:      strings.TrimSpace(method.URL),
			Headers:  append([]string(nil), method.Headers...),
		})
	}
	return out
}

func projectAccessMethodsFromCleanupMethods(methods []GitStorageCleanupAccessMethod) []gintegrationsyfon.ProjectAccessMethod {
	out := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(methods))
	for _, method := range methods {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		out = append(out, gintegrationsyfon.ProjectAccessMethod{
			AccessID: strings.TrimSpace(method.AccessID),
			Type:     strings.TrimSpace(method.Type),
			URL:      url,
			Headers:  append([]string(nil), method.Headers...),
		})
	}
	return out
}

func repoOrphanAction(kind string) string {
	if kind == "repo_orphan_live_object" {
		return "Delete Syfon record and purge storage object"
	}
	return "Delete stale Syfon record"
}

func storageCleanupActionSupport(kind string) (string, []string, string, bool) {
	policy := storageRepairPolicyForKind(kind)
	return policy.actionability, append([]string(nil), policy.actions...), policy.defaultAction, policy.supportsDryRun
}

func compareRecordState(left projectRecordState, right projectRecordState) int {
	if left.Usage.DownloadCount != right.Usage.DownloadCount {
		if left.Usage.DownloadCount > right.Usage.DownloadCount {
			return 1
		}
		return -1
	}
	if compareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime) != 0 {
		return compareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime)
	}
	return compareOptionalTime(left.UpdatedAt, right.UpdatedAt)
}

func compareOptionalTime(left *time.Time, right *time.Time) int {
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

func recordHasBrokenAccess(record projectRecordState) bool {
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

func recordsContainLiveUsage(matches []projectRecordState) bool {
	for _, match := range matches {
		if match.Usage.DownloadCount > 0 || match.Usage.UploadCount > 0 || match.Usage.LastAccessTime != nil || match.Usage.LastDownloadTime != nil || match.Usage.LastUploadTime != nil {
			return true
		}
	}
	return false
}

func classifyStorageFinding(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) storageFindingKind {
	if recordHasBrokenAccess(record) {
		return storageFindingBrokenAccessURL
	}
	resolution := resolveRecordStorage(record, bucketObjectsByURL)
	if hasExactPathBucketMismatch(record, resolution, bucketObjectsByURL) {
		return storageFindingBrokenBucketMap
	}
	assessment := assessStorageRecordEvidence(record, len(resolution.matchedBucketObjectURLs) > 0)
	if len(repairableBrokenAccessProbes(record)) > 0 && recordHasUsableAccessProbe(record) {
		return storageFindingBrokenBucketMap
	}
	if resolution.hasAcceptedCanonicalProbe {
		return storageFindingNone
	}
	if inventoryHasValidationMismatch(record, resolution.matchedBucketObjectURLs, bucketObjectsByURL) {
		return storageFindingValidationMismatch
	}
	if resolution.hasValidationMismatch || assessment.MetadataMismatch {
		return storageFindingValidationMismatch
	}
	if resolution.hasAcceptedCanonicalProbe || assessment.Present {
		return storageFindingNone
	}
	if assessment.MappingBroken {
		return storageFindingBrokenBucketMap
	}
	if len(record.AccessProbes) == 0 {
		if len(bucketObjectsByURL) > 0 && len(resolution.candidateURLs) > 0 {
			return storageFindingProbeError
		}
		return storageFindingNone
	}
	if assessment.Missing {
		return storageFindingObjectMissing
	}
	// A metadata miss is definitive for cleanup and chain classification when
	// the backend did not attach a more specific error kind.
	if hasNotFoundMetadataProbe(record) {
		return storageFindingObjectMissing
	}
	if hasNotFoundListProbe(record) && len(bucketObjectsByURL) > 0 {
		return storageFindingObjectMissing
	}
	if resolution.hasProbeError || assessment.Status == "unknown" {
		return storageFindingProbeError
	}
	return storageFindingNone
}

func hasNotFoundMetadataProbe(record projectRecordState) bool {
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) == StorageChainValidationModeMetadata && strings.TrimSpace(probe.Status) == "not_found" {
			return true
		}
	}
	return false
}

func hasNotFoundListProbe(record projectRecordState) bool {
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) == StorageChainValidationModeList && strings.TrimSpace(probe.Status) == "not_found" {
			return true
		}
	}
	return false
}

func hasExactPathBucketMismatch(record projectRecordState, resolution recordStorageResolution, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	// A basename or size match elsewhere in a project bucket is not evidence
	// that this record's mapping is wrong: BForePC contains many repeated names
	// such as features.tsv.gz. Mapping failures must come from an explicit
	// resolver/probe error, not an inventory-side similarity heuristic.
	_ = record
	_ = resolution
	_ = bucketObjectsByURL
	return false
}

func storageProbeValidationMismatchIsSignificant(record projectRecordState, probe gintegrationsyfon.BulkStorageProbeResult) bool {
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
	if !syfonProbeHasMismatch(probe, "size_mismatch") || significantMismatches != 1 {
		return true
	}
	if probe.SizeBytes == nil {
		return true
	}
	return !storageSizesMatchForAudit(record.Size, *probe.SizeBytes)
}

func inventoryHasValidationMismatch(record projectRecordState, bucketObjectURLs []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(bucketObjectURLs) == 0 {
		return false
	}
	for _, objectURL := range bucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if !storageSizesMatchForAudit(record.Size, item.SizeBytes) {
			return true
		}
	}
	return false
}

func storageSizesMatchForAudit(expectedSize int64, observedSize int64) bool {
	if expectedSize <= 0 || observedSize <= 0 {
		return true
	}
	diff := expectedSize - observedSize
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1
}

func accessProbesForRecord(record projectRecordState) []GitStorageCleanupAccessProbe {
	if len(record.AccessProbes) > 0 {
		probes := make([]GitStorageCleanupAccessProbe, 0, len(record.AccessProbes))
		for _, probe := range record.AccessProbes {
			exists := probe.Exists
			probes = append(probes, GitStorageCleanupAccessProbe{
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
		return []GitStorageCleanupAccessProbe{{
			URL:       "",
			Status:    "missing",
			Error:     "no access URLs present",
			ErrorKind: "missing_access_url",
		}}
	}
	probes := make([]GitStorageCleanupAccessProbe, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) == "" {
			probes = append(probes, GitStorageCleanupAccessProbe{
				URL:       accessURL,
				Status:    "missing",
				Error:     "blank access URL",
				ErrorKind: "missing_access_url",
			})
			continue
		}
		probes = append(probes, GitStorageCleanupAccessProbe{
			URL:    accessURL,
			Status: "present",
		})
	}
	return probes
}

func accessStatusForRecord(record projectRecordState) string {
	switch classifyStorageFinding(record, nil) {
	case storageFindingBrokenAccessURL, storageFindingObjectMissing:
		return "missing"
	case storageFindingBrokenBucketMap, storageFindingProbeError:
		return "error"
	case storageFindingValidationMismatch:
		return "mismatched"
	}
	return "present"
}

func accessErrorForRecord(record projectRecordState) string {
	switch classifyStorageFinding(record, nil) {
	case storageFindingBrokenAccessURL:
		return "no usable access URL present"
	case storageFindingBrokenBucketMap:
		return "Syfon access URL did not resolve through a configured bucket mapping"
	case storageFindingObjectMissing:
		return "storage object not found"
	case storageFindingValidationMismatch:
		return "mapped bucket object exists, but object evidence disagrees with the Syfon/Git record"
	case storageFindingProbeError:
		return "storage probe failed"
	}
	return ""
}

func buildBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) GitStorageCleanupFinding {
	objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := storageCleanupActionSupport("bucket_only_object")
	return GitStorageCleanupFinding{
		Kind:                "bucket_only_object",
		NormalizedPath:      objectURL,
		ObjectIDs:           []string{},
		Records:             []GitStorageCleanupRecordAudit{},
		RecommendedAction:   "Review and delete bucket object that has no Syfon record",
		RepoDeleteCandidate: false,
		CleanupScope:        "bucket_object",
		SizeBytes:           item.SizeBytes,
		LastUpdated:         strings.TrimSpace(item.LastModified),
		Actionability:       actionability,
		AvailableActions:    availableActions,
		DefaultAction:       defaultAction,
		SupportsDryRun:      supportsDryRun,
		Evidence: &GitAuditEvidence{
			AccessURLs:        []string{objectURL},
			Buckets:           uniqueStrings([]string{item.Bucket}),
			Keys:              uniqueStrings([]string{item.Key}),
			StorageOperations: []string{StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}

func buildChainBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) GitStorageChainFinding {
	objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("bucket_only_object", "verified")
	return GitStorageChainFinding{
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
		Evidence: &GitAuditEvidence{
			AccessURLs:        []string{objectURL},
			BucketObjectURLs:  []string{objectURL},
			Buckets:           uniqueStrings([]string{item.Bucket}),
			Keys:              uniqueStrings([]string{item.Key}),
			StorageOperations: []string{StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}

func buildChainRecordFindings(kind string, record projectRecordState, gitPaths []string, bucketObjectURLs []string, action string) []GitStorageChainFinding {
	return buildChainRecordFindingsWithOptions(kind, record, gitPaths, bucketObjectURLs, action, false)
}

func buildChainRecordFindingsWithOptions(kind string, record projectRecordState, gitPaths []string, bucketObjectURLs []string, action string, preferSyfonRecordPath bool) []GitStorageChainFinding {
	paths := uniqueStrings(gitPaths)
	if len(paths) == 0 {
		displayPath := orphanDisplayPath(strings.TrimSpace(record.Checksum), append(bucketObjectURLs, record.AccessURLs...))
		if preferSyfonRecordPath {
			displayPath = syfonRecordDisplayPath(record)
		}
		if displayPath == "" {
			displayPath = strings.TrimSpace(record.Checksum)
		}
		paths = []string{displayPath}
	}
	objectIDs := uniqueStrings([]string{record.ObjectID})
	accessURLs := uniqueStrings(record.AccessURLs)
	evidence := buildFindingEvidence(strings.TrimSpace(record.Checksum), gitPaths, []projectRecordState{record}, "enumerated_and_probed")
	if evidence != nil {
		evidence.BucketObjectURLs = uniqueStrings(append(evidence.BucketObjectURLs, bucketObjectURLs...))
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
	findings := make([]GitStorageChainFinding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, GitStorageChainFinding{
			Kind:              kind,
			EvidenceStatus:    evidenceStatus,
			NormalizedPath:    path,
			Checksum:          strings.TrimSpace(record.Checksum),
			SourcePaths:       uniqueStrings(gitPaths),
			ObjectIDs:         objectIDs,
			Records:           []GitStorageCleanupRecordAudit{cleanupRecordAuditForRecord(path, "access_url", record)},
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

func suggestedActionForChainFinding(kind string, record projectRecordState) string {
	return ""
}

func suggestedFixForChainFinding(kind string, record projectRecordState) string {
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

func firstAccessProbeMismatch(record projectRecordState, mismatch string) (GitStorageCleanupAccessProbe, bool) {
	for _, probe := range accessProbesForRecord(record) {
		if accessProbeHasMismatch(probe, mismatch) {
			return probe, true
		}
	}
	return GitStorageCleanupAccessProbe{}, false
}

func accessProbeHasMismatch(probe GitStorageCleanupAccessProbe, mismatch string) bool {
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
	probe           GitStorageCleanupAccessProbe
}

func selectChainProbe(record projectRecordState, bucketObjectURLs []string) chainProbeSelection {
	probes := accessProbesForRecord(record)
	if len(bucketObjectURLs) > 0 {
		targets := map[string]struct{}{}
		for _, bucketURL := range bucketObjectURLs {
			targets[strings.TrimSpace(bucketURL)] = struct{}{}
		}
		for _, probe := range probes {
			objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if _, ok := targets[objectURL]; ok {
				return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
			}
		}
	}
	for _, probe := range probes {
		operation := strings.TrimSpace(probe.Operation)
		if operation != StorageChainValidationModeMetadata && operation != StorageChainValidationModeList {
			continue
		}
		objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	for _, probe := range probes {
		objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	return chainProbeSelection{}
}

func chainFindingError(kind string, record projectRecordState, probe GitStorageCleanupAccessProbe) string {
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
		return accessErrorForRecord(record)
	default:
		return ""
	}
}

func recordsReferenceBucketObject(matches []projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if len(resolveRecordStorage(match, bucketObjectsByURL).matchedBucketObjectURLs) > 0 {
			return true
		}
	}
	return false
}

func matchedBucketObjectURLs(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	return resolveRecordStorage(record, bucketObjectsByURL).matchedBucketObjectURLs
}

func cleanupMatchedBucketObjectURLs(matches []projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	objectURLs := make([]string, 0)
	for _, match := range matches {
		objectURLs = append(objectURLs, matchedBucketObjectURLs(match, bucketObjectsByURL)...)
		for _, probe := range accessProbesForRecord(match) {
			if strings.TrimSpace(probe.Status) != "present" {
				continue
			}
			objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if strings.TrimSpace(objectURL) == "" {
				continue
			}
			objectURLs = append(objectURLs, objectURL)
		}
	}
	return uniqueStrings(objectURLs)
}

func recordBucketURLs(record projectRecordState) []string {
	return recordStorageCandidateURLs(record)
}

func recordStorageCandidateURLs(record projectRecordState) []string {
	out := make([]string, 0)
	for _, accessURL := range record.CanonicalAccessURLs {
		if objectURL := canonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	for _, probe := range record.AccessProbes {
		if objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	if len(record.CanonicalAccessURLs) == 0 {
		for _, accessURL := range rawAccessURLsForRecord(record) {
			if objectURL := canonicalStorageURL("", "", accessURL); objectURL != "" {
				out = append(out, objectURL)
			}
		}
	}
	return uniqueStrings(out)
}

type recordStorageResolution struct {
	candidateURLs             []string
	matchedBucketObjectURLs   []string
	hasPresentProbe           bool
	hasMissingProbe           bool
	hasBrokenMappingProbe     bool
	hasProbeError             bool
	hasValidationMismatch     bool
	hasAcceptedCanonicalProbe bool
	hasPresentRawAccessProbe  bool
	hasMissingRawAccessProbe  bool
	hasBrokenRawAccessProbe   bool
	hasRawAccessProbeError    bool
}

func resolveRecordStorage(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) recordStorageResolution {
	resolution := recordStorageResolution{
		candidateURLs: recordStorageCandidateURLs(record),
	}
	rawURLs := make(map[string]struct{}, len(record.AccessURLs))
	rawCandidateURLs := make(map[string]struct{}, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
			rawURLs[trimmed] = struct{}{}
			if objectURL := canonicalStorageURL("", "", trimmed); objectURL != "" {
				rawCandidateURLs[objectURL] = struct{}{}
			}
		}
	}
	canonicalURLs := make(map[string]struct{}, len(record.CanonicalAccessURLs))
	for _, accessURL := range record.CanonicalAccessURLs {
		trimmed := strings.TrimSpace(accessURL)
		if trimmed == "" {
			continue
		}
		if _, raw := rawURLs[trimmed]; !raw {
			canonicalURLs[trimmed] = struct{}{}
		}
	}
	for _, probe := range record.AccessProbes {
		objectURL := syfonProbeObjectURL(probe)
		status := strings.TrimSpace(probe.Status)
		_, rawAccessProbe := rawCandidateURLs[objectURL]
		if status == "present" {
			resolution.hasPresentProbe = true
			if rawAccessProbe {
				resolution.hasPresentRawAccessProbe = true
			}
			if _, ok := canonicalURLs[objectURL]; ok {
				switch strings.TrimSpace(probe.ValidationStatus) {
				case "", "not_requested", "matched":
					resolution.hasAcceptedCanonicalProbe = true
				}
			}
		}
		if storageProbeValidationMismatchIsSignificant(record, probe) {
			resolution.hasValidationMismatch = true
		}
		switch strings.TrimSpace(probe.ErrorKind) {
		case "credential_missing":
			resolution.hasBrokenMappingProbe = true
			if rawAccessProbe {
				resolution.hasBrokenRawAccessProbe = true
			}
		}
		switch status {
		case "not_found":
			if strings.TrimSpace(probe.Operation) == StorageChainValidationModeInventory {
				resolution.hasProbeError = true
				if rawAccessProbe {
					resolution.hasRawAccessProbeError = true
				}
				continue
			}
			resolution.hasMissingProbe = true
			if rawAccessProbe {
				resolution.hasMissingRawAccessProbe = true
			}
		case "unknown", "forbidden", "unsupported", "invalid", "error":
			resolution.hasProbeError = true
			if rawAccessProbe {
				resolution.hasRawAccessProbeError = true
			}
		}
	}
	if len(bucketObjectsByURL) > 0 {
		for _, objectURL := range resolution.candidateURLs {
			if _, ok := bucketObjectsByURL[objectURL]; ok {
				resolution.matchedBucketObjectURLs = append(resolution.matchedBucketObjectURLs, objectURL)
			}
		}
		resolution.matchedBucketObjectURLs = uniqueStrings(resolution.matchedBucketObjectURLs)
	}
	return resolution
}
