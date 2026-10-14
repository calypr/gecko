package git

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func accessURLsForStorage(record projectRecordState) []string {
	if len(record.CanonicalAccessURLs) > 0 {
		return record.CanonicalAccessURLs
	}
	return record.AccessURLs
}

func probeAccessURLsForRecord(record projectRecordState) []string {
	urls := make([]string, 0, len(record.CanonicalAccessURLs)+len(record.AccessURLs)+len(record.AccessProbes))
	if len(record.CanonicalAccessURLs) > 0 {
		urls = append(urls, record.CanonicalAccessURLs...)
	} else {
		urls = append(urls, rawAccessURLsForRecord(record)...)
	}
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) != StorageChainValidationModeInventory || strings.TrimSpace(probe.Status) != "unknown" {
			continue
		}
		if objectURL := syfonProbeObjectURL(probe); objectURL != "" {
			urls = append(urls, objectURL)
		}
	}
	return uniqueStrings(urls)
}

func rawAccessURLsForRecord(record projectRecordState) []string {
	out := make([]string, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return uniqueStrings(out)
}

func brokenBucketMappingRepairPlan(records []projectRecordState) ([]string, map[string][]gintegrationsyfon.ProjectAccessMethod) {
	deleteIDs := make([]string, 0)
	updateAccessMethods := make(map[string][]gintegrationsyfon.ProjectAccessMethod)
	for _, record := range records {
		remainingMethods, shouldDelete, ok := repairBrokenBucketMappingRecord(record)
		if !ok {
			continue
		}
		if shouldDelete {
			deleteIDs = append(deleteIDs, record.ObjectID)
			continue
		}
		updateAccessMethods[record.ObjectID] = remainingMethods
	}
	return uniqueStrings(deleteIDs), updateAccessMethods
}

func repairBrokenBucketMappingRecord(record projectRecordState) ([]gintegrationsyfon.ProjectAccessMethod, bool, bool) {
	if len(record.AccessMethods) == 0 || len(record.AccessProbes) == 0 {
		return nil, false, false
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probesByURL[strings.TrimSpace(probe.ObjectURL)] = append(probesByURL[strings.TrimSpace(probe.ObjectURL)], probe)
	}
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(record.AccessMethods))
	removedAny := false
	for _, method := range record.AccessMethods {
		if !accessURLHasBrokenBucketMapping(method.URL, probesByURL) {
			remaining = append(remaining, method)
			continue
		}
		removedAny = true
	}
	if !removedAny {
		return nil, false, false
	}
	auditRecord := cleanupRecordAuditForRecord("", "access_url", record)
	remaining = appendReplacementAccessMethods(remaining, auditRecord.AccessProbes, brokenAccessURLsForRecord(auditRecord))
	if len(remaining) == 0 {
		return nil, true, true
	}
	return remaining, false, true
}

func accessURLHasBrokenBucketMapping(accessURL string, probesByURL map[string][]gintegrationsyfon.BulkStorageProbeResult) bool {
	probes := probesByURL[strings.TrimSpace(accessURL)]
	if len(probes) == 0 {
		return false
	}
	hasBrokenBucketMapping := false
	for _, probe := range probes {
		if strings.TrimSpace(probe.Status) == "present" {
			return false
		}
		if syfonProbeIsBrokenAccess(probe) {
			hasBrokenBucketMapping = true
		}
	}
	return hasBrokenBucketMapping
}

func repairableBrokenAccessRecord(record projectRecordState) projectRecordState {
	probes := repairableBrokenAccessProbes(record)
	if len(probes) == 0 {
		return record
	}
	clone := record
	clone.AccessProbes = probes
	return clone
}

func repairableBrokenAccessProbes(record projectRecordState) []gintegrationsyfon.BulkStorageProbeResult {
	if len(record.AccessProbes) == 0 {
		return nil
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		objectURL := syfonProbeObjectURL(probe)
		if objectURL == "" {
			continue
		}
		probesByURL[objectURL] = append(probesByURL[objectURL], probe)
	}
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0)
	for _, accessURL := range rawAccessURLsForRecord(record) {
		rawURL := strings.TrimSpace(accessURL)
		if rawURL == "" {
			continue
		}
		probeURLs := []string{rawURL}
		if mappedURL := strings.TrimSpace(record.CanonicalAccessURLByRaw[rawURL]); mappedURL != "" && mappedURL != rawURL {
			probeURLs = append(probeURLs, mappedURL)
		}
		hasPresentProbe := false
		for _, probeURL := range probeURLs {
			if accessURLHasPresentProbe(probeURL, probesByURL) {
				hasPresentProbe = true
				break
			}
		}
		if hasPresentProbe {
			continue
		}
		for _, probeURL := range probeURLs {
			for _, probe := range probesByURL[probeURL] {
				if syfonProbeIsBrokenAccess(probe) {
					probe.ObjectURL = rawURL
					out = append(out, probe)
				}
			}
		}
	}
	return out
}

func recordHasUsableAccessProbe(record projectRecordState) bool {
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		if objectURL := syfonProbeObjectURL(probe); objectURL != "" {
			probesByURL[objectURL] = append(probesByURL[objectURL], probe)
		}
	}
	for _, accessURL := range rawAccessURLsForRecord(record) {
		rawURL := strings.TrimSpace(accessURL)
		if accessURLHasPresentProbe(rawURL, probesByURL) {
			return true
		}
		mappedURL := strings.TrimSpace(record.CanonicalAccessURLByRaw[rawURL])
		if mappedURL != "" && accessURLHasPresentProbe(mappedURL, probesByURL) {
			return true
		}
	}
	return false
}

func accessURLHasPresentProbe(accessURL string, probesByURL map[string][]gintegrationsyfon.BulkStorageProbeResult) bool {
	for _, probe := range probesByURL[strings.TrimSpace(accessURL)] {
		if strings.TrimSpace(probe.Status) != "present" {
			continue
		}
		switch strings.TrimSpace(probe.ValidationStatus) {
		case "", "not_requested", "matched":
			return true
		}
	}
	return false
}

func syfonProbeObjectURL(probe gintegrationsyfon.BulkStorageProbeResult) string {
	if objectURL := strings.TrimSpace(probe.ObjectURL); objectURL != "" {
		return objectURL
	}
	return canonicalStorageURL(probe.Bucket, probe.Key, "")
}

func syfonProbeIsBrokenAccess(probe gintegrationsyfon.BulkStorageProbeResult) bool {
	switch strings.TrimSpace(probe.ErrorKind) {
	case "missing_access_url", "scope_not_found", "credential_missing":
		return true
	}
	return strings.TrimSpace(probe.Operation) == StorageChainValidationModeMetadata && strings.TrimSpace(probe.Status) == "not_found"
}

func syfonProbeHasMismatch(probe gintegrationsyfon.BulkStorageProbeResult, mismatch string) bool {
	for _, item := range probe.ValidationMismatches {
		if strings.TrimSpace(item) == mismatch {
			return true
		}
	}
	return false
}

func canonicalizeRecordAccessURLs(accessURLs []string, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(accessURLs))
	for _, accessURL := range accessURLs {
		if objectURL := canonicalizeScopedStorageURL(accessURL, scopes, organization, project); objectURL != "" {
			out = append(out, objectURL)
			continue
		}
		if objectURL := canonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	return uniqueStrings(out)
}

func canonicalizeRecordAccessURLMappings(accessURLs []string, scopes []domain.StorageBucketScope, organization string, project string) map[string]string {
	out := make(map[string]string, len(accessURLs))
	for _, accessURL := range accessURLs {
		rawURL := strings.TrimSpace(accessURL)
		if rawURL == "" {
			continue
		}
		if objectURL := canonicalizeScopedStorageURL(rawURL, scopes, organization, project); objectURL != "" {
			out[rawURL] = objectURL
			continue
		}
		if objectURL := canonicalStorageURL("", "", rawURL); objectURL != "" {
			out[rawURL] = objectURL
		}
	}
	return out
}

func canonicalizeScopedStorageURL(accessURL string, scopes []domain.StorageBucketScope, organization string, project string) string {
	if len(scopes) == 0 {
		return ""
	}
	_, key, ok := parseStorageURL(accessURL)
	if !ok {
		return ""
	}
	targetBucket := ""
	prefixes := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if !storageScopeApplies(scope, organization, project) {
			continue
		}
		scopeBucket, scopePrefix, ok := parseStorageScopePath(scope)
		if !ok {
			continue
		}
		if strings.TrimSpace(scopeBucket) != "" {
			targetBucket = strings.TrimSpace(scopeBucket)
		}
		if strings.TrimSpace(scopePrefix) != "" {
			prefixes = append(prefixes, strings.Trim(strings.TrimSpace(scopePrefix), "/"))
		}
	}
	if targetBucket == "" {
		return ""
	}
	normalizedKey := normalizeScopedStorageKeyForGecko(key, prefixes)
	if normalizedKey == "" {
		return ""
	}
	return canonicalStorageURL(targetBucket, normalizedKey, "")
}

func storageScopeApplies(scope domain.StorageBucketScope, organization string, project string) bool {
	scopeOrg := strings.TrimSpace(scope.Organization)
	if scopeOrg != "" && !strings.EqualFold(scopeOrg, strings.TrimSpace(organization)) {
		return false
	}
	scopeProject := strings.TrimSpace(scope.ProjectID)
	return scopeProject == "" || strings.EqualFold(scopeProject, strings.TrimSpace(project))
}

func parseStorageScopePath(scope domain.StorageBucketScope) (string, string, bool) {
	bucket := strings.TrimSpace(scope.Bucket)
	pathValue := strings.TrimSpace(scope.Path)
	if pathValue == "" {
		return bucket, "", bucket != ""
	}
	if strings.HasPrefix(strings.ToLower(pathValue), "s3://") {
		parsedBucket, parsedPrefix, ok := parseStorageURLAllowRoot(pathValue)
		if !ok {
			return bucket, "", bucket != ""
		}
		if bucket == "" {
			bucket = parsedBucket
		}
		return bucket, parsedPrefix, bucket != ""
	}
	return bucket, strings.Trim(pathValue, "/"), bucket != ""
}

func parseStorageURL(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	rest = strings.TrimLeft(rest, "/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	bucket := strings.TrimSpace(parts[0])
	key := strings.Trim(strings.TrimSpace(parts[1]), "/")
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}

func parseStorageURLAllowRoot(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	rest = strings.TrimLeft(rest, "/")
	parts := strings.SplitN(rest, "/", 2)
	bucket := strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return bucket, "", true
	}
	return bucket, strings.Trim(strings.TrimSpace(parts[1]), "/"), true
}

func normalizeScopedStorageKeyForGecko(key string, prefixes []string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	normalizedPrefixes := normalizedScopePrefixesForGecko(prefixes)
	remainder := key
	for _, prefix := range normalizedPrefixes {
		remainder = trimLeadingStoragePrefixForGecko(remainder, prefix)
	}
	composedPrefix := strings.Join(normalizedPrefixes, "/")
	switch {
	case composedPrefix == "":
		return remainder
	case remainder == "":
		return composedPrefix
	default:
		return path.Join(composedPrefix, remainder)
	}
}

func normalizedScopePrefixesForGecko(prefixes []string) []string {
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if prefix == "" {
			continue
		}
		if len(out) == 0 {
			out = append(out, prefix)
			continue
		}
		last := out[len(out)-1]
		switch {
		case prefix == last:
			continue
		case strings.HasPrefix(prefix, last+"/"):
			out[len(out)-1] = prefix
		case strings.HasPrefix(last, prefix+"/"):
			continue
		default:
			out = append(out, prefix)
		}
	}
	return out
}

func trimLeadingStoragePrefixForGecko(key, prefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if key == "" || prefix == "" {
		return key
	}
	if key == prefix {
		return ""
	}
	if strings.HasPrefix(key, prefix+"/") {
		return strings.TrimPrefix(key, prefix+"/")
	}
	return key
}

func canonicalStorageURL(bucket string, key string, objectURL string) string {
	cleanBucket := strings.TrimSpace(bucket)
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	if cleanBucket != "" && cleanKey != "" {
		return "s3://" + cleanBucket + "/" + cleanKey
	}
	trimmed := strings.TrimSpace(objectURL)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return ""
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	if strings.HasPrefix(rest, "/") {
		rest = strings.TrimLeft(rest, "/")
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	return "s3://" + strings.TrimSpace(parts[0]) + "/" + strings.Trim(strings.TrimSpace(parts[1]), "/")
}

func normalizeAnalyticsChecksum(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "sha256:")
	trimmed = strings.TrimPrefix(trimmed, "SHA256:")
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

func applyUsage(agg *storageAggregate, record projectRecordState) {
	agg.downloadCount += record.Usage.DownloadCount
	agg.lastDownload = laterTime(agg.lastDownload, record.Usage.LastDownloadTime)
	agg.latestUpdate = laterTime(agg.latestUpdate, record.UpdatedAt)
}

func aggregateMatchedSize(matches []projectRecordState, fallback int64) int64 {
	var total int64
	for _, match := range matches {
		if match.Size > 0 {
			total += match.Size
		}
	}
	if total == 0 {
		return fallback
	}
	return total
}

func aggregateMatchedDownloads(matches []projectRecordState) int64 {
	var total int64
	for _, match := range matches {
		total += match.Usage.DownloadCount
	}
	return total
}

func latestMatchedDownload(matches []projectRecordState) *time.Time {
	var latest *time.Time
	for _, match := range matches {
		latest = laterTime(latest, match.Usage.LastDownloadTime)
	}
	return latest
}

func laterTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		copyTime := candidate.UTC()
		return &copyTime
	}
	return current
}

func immediateChild(root string, repoPath string) (string, string, string) {
	normalizedPath := normalizeRepoSubpath(repoPath)
	if root != "" {
		prefix := root + "/"
		if !strings.HasPrefix(normalizedPath, prefix) {
			return "", "", ""
		}
		normalizedPath = strings.TrimPrefix(normalizedPath, prefix)
	}
	if normalizedPath == "" {
		return "", "", ""
	}
	parts := strings.Split(normalizedPath, "/")
	childName := parts[0]
	if len(parts) == 1 {
		if root == "" {
			return childName, childName, "file"
		}
		return childName, root + "/" + childName, "file"
	}
	if root == "" {
		return childName, childName, "directory"
	}
	return childName, root + "/" + childName, "directory"
}

func normalizeRepoSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func recordObjectIDs(matches []projectRecordState) []string {
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		if trimmed := strings.TrimSpace(match.ObjectID); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return uniqueStrings(out)
}

func buildFindingEvidence(checksum string, sourcePaths []string, matches []projectRecordState, bucketEvaluation string) *GitAuditEvidence {
	evidence := &GitAuditEvidence{
		Checksum:          strings.TrimSpace(checksum),
		SourcePaths:       uniqueStrings(sourcePaths),
		ObjectIDs:         []string{},
		AccessURLs:        []string{},
		BucketObjectURLs:  []string{},
		Buckets:           []string{},
		Keys:              []string{},
		StorageOperations: []string{},
		ProbeStatuses:     []string{},
		ValidationStates:  []string{},
		ErrorKinds:        []string{},
		Errors:            []string{},
		BucketEvaluation:  strings.TrimSpace(bucketEvaluation),
	}
	for _, match := range matches {
		if objectID := strings.TrimSpace(match.ObjectID); objectID != "" {
			evidence.ObjectIDs = append(evidence.ObjectIDs, objectID)
		}
		evidence.AccessURLs = append(evidence.AccessURLs, match.AccessURLs...)
		if len(match.AccessProbes) == 0 {
			continue
		}
		for _, probe := range match.AccessProbes {
			if objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
				evidence.BucketObjectURLs = append(evidence.BucketObjectURLs, objectURL)
			}
			if bucket := strings.TrimSpace(probe.Bucket); bucket != "" {
				evidence.Buckets = append(evidence.Buckets, bucket)
			}
			if key := strings.TrimSpace(probe.Key); key != "" {
				evidence.Keys = append(evidence.Keys, key)
			}
			if operation := strings.TrimSpace(probe.Operation); operation != "" {
				evidence.StorageOperations = append(evidence.StorageOperations, operation)
			}
			if status := strings.TrimSpace(probe.Status); status != "" {
				evidence.ProbeStatuses = append(evidence.ProbeStatuses, status)
			}
			if validation := strings.TrimSpace(probe.ValidationStatus); validation != "" {
				evidence.ValidationStates = append(evidence.ValidationStates, validation)
			}
			if kind := strings.TrimSpace(probe.ErrorKind); kind != "" {
				evidence.ErrorKinds = append(evidence.ErrorKinds, kind)
			}
			if err := strings.TrimSpace(probe.Error); err != "" {
				evidence.Errors = append(evidence.Errors, err)
			}
		}
	}
	evidence.ObjectIDs = uniqueStrings(evidence.ObjectIDs)
	evidence.AccessURLs = uniqueStrings(evidence.AccessURLs)
	evidence.BucketObjectURLs = uniqueStrings(evidence.BucketObjectURLs)
	evidence.Buckets = uniqueStrings(evidence.Buckets)
	evidence.Keys = uniqueStrings(evidence.Keys)
	evidence.StorageOperations = uniqueStrings(evidence.StorageOperations)
	evidence.ProbeStatuses = uniqueStrings(evidence.ProbeStatuses)
	evidence.ValidationStates = uniqueStrings(evidence.ValidationStates)
	evidence.ErrorKinds = uniqueStrings(evidence.ErrorKinds)
	evidence.Errors = uniqueStrings(evidence.Errors)
	if evidence.Checksum == "" &&
		len(evidence.SourcePaths) == 0 &&
		len(evidence.ObjectIDs) == 0 &&
		len(evidence.AccessURLs) == 0 &&
		len(evidence.BucketObjectURLs) == 0 &&
		len(evidence.Buckets) == 0 &&
		len(evidence.Keys) == 0 &&
		len(evidence.StorageOperations) == 0 &&
		len(evidence.ProbeStatuses) == 0 &&
		len(evidence.ValidationStates) == 0 &&
		len(evidence.ErrorKinds) == 0 &&
		len(evidence.Errors) == 0 &&
		evidence.BucketEvaluation == "" {
		return nil
	}
	return evidence
}

func flattenRecordStates(recordsByChecksum map[string][]projectRecordState) []projectRecordState {
	out := make([]projectRecordState, 0)
	seen := map[string]struct{}{}
	for _, group := range recordsByChecksum {
		for _, record := range group {
			objectID := strings.TrimSpace(record.ObjectID)
			if objectID == "" {
				continue
			}
			if _, ok := seen[objectID]; ok {
				continue
			}
			seen[objectID] = struct{}{}
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObjectID < out[j].ObjectID
	})
	return out
}

func chainPathCount(gitPaths []string) int {
	if len(gitPaths) == 0 {
		return 1
	}
	return len(gitPaths)
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func storageProbeRequestKey(objectURL string, size int64, checksum string) string {
	return strings.TrimSpace(objectURL) + "|" + fmt.Sprintf("%d", size) + "|" + strings.TrimSpace(checksum)
}

func storageListValidationRequestKey(objectURL string, size int64, expectedName string) string {
	return strings.TrimSpace(objectURL) + "|" + fmt.Sprintf("%d", size) + "|" + strings.TrimSpace(expectedName)
}

func expectedStorageObjectNameForListValidation(objectURL string, recordName string) string {
	expectedName := path.Base(strings.Trim(strings.TrimSpace(recordName), "/"))
	if expectedName == "." || expectedName == "/" || expectedName == "" {
		return ""
	}
	return expectedName
}

func sortStorageAggregates(items []storageAggregate, sortBy string, sortOrder string) {
	desc := !strings.EqualFold(strings.TrimSpace(sortOrder), "asc")
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		sort.Slice(items, func(i, j int) bool {
			left := strings.ToLower(items[i].name)
			right := strings.ToLower(items[j].name)
			if desc {
				return left > right
			}
			return left < right
		})
	default:
		sort.Slice(items, func(i, j int) bool {
			if items[i].totalBytes != items[j].totalBytes {
				if desc {
					return items[i].totalBytes > items[j].totalBytes
				}
				return items[i].totalBytes < items[j].totalBytes
			}
			if items[i].rowType != items[j].rowType {
				return items[i].rowType == "directory"
			}
			return strings.ToLower(items[i].name) < strings.ToLower(items[j].name)
		})
	}
}

func orphanDisplayPath(checksum string, sourcePaths []string) string {
	if len(sourcePaths) > 0 && strings.TrimSpace(sourcePaths[0]) != "" {
		return strings.TrimSpace(sourcePaths[0])
	}
	return "sha256/" + strings.TrimSpace(checksum)
}

func syfonRecordDisplayPath(record projectRecordState) string {
	if name := strings.TrimSpace(record.Name); name != "" {
		return name
	}
	if objectID := strings.TrimSpace(record.ObjectID); objectID != "" {
		return "syfon/" + objectID
	}
	if checksum := strings.TrimSpace(record.Checksum); checksum != "" {
		return "sha256/" + checksum
	}
	return ""
}

func recordSourcePaths(matches []projectRecordState) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, match := range matches {
		for _, accessURL := range match.AccessURLs {
			normalized := strings.TrimSpace(accessURL)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			out = append(out, normalized)
		}
	}
	sort.Strings(out)
	return out
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func boolPtr(value bool) *bool {
	return &value
}

func differenceStrings(values []string, toRemove []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	removeSet := make(map[string]struct{}, len(toRemove))
	for _, value := range toRemove {
		removeSet[value] = struct{}{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}
