package git

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

type storageCleanupApplySelection struct {
	selected                map[string]struct{}
	actionSelection         map[string]string
	deleteRepoOrphans       bool
	deleteStaleDuplicates   bool
	deleteBucketOnlyObjects bool
	repairBrokenMappings    bool
}

type brokenAccessURLFamily struct {
	records      []gintegrationsyfon.ProjectRecord
	brokenURLs   map[string]struct{}
	brokenProbes map[string][]GitStorageCleanupAccessProbe
}

type cleanupReconciliationScope struct {
	targetBucket string
	prefix       string
}

func (service *StorageAnalyticsService) reconcileBrokenAccessURLFindings(
	ctx context.Context,
	authorizationHeader string,
	organization string,
	project string,
	scopes []domain.StorageBucketScope,
	findings []GitStorageCleanupApplyFinding,
	selection storageCleanupApplySelection,
) ([]GitStorageCleanupApplyFinding, []string, error) {
	needsRefresh := false
	for _, finding := range findings {
		if len(selection.selected) > 0 && !storageApplyFindingSelected(selection.selected, finding) {
			continue
		}
		action, err := resolveStorageCleanupApplyAction(
			finding,
			selection.actionSelection,
			selection.deleteRepoOrphans,
			selection.deleteStaleDuplicates,
			selection.deleteBucketOnlyObjects,
			selection.repairBrokenMappings,
		)
		if err != nil {
			return nil, nil, err
		}
		if action == storageActionRemoveBrokenAccessURLs {
			needsRefresh = true
		}
	}
	if !needsRefresh {
		return findings, nil, nil
	}

	freshRecords, err := service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, "")
	if err != nil {
		return nil, nil, fmt.Errorf("reload current Syfon project records for access repair: %w", err)
	}
	byChecksum, byObjectID := indexFreshProjectRecords(freshRecords)
	families := make(map[string]*brokenAccessURLFamily)
	findingFamilies := make(map[int][]string)
	for findingIndex, finding := range findings {
		if len(selection.selected) > 0 && !storageApplyFindingSelected(selection.selected, finding) {
			continue
		}
		action, err := resolveStorageCleanupApplyAction(
			finding,
			selection.actionSelection,
			selection.deleteRepoOrphans,
			selection.deleteStaleDuplicates,
			selection.deleteBucketOnlyObjects,
			selection.repairBrokenMappings,
		)
		if err != nil {
			return nil, nil, err
		}
		if action != storageActionRemoveBrokenAccessURLs {
			continue
		}
		staleRecords := accessRepairFindingRecords(finding)
		if len(staleRecords) == 0 {
			return nil, nil, fmt.Errorf("cleanup finding %q is missing records for %s", finding.Kind, storageActionRemoveBrokenAccessURLs)
		}
		seenFamilies := make(map[string]struct{})
		for _, staleRecord := range staleRecords {
			evidence, err := brokenAccessEvidence(staleRecord, scopes, organization, project)
			if err != nil {
				return nil, nil, fmt.Errorf("cleanup finding %q record %q: %w", finding.NormalizedPath, staleRecord.ObjectID, err)
			}
			familyKey, currentRecords, err := currentAccessURLFamily(finding, staleRecord, byChecksum, byObjectID)
			if err != nil {
				return nil, nil, err
			}
			family := families[familyKey]
			if family == nil {
				family = &brokenAccessURLFamily{
					records:      append([]gintegrationsyfon.ProjectRecord(nil), currentRecords...),
					brokenURLs:   make(map[string]struct{}),
					brokenProbes: make(map[string][]GitStorageCleanupAccessProbe),
				}
				families[familyKey] = family
			}
			for brokenURL := range evidence.brokenURLs {
				family.brokenURLs[brokenURL] = struct{}{}
			}
			for brokenURL, probes := range evidence.brokenProbes {
				family.brokenProbes[brokenURL] = append(family.brokenProbes[brokenURL], probes...)
			}
			if _, seen := seenFamilies[familyKey]; !seen {
				findingFamilies[findingIndex] = append(findingFamilies[findingIndex], familyKey)
				seenFamilies[familyKey] = struct{}{}
			}
		}
	}
	out := make([]GitStorageCleanupApplyFinding, 0, len(findings))
	skippedPaths := make([]string, 0)
	for findingIndex, finding := range findings {
		if len(selection.selected) > 0 && !storageApplyFindingSelected(selection.selected, finding) {
			out = append(out, finding)
			continue
		}
		action, err := resolveStorageCleanupApplyAction(
			finding,
			selection.actionSelection,
			selection.deleteRepoOrphans,
			selection.deleteStaleDuplicates,
			selection.deleteBucketOnlyObjects,
			selection.repairBrokenMappings,
		)
		if err != nil {
			return nil, nil, err
		}
		if action != storageActionRemoveBrokenAccessURLs {
			out = append(out, finding)
			continue
		}

		reconciled, hasRepair, err := reconcileBrokenAccessURLFinding(finding, findingFamilies[findingIndex], families, scopes, organization, project)
		if err != nil {
			return nil, nil, err
		}
		if !hasRepair {
			skippedPaths = append(skippedPaths, accessRepairFindingPath(finding))
			continue
		}
		out = append(out, reconciled)
	}
	return out, uniqueStrings(skippedPaths), nil
}

func indexFreshProjectRecords(records []gintegrationsyfon.ProjectRecord) (map[string][]gintegrationsyfon.ProjectRecord, map[string]gintegrationsyfon.ProjectRecord) {
	byChecksum := make(map[string][]gintegrationsyfon.ProjectRecord)
	byObjectID := make(map[string]gintegrationsyfon.ProjectRecord, len(records))
	for _, record := range records {
		objectID := strings.TrimSpace(record.ObjectID)
		if objectID == "" {
			continue
		}
		record.ObjectID = objectID
		byObjectID[objectID] = record
		if checksum := normalizeAnalyticsChecksum(record.Checksum); checksum != "" {
			byChecksum[checksum] = append(byChecksum[checksum], record)
		}
	}
	for checksum := range byChecksum {
		sort.SliceStable(byChecksum[checksum], func(i, j int) bool {
			return byChecksum[checksum][i].ObjectID < byChecksum[checksum][j].ObjectID
		})
	}
	return byChecksum, byObjectID
}

func reconcileBrokenAccessURLFinding(
	finding GitStorageCleanupApplyFinding,
	familyKeys []string,
	families map[string]*brokenAccessURLFamily,
	scopes []domain.StorageBucketScope,
	organization string,
	project string,
) (GitStorageCleanupApplyFinding, bool, error) {
	updatedByObjectID := make(map[string]GitStorageCleanupRecordAudit)
	for _, familyKey := range familyKeys {
		family := families[familyKey]
		if family == nil {
			return GitStorageCleanupApplyFinding{}, false, fmt.Errorf("cannot reconcile cleanup finding %q: missing current access URL family %q", finding.NormalizedPath, familyKey)
		}
		for _, currentRecord := range family.records {
			fresh := cleanupRecordAuditFromProjectRecord(currentRecord)
			brokenMethods := accessMethodURLsInSet(fresh, family.brokenURLs, scopes, organization, project)
			if len(brokenMethods) == 0 {
				continue
			}
			fresh.AccessProbes = brokenAccessProbesForRecord(brokenMethods, family.brokenProbes, scopes, organization, project)
			updatedByObjectID[fresh.ObjectID] = fresh
		}
	}
	if len(updatedByObjectID) == 0 {
		return GitStorageCleanupApplyFinding{}, false, nil
	}
	objectIDs := make([]string, 0, len(updatedByObjectID))
	for objectID := range updatedByObjectID {
		objectIDs = append(objectIDs, objectID)
	}
	sort.Strings(objectIDs)
	updatedRecords := make([]GitStorageCleanupRecordAudit, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		updatedRecords = append(updatedRecords, updatedByObjectID[objectID])
	}

	reconciled := finding
	reconciled.Records = updatedRecords
	return reconciled, true, nil
}

type brokenAccessEvidenceSet struct {
	brokenURLs   map[string]struct{}
	brokenProbes map[string][]GitStorageCleanupAccessProbe
}

func brokenAccessEvidence(record GitStorageCleanupRecordAudit, scopes []domain.StorageBucketScope, organization string, project string) (brokenAccessEvidenceSet, error) {
	rawBrokenURLs := brokenAccessURLsForRecord(record)
	if len(rawBrokenURLs) == 0 {
		return brokenAccessEvidenceSet{}, fmt.Errorf("missing broken access URL evidence")
	}
	brokenURLs := make(map[string]struct{})
	for brokenURL := range rawBrokenURLs {
		for key := range cleanupReconciliationURLForms(brokenURL, scopes, organization, project) {
			brokenURLs[key] = struct{}{}
		}
	}
	probes := make(map[string][]GitStorageCleanupAccessProbe)
	for _, probe := range record.AccessProbes {
		key := normalizeCleanupSelectionKey(probe.URL)
		if _, broken := rawBrokenURLs[key]; !broken {
			continue
		}
		for form := range cleanupReconciliationURLForms(probe.URL, scopes, organization, project) {
			probes[form] = append(probes[form], probe)
		}
	}
	return brokenAccessEvidenceSet{brokenURLs: brokenURLs, brokenProbes: probes}, nil
}

func currentAccessURLFamily(
	finding GitStorageCleanupApplyFinding,
	staleRecord GitStorageCleanupRecordAudit,
	byChecksum map[string][]gintegrationsyfon.ProjectRecord,
	byObjectID map[string]gintegrationsyfon.ProjectRecord,
) (string, []gintegrationsyfon.ProjectRecord, error) {
	checksum := normalizeAnalyticsChecksum(staleRecord.Checksum)
	if checksum == "" {
		checksum = cleanupFindingChecksum(finding)
	}
	if checksum != "" {
		current := byChecksum[checksum]
		if len(current) == 0 {
			return "", nil, fmt.Errorf("cannot reconcile cleanup finding %q: current project inventory has no record for checksum %q", finding.NormalizedPath, checksum)
		}
		return "sha256:" + checksum, current, nil
	}

	objectID := strings.TrimSpace(staleRecord.ObjectID)
	if objectID == "" {
		return "", nil, fmt.Errorf("cannot reconcile cleanup finding %q: record has neither checksum nor object_id", finding.NormalizedPath)
	}
	current, ok := byObjectID[objectID]
	if !ok {
		return "", nil, fmt.Errorf("cannot reconcile cleanup finding %q: current project inventory has no record for object %q", finding.NormalizedPath, objectID)
	}
	if checksum := normalizeAnalyticsChecksum(current.Checksum); checksum != "" {
		return "sha256:" + checksum, byChecksum[checksum], nil
	}
	return "object:" + objectID, []gintegrationsyfon.ProjectRecord{current}, nil
}

func accessRepairFindingRecords(finding GitStorageCleanupApplyFinding) []GitStorageCleanupRecordAudit {
	if len(finding.Records) > 0 {
		return append([]GitStorageCleanupRecordAudit(nil), finding.Records...)
	}
	records := make([]GitStorageCleanupRecordAudit, 0, len(finding.ObjectIDs))
	for _, objectID := range finding.ObjectIDs {
		records = append(records, GitStorageCleanupRecordAudit{
			ObjectID:   strings.TrimSpace(objectID),
			Checksum:   cleanupFindingChecksum(finding),
			AccessURLs: append([]string(nil), finding.AccessURLs...),
		})
	}
	return records
}

func cleanupRecordAuditFromProjectRecord(record gintegrationsyfon.ProjectRecord) GitStorageCleanupRecordAudit {
	accessURLs := append([]string(nil), record.AccessURLs...)
	for _, method := range record.AccessMethods {
		accessURLs = append(accessURLs, method.URL)
	}
	return GitStorageCleanupRecordAudit{
		ObjectID:      strings.TrimSpace(record.ObjectID),
		Checksum:      strings.TrimSpace(record.Checksum),
		AccessURLs:    uniqueStrings(accessURLs),
		AccessMethods: cleanupAccessMethodsFromProjectMethods(record.AccessMethods),
		SizeBytes:     record.Size,
		LastUpdated:   formatOptionalTime(record.UpdatedAt),
	}
}

func accessMethodURLsInSet(record GitStorageCleanupRecordAudit, wanted map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) []string {
	methods := record.AccessMethods
	if len(methods) == 0 {
		methods = make([]GitStorageCleanupAccessMethod, 0, len(record.AccessURLs))
		for _, accessURL := range record.AccessURLs {
			methods = append(methods, GitStorageCleanupAccessMethod{URL: accessURL})
		}
	}
	matched := make([]string, 0)
	seen := make(map[string]struct{})
	for _, method := range methods {
		isMatch := false
		for key := range cleanupReconciliationURLForms(method.URL, scopes, organization, project) {
			if _, ok := wanted[key]; ok {
				isMatch = true
				break
			}
		}
		if !isMatch {
			continue
		}
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		key := normalizeCleanupSelectionKey(url)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		matched = append(matched, url)
	}
	return matched
}

func brokenAccessProbesForRecord(
	brokenURLs []string,
	probesByURL map[string][]GitStorageCleanupAccessProbe,
	scopes []domain.StorageBucketScope,
	organization string,
	project string,
) []GitStorageCleanupAccessProbe {
	probes := make([]GitStorageCleanupAccessProbe, 0, len(brokenURLs))
	for _, brokenURL := range brokenURLs {
		var existing []GitStorageCleanupAccessProbe
		for form := range cleanupReconciliationURLForms(brokenURL, scopes, organization, project) {
			if candidates := probesByURL[form]; len(candidates) > 0 {
				existing = candidates
				break
			}
		}
		if len(existing) > 0 {
			for _, existingProbe := range existing {
				probe := existingProbe
				probe.URL = brokenURL
				probes = append(probes, probe)
			}
			continue
		}
		probes = append(probes, GitStorageCleanupAccessProbe{
			URL:    brokenURL,
			Status: "error",
		})
	}
	return probes
}

func cleanupReconciliationURLForms(value string, scopes []domain.StorageBucketScope, organization string, project string) map[string]struct{} {
	forms := make(map[string]struct{}, 2)
	if raw := normalizeCleanupSelectionKey(value); raw != "" {
		forms[raw] = struct{}{}
	}
	sourceBucket, key, ok := parseStorageURL(value)
	if !ok {
		return forms
	}
	applicableScopes := make([]cleanupReconciliationScope, 0, len(scopes))
	for _, scope := range scopes {
		if !storageScopeApplies(scope, organization, project) {
			continue
		}
		targetBucket, scopePrefix, ok := parseStorageScopePath(scope)
		if !ok || targetBucket == "" {
			continue
		}
		applicableScopes = append(applicableScopes, cleanupReconciliationScope{targetBucket: targetBucket, prefix: scopePrefix})
	}
	for _, scope := range applicableScopes {
		if sourceBucket == scope.targetBucket {
			return forms
		}
	}
	canonicalCandidates := make(map[string]struct{}, len(applicableScopes))
	for _, scope := range applicableScopes {
		if scope.prefix == "" || sourceBucket != scope.prefix {
			continue
		}
		canonicalKey := composeCleanupReconciliationKey(key, scope.prefix)
		if canonical := normalizeCleanupSelectionKey(canonicalStorageURL(scope.targetBucket, canonicalKey, "")); canonical != "" {
			canonicalCandidates[canonical] = struct{}{}
		}
	}
	if len(canonicalCandidates) == 1 {
		for canonical := range canonicalCandidates {
			forms[canonical] = struct{}{}
		}
	}
	return forms
}

func composeCleanupReconciliationKey(key string, scopePrefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	scopePrefix = strings.Trim(strings.TrimSpace(scopePrefix), "/")
	if scopePrefix == "" {
		return key
	}
	key = trimLeadingStoragePrefixForGecko(key, scopePrefix)
	if key == "" {
		return scopePrefix
	}
	return scopePrefix + "/" + key
}

func accessRepairFindingPath(finding GitStorageCleanupApplyFinding) string {
	if path := strings.TrimSpace(finding.NormalizedPath); path != "" {
		return path
	}
	if checksum := cleanupFindingChecksum(finding); checksum != "" {
		return checksum
	}
	if len(finding.ObjectIDs) > 0 {
		return strings.TrimSpace(finding.ObjectIDs[0])
	}
	return storageActionRemoveBrokenAccessURLs
}

func cleanupFindingChecksum(finding GitStorageCleanupApplyFinding) string {
	if finding.Evidence == nil {
		return ""
	}
	return normalizeAnalyticsChecksum(finding.Evidence.Checksum)
}
