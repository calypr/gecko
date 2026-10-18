package cleanup

import (
	"fmt"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

type brokenAccessEvidenceSet struct {
	brokenURLs   map[string]struct{}
	brokenProbes map[string][]model.CleanupAccessProbe
}

func brokenAccessEvidence(record model.CleanupRecordAudit, scopes []domain.StorageBucketScope, organization string, project string) (brokenAccessEvidenceSet, error) {
	rawBrokenURLs := sarecord.BrokenAccessURLsForRecord(record)
	if len(rawBrokenURLs) == 0 {
		return brokenAccessEvidenceSet{}, fmt.Errorf("missing broken access URL evidence")
	}
	brokenURLs := make(map[string]struct{})
	for brokenURL := range rawBrokenURLs {
		for key := range cleanupReconciliationURLForms(brokenURL, scopes, organization, project) {
			brokenURLs[key] = struct{}{}
		}
	}
	probes := make(map[string][]model.CleanupAccessProbe)
	for _, probe := range record.AccessProbes {
		key := sarecord.NormalizeCleanupSelectionKey(probe.URL)
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
	finding model.CleanupApplyFinding,
	staleRecord model.CleanupRecordAudit,
	byChecksum map[string][]gintegrationsyfon.ProjectRecord,
	byObjectID map[string]gintegrationsyfon.ProjectRecord,
) (string, []gintegrationsyfon.ProjectRecord, error) {
	checksum := sarecord.NormalizeAnalyticsChecksum(staleRecord.Checksum)
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
	if checksum := sarecord.NormalizeAnalyticsChecksum(current.Checksum); checksum != "" {
		return "sha256:" + checksum, byChecksum[checksum], nil
	}
	return "object:" + objectID, []gintegrationsyfon.ProjectRecord{current}, nil
}

func accessRepairFindingRecords(finding model.CleanupApplyFinding) []model.CleanupRecordAudit {
	if len(finding.Records) > 0 {
		return append([]model.CleanupRecordAudit(nil), finding.Records...)
	}
	records := make([]model.CleanupRecordAudit, 0, len(finding.ObjectIDs))
	for _, objectID := range finding.ObjectIDs {
		records = append(records, model.CleanupRecordAudit{
			ObjectID:   strings.TrimSpace(objectID),
			Checksum:   cleanupFindingChecksum(finding),
			AccessURLs: append([]string(nil), finding.AccessURLs...),
		})
	}
	return records
}

func cleanupRecordAuditFromProjectRecord(record gintegrationsyfon.ProjectRecord) model.CleanupRecordAudit {
	accessURLs := append([]string(nil), record.AccessURLs...)
	for _, method := range record.AccessMethods {
		accessURLs = append(accessURLs, method.URL)
	}
	return model.CleanupRecordAudit{
		ObjectID:      strings.TrimSpace(record.ObjectID),
		Checksum:      strings.TrimSpace(record.Checksum),
		AccessURLs:    sarecord.UniqueStrings(accessURLs),
		AccessMethods: sarecord.CleanupAccessMethodsFromProjectMethods(record.AccessMethods),
		SizeBytes:     record.Size,
		LastUpdated:   sarecord.FormatOptionalTime(record.UpdatedAt),
	}
}

func accessMethodURLsInSet(record model.CleanupRecordAudit, wanted map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) []string {
	methods := record.AccessMethods
	if len(methods) == 0 {
		methods = make([]model.CleanupAccessMethod, 0, len(record.AccessURLs))
		for _, accessURL := range record.AccessURLs {
			methods = append(methods, model.CleanupAccessMethod{URL: accessURL})
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
		key := sarecord.NormalizeCleanupSelectionKey(url)
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
	probesByURL map[string][]model.CleanupAccessProbe,
	scopes []domain.StorageBucketScope,
	organization string,
	project string,
) []model.CleanupAccessProbe {
	probes := make([]model.CleanupAccessProbe, 0, len(brokenURLs))
	for _, brokenURL := range brokenURLs {
		var existing []model.CleanupAccessProbe
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
		probes = append(probes, model.CleanupAccessProbe{
			URL:    brokenURL,
			Status: "error",
		})
	}
	return probes
}
