package cleanup

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

type brokenAccessURLFamily struct {
	records      []gintegrationsyfon.ProjectRecord
	brokenURLs   map[string]struct{}
	brokenProbes map[string][]model.CleanupAccessProbe
}

type cleanupReconciliationScope struct {
	targetBucket string
	prefix       string
}

func (service *Service) reconcileBrokenAccessURLFindings(
	ctx context.Context,
	authorizationHeader string,
	organization string,
	project string,
	scopes []domain.StorageBucketScope,
	findings []model.CleanupApplyFinding,
	selection ApplySelection,
) ([]model.CleanupApplyFinding, []string, error) {
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
		if action == sarecord.ActionRemoveBrokenAccessURLs {
			needsRefresh = true
		}
	}
	if !needsRefresh {
		return findings, nil, nil
	}

	freshRecords, err := service.store.ListProjectAuditRecords(ctx, authorizationHeader, organization, project)
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
		if action != sarecord.ActionRemoveBrokenAccessURLs {
			continue
		}
		staleRecords := accessRepairFindingRecords(finding)
		if len(staleRecords) == 0 {
			return nil, nil, fmt.Errorf("cleanup finding %q is missing records for %s", finding.Kind, sarecord.ActionRemoveBrokenAccessURLs)
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
					brokenProbes: make(map[string][]model.CleanupAccessProbe),
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
	out := make([]model.CleanupApplyFinding, 0, len(findings))
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
		if action != sarecord.ActionRemoveBrokenAccessURLs {
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
	return out, sarecord.UniqueStrings(skippedPaths), nil
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
		if checksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum); checksum != "" {
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
	finding model.CleanupApplyFinding,
	familyKeys []string,
	families map[string]*brokenAccessURLFamily,
	scopes []domain.StorageBucketScope,
	organization string,
	project string,
) (model.CleanupApplyFinding, bool, error) {
	updatedByObjectID := make(map[string]model.CleanupRecordAudit)
	for _, familyKey := range familyKeys {
		family := families[familyKey]
		if family == nil {
			return model.CleanupApplyFinding{}, false, fmt.Errorf("cannot reconcile cleanup finding %q: missing current access URL family %q", finding.NormalizedPath, familyKey)
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
		return model.CleanupApplyFinding{}, false, nil
	}
	objectIDs := make([]string, 0, len(updatedByObjectID))
	for objectID := range updatedByObjectID {
		objectIDs = append(objectIDs, objectID)
	}
	sort.Strings(objectIDs)
	updatedRecords := make([]model.CleanupRecordAudit, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		updatedRecords = append(updatedRecords, updatedByObjectID[objectID])
	}

	reconciled := finding
	reconciled.Records = updatedRecords
	return reconciled, true, nil
}
