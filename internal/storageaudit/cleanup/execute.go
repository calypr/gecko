package cleanup

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func (service *Service) executeStorageCleanupApplyPlan(ctx context.Context, authorizationHeader string, organization string, project string, plan ApplyPlan, dryRun bool) (*model.CleanupApplyResponse, error) {
	if err := service.verifyAccessMethodChanges(ctx, authorizationHeader, plan.AccessMethodChanges); err != nil {
		return nil, err
	}
	toDelete := sarecord.UniqueStrings(plan.DeleteObjectIDs)
	toDeleteWithStorage := sarecord.UniqueStrings(plan.DeleteStorageObjectIDs)
	toDeleteMetadataOnly := sarecord.DifferenceStrings(toDelete, toDeleteWithStorage)
	toDeleteBucketObjects := sarecord.UniqueStrings(plan.DeleteBucketObjects)
	repoDeletePaths := sarecord.UniqueStrings(plan.RepoDeletePaths)
	deletedBucketObjectURLs := make([]string, 0)
	updatedRecordIDs := sarecord.UniqueStrings(plan.UpdatedRecordIDs)
	manualPaths := sarecord.UniqueStrings(plan.ManualPaths)
	skippedPaths := sarecord.UniqueStrings(plan.SkippedPaths)
	purgeResults := make([]model.CleanupPurgeResult, 0, len(toDelete)+len(toDeleteBucketObjects))
	if dryRun {
		for _, objectID := range toDelete {
			purgeResults = append(purgeResults, model.CleanupPurgeResult{
				ObjectID: objectID,
				Success:  nil,
				Status:   "dry_run",
			})
		}
		return &model.CleanupApplyResponse{
			DeletedRecordIDs:        toDelete,
			DeletedBucketObjectURLs: toDeleteBucketObjects,
			UpdatedRecordIDs:        updatedRecordIDs,
			PurgeResults:            purgeResults,
			RepoDeletePaths:         repoDeletePaths,
			ManualPaths:             manualPaths,
			SkippedPaths:            skippedPaths,
			DryRun:                  true,
			AccessMethodChanges:     plan.AccessMethodChanges,
		}, nil
	}
	if err := service.verifyStorageCleanupApplyPlan(ctx, authorizationHeader, &plan); err != nil {
		return nil, err
	}
	toDeleteBucketObjects = sarecord.UniqueStrings(plan.DeleteBucketObjects)
	skippedPaths = sarecord.UniqueStrings(plan.SkippedPaths)
	if len(plan.UpdateAccessMethods) > 0 {
		if err := service.store.BulkUpdateAccessMethods(ctx, authorizationHeader, plan.UpdateAccessMethods); err != nil {
			return nil, fmt.Errorf("update syfon access methods: %w", err)
		}
	}
	if len(toDeleteMetadataOnly) > 0 {
		if err := service.store.BulkDeleteObjects(ctx, authorizationHeader, toDeleteMetadataOnly, false); err != nil {
			return nil, fmt.Errorf("delete syfon objects: %w", err)
		}
	}
	if len(toDeleteWithStorage) > 0 {
		if err := service.store.BulkDeleteObjects(ctx, authorizationHeader, toDeleteWithStorage, true); err != nil {
			return nil, fmt.Errorf("delete syfon objects: %w", err)
		}
	}
	if len(toDeleteBucketObjects) > 0 {
		results, err := service.store.DeleteProjectBucketObjects(ctx, authorizationHeader, organization, project, toDeleteBucketObjects)
		if err != nil {
			return nil, fmt.Errorf("delete syfon project bucket objects: %w", err)
		}
		for _, result := range results {
			if strings.EqualFold(strings.TrimSpace(result.Status), "deleted") {
				deletedBucketObjectURLs = append(deletedBucketObjectURLs, result.ObjectURL)
				continue
			}
			purgeResults = append(purgeResults, model.CleanupPurgeResult{
				ObjectID: result.ObjectURL,
				Success:  sarecord.BoolPtr(false),
				Status:   strings.TrimSpace(result.Status),
				Error:    strings.TrimSpace(result.Error),
			})
		}
		deletedBucketObjectURLs = sarecord.UniqueStrings(deletedBucketObjectURLs)
	}
	if len(plan.UpdateAccessMethods) > 0 || len(toDelete) > 0 || len(toDeleteBucketObjects) > 0 {
		service.store.EvictProjectJoinCache(organization, project)
		service.store.EvictProjectAuditRecordCache(organization, project)
		service.store.EvictProjectChainInputCache(organization, project)
	}
	for _, objectID := range toDelete {
		success := true
		purgeResults = append(purgeResults, model.CleanupPurgeResult{
			ObjectID: objectID,
			Success:  &success,
			Status:   "deleted",
		})
	}
	return &model.CleanupApplyResponse{
		DeletedRecordIDs:        toDelete,
		DeletedBucketObjectURLs: deletedBucketObjectURLs,
		UpdatedRecordIDs:        updatedRecordIDs,
		PurgeResults:            purgeResults,
		RepoDeletePaths:         repoDeletePaths,
		ManualPaths:             manualPaths,
		SkippedPaths:            skippedPaths,
		DryRun:                  false,
		AccessMethodChanges:     plan.AccessMethodChanges,
	}, nil
}

func indexCleanupSelection(paths []string) map[string]struct{} {
	index := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		normalized := sarecord.NormalizeCleanupSelectionKey(path)
		if normalized == "" {
			continue
		}
		index[normalized] = struct{}{}
	}
	return index
}

func storageApplyFindingSelected(selected map[string]struct{}, finding model.CleanupApplyFinding) bool {
	for _, candidate := range storageApplyFindingSelectionCandidates(finding) {
		if _, ok := selected[candidate]; ok {
			return true
		}
	}
	return false
}

func storageApplyFindingSelectionCandidates(finding model.CleanupApplyFinding) []string {
	candidates := []string{
		finding.NormalizedPath,
		finding.BucketObjectURL,
	}
	candidates = append(candidates, finding.ObjectIDs...)
	candidates = append(candidates, finding.BucketObjectURLs...)
	candidates = append(candidates, finding.AccessURLs...)
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.AccessURLs...)
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.ObjectIDs...)
	}
	return sarecord.UniqueCleanupSelectionCandidates(candidates)
}

func storageApplyFindingBucketObjectURLs(finding model.CleanupApplyFinding) []string {
	candidates := []string{
		finding.BucketObjectURL,
	}
	candidates = append(candidates, finding.BucketObjectURLs...)
	candidates = append(candidates, finding.AccessURLs...)
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.AccessURLs...)
	}
	if len(sarecord.UniqueCleanupSelectionCandidates(candidates)) == 0 && sarecord.CanonicalStorageURL("", "", finding.NormalizedPath) != "" {
		candidates = append(candidates, finding.NormalizedPath)
	}
	return sarecord.UniqueCleanupSelectionCandidates(candidates)
}

func cleanupActionForApplyFinding(index map[string]string, finding model.CleanupApplyFinding) string {
	for _, path := range storageApplyFindingSelectionCandidates(finding) {
		if action := strings.TrimSpace(index[cleanupActionKey(finding.Kind, path)]); action != "" {
			return action
		}
		if action := strings.TrimSpace(index[cleanupActionKey("", path)]); action != "" {
			return action
		}
	}
	return ""
}

func indexCleanupActions(actions []model.CleanupApplyAction) map[string]string {
	if len(actions) == 0 {
		return nil
	}
	index := make(map[string]string, len(actions))
	for _, action := range actions {
		if strings.TrimSpace(action.Action) == "" {
			continue
		}
		path := sarecord.NormalizeRepoSubpath(action.NormalizedPath)
		if path == "" {
			continue
		}
		key := cleanupActionKey(strings.TrimSpace(action.Kind), path)
		index[key] = strings.TrimSpace(action.Action)
	}
	return index
}

func cleanupActionKey(kind string, path string) string {
	return strings.TrimSpace(kind) + "::" + sarecord.NormalizeRepoSubpath(path)
}

func addStorageCleanupApplyFindingToPlan(plan *ApplyPlan, finding model.CleanupApplyFinding, action string) error {
	if err := appendStorageCleanupVerification(plan, finding, action); err != nil {
		return err
	}
	switch action {
	case sarecord.ActionInspectEvidence:
		plan.ManualPaths = append(plan.ManualPaths, finding.NormalizedPath)
	case sarecord.ActionDeleteSyfonRecord:
		objectIDs := storageApplyFindingObjectIDs(finding)
		if len(objectIDs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing object_ids for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
		plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
	case sarecord.ActionDeleteBucketObject:
		bucketURLs := storageApplyFindingBucketObjectURLs(finding)
		if len(bucketURLs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteBucketObjects = append(plan.DeleteBucketObjects, bucketURLs...)
	case sarecord.ActionDeleteBoth:
		objectIDs := storageApplyFindingObjectIDs(finding)
		bucketURLs := storageApplyFindingBucketObjectURLs(finding)
		if len(objectIDs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing object_ids for %s", finding.Kind, finding.NormalizedPath, action)
		}
		if strings.TrimSpace(finding.Kind) == "repo_orphan_live_object" {
			plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
			plan.DeleteStorageObjectIDs = append(plan.DeleteStorageObjectIDs, objectIDs...)
			plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
			return nil
		}
		if len(bucketURLs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
		plan.DeleteBucketObjects = append(plan.DeleteBucketObjects, bucketURLs...)
		plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
	case sarecord.ActionRemoveBrokenAccessURLs:
		return addAccessMethodRepairToPlan(plan, finding)
	default:
		return fmt.Errorf("unsupported cleanup action %q for finding kind %q", action, finding.Kind)
	}
	return nil
}

func storageApplyFindingObjectIDs(finding model.CleanupApplyFinding) []string {
	objectIDs := append([]string(nil), finding.ObjectIDs...)
	for _, record := range finding.Records {
		objectIDs = append(objectIDs, record.ObjectID)
	}
	return sarecord.UniqueStrings(objectIDs)
}

func addAccessMethodRepairToPlan(plan *ApplyPlan, finding model.CleanupApplyFinding) error {
	records := finding.Records
	if len(records) == 0 {
		for _, objectID := range finding.ObjectIDs {
			records = append(records, model.CleanupRecordAudit{
				ObjectID:   objectID,
				AccessURLs: append([]string(nil), finding.AccessURLs...),
			})
		}
	}
	if len(records) == 0 {
		return fmt.Errorf("cleanup finding %q at %q is missing records for %s", finding.Kind, finding.NormalizedPath, sarecord.ActionRemoveBrokenAccessURLs)
	}
	for _, record := range records {
		objectID := strings.TrimSpace(record.ObjectID)
		if objectID == "" {
			return fmt.Errorf("cleanup finding %q at %q has a record without object_id", finding.Kind, finding.NormalizedPath)
		}
		if len(sarecord.BrokenAccessURLsForRecord(record)) == 0 {
			return fmt.Errorf("cleanup finding %q at %q record %q is missing broken access URL evidence", finding.Kind, finding.NormalizedPath, objectID)
		}
		remaining := RemainingAccessMethodsAfterBrokenRemoval(record)
		plan.AccessMethodChanges = append(plan.AccessMethodChanges, accessMethodChangeForRecord(record, remaining))
		if len(remaining) == 0 {
			plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectID)
			plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
			continue
		}
		plan.UpdateAccessMethods[objectID] = remaining
		plan.UpdatedRecordIDs = append(plan.UpdatedRecordIDs, objectID)
	}
	return nil
}

func RemainingAccessMethodsAfterBrokenRemoval(record model.CleanupRecordAudit) []gintegrationsyfon.ProjectAccessMethod {
	methods := sarecord.ProjectAccessMethodsFromCleanupMethods(record.AccessMethods)
	if len(methods) == 0 {
		for _, accessURL := range record.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				methods = append(methods, gintegrationsyfon.ProjectAccessMethod{URL: trimmed})
			}
		}
	}
	brokenURLs := sarecord.BrokenAccessURLsForRecord(record)
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(methods))
	for _, method := range methods {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		if _, broken := brokenURLs[sarecord.NormalizeCleanupSelectionKey(url)]; broken {
			continue
		}
		remaining = append(remaining, method)
	}
	return sarecord.AppendReplacementAccessMethods(remaining, record.AccessProbes, brokenURLs)
}

func coalesceAccessMethodChanges(changes []model.AccessMethodChange) ([]model.AccessMethodChange, error) {
	out := make([]model.AccessMethodChange, 0, len(changes))
	byID := make(map[string]model.AccessMethodChange, len(changes))
	for _, change := range changes {
		previous, exists := byID[change.ObjectID]
		if exists {
			if !reflect.DeepEqual(previous.Before, change.Before) || !reflect.DeepEqual(previous.After, change.After) {
				return nil, fmt.Errorf("conflicting access method changes for Syfon record %q", change.ObjectID)
			}
			continue
		}
		byID[change.ObjectID] = change
		out = append(out, change)
	}
	return out, nil
}

func accessMethodChangeForRecord(record model.CleanupRecordAudit, after []gintegrationsyfon.ProjectAccessMethod) model.AccessMethodChange {
	before := sarecord.ProjectAccessMethodsFromCleanupMethods(record.AccessMethods)
	if len(before) == 0 {
		for _, url := range record.AccessURLs {
			if strings.TrimSpace(url) != "" {
				before = append(before, gintegrationsyfon.ProjectAccessMethod{URL: url, Type: "s3"})
			}
		}
	}
	change := model.AccessMethodChange{
		ObjectID: record.ObjectID,
		Before:   publicAccessMethods(before),
		After:    publicAccessMethods(after),
	}
	beforeURLs := accessMethodURLSet(before)
	afterURLs := accessMethodURLSet(after)
	for key, url := range beforeURLs {
		if _, kept := afterURLs[key]; !kept {
			change.Removed = append(change.Removed, url)
		}
	}
	for key, url := range afterURLs {
		if _, existed := beforeURLs[key]; existed {
			change.Retained = append(change.Retained, url)
		} else {
			change.Added = append(change.Added, url)
		}
	}
	sort.Strings(change.Removed)
	sort.Strings(change.Retained)
	sort.Strings(change.Added)
	return change
}

func publicAccessMethods(methods []gintegrationsyfon.ProjectAccessMethod) []model.CleanupAccessMethod {
	out := make([]model.CleanupAccessMethod, 0, len(methods))
	for _, method := range methods {
		out = append(out, model.CleanupAccessMethod{
			AccessID: method.AccessID,
			Type:     method.Type,
			URL:      method.URL,
		})
	}
	return out
}

func accessMethodURLSet(methods []gintegrationsyfon.ProjectAccessMethod) map[string]string {
	out := make(map[string]string, len(methods))
	for _, method := range methods {
		if key := sarecord.NormalizeCleanupSelectionKey(method.URL); key != "" {
			out[key] = strings.TrimSpace(method.URL)
		}
	}
	return out
}

func (service *Service) verifyAccessMethodChanges(ctx context.Context, authorizationHeader string, changes []model.AccessMethodChange) error {
	if len(changes) == 0 {
		return nil
	}
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemByURL := make(map[string]string)
	for _, change := range changes {
		for _, method := range append(append([]model.CleanupAccessMethod(nil), change.Before...), change.After...) {
			url := strings.TrimSpace(method.URL)
			if url == "" || itemByURL[url] != "" {
				continue
			}
			id := fmt.Sprintf("access-repair-%d", len(items))
			itemByURL[url] = id
			items = append(items, gintegrationsyfon.BulkStorageProbeItem{ID: id, ObjectURL: url})
		}
	}
	results, err := service.store.BulkConfirmStorageObjects(ctx, authorizationHeader, items)
	if err != nil {
		return fmt.Errorf("revalidate Syfon access methods: %w", err)
	}
	byID := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(results))
	for _, result := range results {
		if _, duplicate := byID[result.ID]; duplicate {
			return fmt.Errorf("revalidate Syfon access methods: duplicate probe result %q", result.ID)
		}
		result.Operation = model.StorageChainValidationModeMetadata
		byID[result.ID] = result
	}
	if len(byID) != len(items) {
		return fmt.Errorf("revalidate Syfon access methods: expected %d probe results, got %d", len(items), len(byID))
	}
	for index := range changes {
		change := &changes[index]
		if err := validateAccessMethodChange(change, itemByURL, byID); err != nil {
			return err
		}
	}
	return nil
}

func validateAccessMethodChange(change *model.AccessMethodChange, itemByURL map[string]string, byID map[string]gintegrationsyfon.BulkStorageProbeResult) error {
	if len(change.Removed) == 0 {
		return fmt.Errorf("revalidate Syfon record %q: repair does not remove an access URL", change.ObjectID)
	}
	removed := make(map[string]struct{}, len(change.Removed))
	for _, url := range change.Removed {
		removed[sarecord.NormalizeCleanupSelectionKey(url)] = struct{}{}
	}
	change.FreshProbes = make([]model.AccessMethodProbe, 0, len(change.Before)+len(change.Added))
	seen := make(map[string]struct{})
	hasPresentAfter := false
	for _, method := range append(append([]model.CleanupAccessMethod(nil), change.Before...), change.After...) {
		url := strings.TrimSpace(method.URL)
		if _, done := seen[url]; done {
			continue
		}
		seen[url] = struct{}{}
		result, ok := byID[itemByURL[url]]
		if !ok {
			return fmt.Errorf("revalidate Syfon record %q access URL %q: missing probe result", change.ObjectID, url)
		}
		change.FreshProbes = append(change.FreshProbes, model.AccessMethodProbe{
			URL: url, Status: result.Status, ErrorKind: result.ErrorKind, VerificationMethod: result.VerificationMethod, ValidationStatus: result.ValidationStatus,
		})
		if _, isRemoved := removed[sarecord.NormalizeCleanupSelectionKey(url)]; isRemoved {
			if !sarecord.SyfonProbeIsBrokenAccess(result) || (result.Status == "not_found" && result.VerificationMethod != "ranged_get") {
				return fmt.Errorf("revalidate Syfon record %q: access URL %q is not confirmed broken by an exact storage check (status=%q, error_kind=%q, verification_method=%q)", change.ObjectID, url, result.Status, result.ErrorKind, result.VerificationMethod)
			}
		}
		for _, after := range change.After {
			if after.URL == url && result.Status == "present" && result.Exists && sarecord.ProbeValidationMatched(result.ValidationStatus) {
				hasPresentAfter = true
			}
		}
	}
	if len(change.After) > 0 && !hasPresentAfter {
		return fmt.Errorf("revalidate Syfon record %q: no retained or replacement access URL was confirmed present", change.ObjectID)
	}
	return nil
}

type ApplySelection struct {
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

func cleanupReconciliationURLForms(value string, scopes []domain.StorageBucketScope, organization string, project string) map[string]struct{} {
	forms := make(map[string]struct{}, 2)
	if raw := sarecord.NormalizeCleanupSelectionKey(value); raw != "" {
		forms[raw] = struct{}{}
	}
	sourceBucket, key, ok := sarecord.ParseStorageURL(value)
	if !ok {
		return forms
	}
	applicableScopes := make([]cleanupReconciliationScope, 0, len(scopes))
	for _, scope := range scopes {
		if !sarecord.StorageScopeApplies(scope, organization, project) {
			continue
		}
		targetBucket, scopePrefix, ok := sarecord.ParseStorageScopePath(scope)
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
		if canonical := sarecord.NormalizeCleanupSelectionKey(sarecord.CanonicalStorageURL(scope.targetBucket, canonicalKey, "")); canonical != "" {
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
	key = sarecord.TrimLeadingStoragePrefixForGecko(key, scopePrefix)
	if key == "" {
		return scopePrefix
	}
	return scopePrefix + "/" + key
}

func accessRepairFindingPath(finding model.CleanupApplyFinding) string {
	if path := strings.TrimSpace(finding.NormalizedPath); path != "" {
		return path
	}
	if checksum := cleanupFindingChecksum(finding); checksum != "" {
		return checksum
	}
	if len(finding.ObjectIDs) > 0 {
		return strings.TrimSpace(finding.ObjectIDs[0])
	}
	return sarecord.ActionRemoveBrokenAccessURLs
}

func cleanupFindingChecksum(finding model.CleanupApplyFinding) string {
	if finding.Evidence == nil {
		return ""
	}
	return sarecord.NormalizeAnalyticsChecksum(finding.Evidence.Checksum)
}

const (
	cleanupExpectationPresent = "present"
	ExpectationMissing        = "missing"
)

type Verification struct {
	FindingKind    string
	NormalizedPath string
	ObjectURL      string
	Expectation    string
}

func appendStorageCleanupVerification(plan *ApplyPlan, finding model.CleanupApplyFinding, action string) error {
	if plan == nil || action == sarecord.ActionInspectEvidence {
		return nil
	}
	expectation := VerificationExpectation(finding.Kind, action)
	if expectation == "" {
		return nil
	}
	objectURLs := storageApplyFindingBucketObjectURLs(finding)
	if len(objectURLs) == 0 {
		return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs required for exact revalidation", finding.Kind, finding.NormalizedPath)
	}
	for _, objectURL := range objectURLs {
		plan.Verifications = append(plan.Verifications, Verification{
			FindingKind:    strings.TrimSpace(finding.Kind),
			NormalizedPath: strings.TrimSpace(finding.NormalizedPath),
			ObjectURL:      strings.TrimSpace(objectURL),
			Expectation:    expectation,
		})
	}
	return nil
}

func VerificationExpectation(kind string, action string) string {
	switch action {
	case sarecord.ActionDeleteBucketObject, sarecord.ActionDeleteBoth:
		return cleanupExpectationPresent
	case sarecord.ActionDeleteSyfonRecord:
		switch strings.TrimSpace(kind) {
		case "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
			return ExpectationMissing
		}
	}
	return ""
}

func (service *Service) verifyStorageCleanupApplyPlan(ctx context.Context, authorizationHeader string, plan *ApplyPlan) error {
	if plan == nil {
		return nil
	}
	verifications := uniqueStorageCleanupVerifications(plan.Verifications)
	if len(verifications) == 0 {
		return nil
	}
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0, len(verifications))
	for index, verification := range verifications {
		items = append(items, gintegrationsyfon.BulkStorageProbeItem{
			ID:        fmt.Sprintf("cleanup-revalidate-%d", index),
			ObjectURL: verification.ObjectURL,
		})
	}
	results, err := service.store.BulkConfirmStorageObjects(ctx, authorizationHeader, items)
	if err != nil {
		return fmt.Errorf("revalidate cleanup storage evidence: %w", err)
	}
	if len(results) != len(items) {
		return fmt.Errorf("revalidate cleanup storage evidence: expected %d results, received %d", len(items), len(results))
	}
	for index, result := range results {
		if cleanupObjectVanishedAfterAudit(verifications[index], result) {
			plan.DeleteBucketObjects = sarecord.DifferenceStrings(plan.DeleteBucketObjects, []string{verifications[index].ObjectURL})
			plan.SkippedPaths = append(plan.SkippedPaths, verifications[index].NormalizedPath)
			continue
		}
		if err := validateStorageCleanupResult(verifications[index], result); err != nil {
			return err
		}
	}
	return nil
}

func cleanupObjectVanishedAfterAudit(verification Verification, result gintegrationsyfon.BulkStorageProbeResult) bool {
	if verification.Expectation != cleanupExpectationPresent {
		return false
	}
	if strings.TrimSpace(result.Status) != "not_found" || strings.TrimSpace(result.ErrorKind) != "object_not_found" || strings.TrimSpace(result.VerificationMethod) != "ranged_get" {
		return false
	}
	switch strings.TrimSpace(verification.FindingKind) {
	case "bucket_only_object", "bucket_syfon_no_git":
		return true
	default:
		return false
	}
}

func uniqueStorageCleanupVerifications(input []Verification) []Verification {
	seen := make(map[string]struct{}, len(input))
	out := make([]Verification, 0, len(input))
	for _, verification := range input {
		key := verification.Expectation + "\x00" + verification.ObjectURL
		if verification.ObjectURL == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, verification)
	}
	return out
}

func validateStorageCleanupResult(verification Verification, result gintegrationsyfon.BulkStorageProbeResult) error {
	status := strings.TrimSpace(result.Status)
	switch verification.Expectation {
	case cleanupExpectationPresent:
		if status == "present" && result.Exists {
			return nil
		}
	case ExpectationMissing:
		if status == "not_found" && strings.TrimSpace(result.ErrorKind) == "object_not_found" && strings.TrimSpace(result.VerificationMethod) == "ranged_get" {
			return nil
		}
	}
	return fmt.Errorf(
		"revalidate cleanup finding %q for %q: expected %s, got status=%q error_kind=%q verification_method=%q error=%q",
		verification.FindingKind,
		verification.ObjectURL,
		verification.Expectation,
		status,
		strings.TrimSpace(result.ErrorKind),
		strings.TrimSpace(result.VerificationMethod),
		strings.TrimSpace(result.Error),
	)
}
