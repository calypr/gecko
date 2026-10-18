package cleanup

import (
	"context"
	"fmt"
	"strings"

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

type ApplySelection struct {
	selected                map[string]struct{}
	actionSelection         map[string]string
	deleteRepoOrphans       bool
	deleteStaleDuplicates   bool
	deleteBucketOnlyObjects bool
	repairBrokenMappings    bool
}
