package cleanup

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func (service *Service) ApplyStorageCleanup(ctx context.Context, authorizationHeader string, organization string, project string, selectedRepoPaths []string, selectedActions []model.CleanupApplyAction, selectedFindings []model.CleanupApplyFinding, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool, dryRun bool) (*model.CleanupApplyResponse, error) {
	if len(selectedFindings) == 0 {
		return nil, fmt.Errorf("cleanup apply requires findings from a prior audit; refusing to rebuild audit during apply")
	}
	canonicalFindings, scopes, err := service.canonicalizeStorageCleanupFindings(ctx, authorizationHeader, organization, project, selectedFindings)
	if err != nil {
		return nil, err
	}
	actionSelection := indexCleanupActions(selectedActions)
	plan := ApplyPlan{
		UpdateAccessMethods: make(map[string][]gintegrationsyfon.ProjectAccessMethod),
	}
	selected := indexCleanupSelection(selectedRepoPaths)
	canonicalFindings, skippedAccessURLPaths, err := service.reconcileBrokenAccessURLFindings(
		ctx,
		authorizationHeader,
		organization,
		project,
		scopes,
		canonicalFindings,
		ApplySelection{
			selected:                selected,
			actionSelection:         actionSelection,
			deleteRepoOrphans:       deleteRepoOrphans,
			deleteStaleDuplicates:   deleteStaleDuplicates,
			deleteBucketOnlyObjects: deleteBucketOnlyObjects,
			repairBrokenMappings:    repairBrokenBucketMappings,
		},
	)
	if err != nil {
		return nil, err
	}
	plan.SkippedPaths = append(plan.SkippedPaths, skippedAccessURLPaths...)
	for _, finding := range canonicalFindings {
		if len(selected) > 0 && !storageApplyFindingSelected(selected, finding) {
			continue
		}
		action, err := resolveStorageCleanupApplyAction(finding, actionSelection, deleteRepoOrphans, deleteStaleDuplicates, deleteBucketOnlyObjects, repairBrokenBucketMappings)
		if err != nil {
			return nil, err
		}
		if err := addStorageCleanupApplyFindingToPlan(&plan, finding, action); err != nil {
			return nil, err
		}
	}
	if len(selectedRepoPaths) > 0 && len(plan.DeleteObjectIDs) == 0 && len(plan.DeleteStorageObjectIDs) == 0 && len(plan.DeleteBucketObjects) == 0 && len(plan.UpdateAccessMethods) == 0 && len(plan.ManualPaths) == 0 && len(plan.SkippedPaths) == 0 {
		return nil, fmt.Errorf("selected cleanup paths did not match provided cleanup findings")
	}
	plan.AccessMethodChanges, err = coalesceAccessMethodChanges(plan.AccessMethodChanges)
	if err != nil {
		return nil, err
	}
	return service.executeStorageCleanupApplyPlan(ctx, authorizationHeader, organization, project, plan, dryRun)
}

func (service *Service) canonicalizeStorageCleanupFindings(ctx context.Context, authorizationHeader string, organization string, project string, findings []model.CleanupApplyFinding) ([]model.CleanupApplyFinding, []domain.StorageBucketScope, error) {
	if !storageCleanupFindingsContainStorageURL(findings) {
		return append([]model.CleanupApplyFinding(nil), findings...), nil, nil
	}
	scopes, err := service.scopeLoader.LoadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, nil, fmt.Errorf("load project storage scopes for cleanup apply: %w", err)
	}
	out := make([]model.CleanupApplyFinding, 0, len(findings))
	for _, finding := range findings {
		clone := finding
		clone.BucketObjectURL = canonicalizeCleanupStorageURL(finding.BucketObjectURL, scopes, organization, project)
		clone.BucketObjectURLs = canonicalizeCleanupStorageURLs(finding.BucketObjectURLs, scopes, organization, project)
		clone.AccessURLs = canonicalizeCleanupStorageURLs(finding.AccessURLs, scopes, organization, project)
		if finding.Evidence != nil {
			evidence := *finding.Evidence
			evidence.BucketObjectURLs = canonicalizeCleanupStorageURLs(evidence.BucketObjectURLs, scopes, organization, project)
			evidence.AccessURLs = canonicalizeCleanupStorageURLs(evidence.AccessURLs, scopes, organization, project)
			clone.Evidence = &evidence
		}
		out = append(out, clone)
	}
	return out, scopes, nil
}

func storageCleanupFindingsContainStorageURL(findings []model.CleanupApplyFinding) bool {
	for _, finding := range findings {
		candidates := []string{finding.NormalizedPath, finding.BucketObjectURL}
		candidates = append(candidates, finding.BucketObjectURLs...)
		candidates = append(candidates, finding.AccessURLs...)
		if finding.Evidence != nil {
			candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
			candidates = append(candidates, finding.Evidence.AccessURLs...)
		}
		for _, candidate := range candidates {
			if _, _, ok := sarecord.ParseStorageURL(candidate); ok {
				return true
			}
		}
	}
	return false
}

func canonicalizeCleanupStorageURL(value string, scopes []domain.StorageBucketScope, organization string, project string) string {
	if objectURL := sarecord.CanonicalizeScopedStorageURL(value, scopes, organization, project); objectURL != "" {
		return objectURL
	}
	if objectURL := sarecord.CanonicalStorageURL("", "", value); objectURL != "" {
		return objectURL
	}
	return strings.TrimSpace(value)
}

func canonicalizeCleanupStorageURLs(values []string, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if canonical := canonicalizeCleanupStorageURL(value, scopes, organization, project); canonical != "" {
			out = append(out, canonical)
		}
	}
	return sarecord.UniqueStrings(out)
}

func resolveStorageCleanupApplyAction(finding model.CleanupApplyFinding, actionSelection map[string]string, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool) (string, error) {
	kind := strings.TrimSpace(finding.Kind)
	if !knownStorageRepairKind(kind) {
		return "", fmt.Errorf("unsupported cleanup finding kind %q", kind)
	}
	action := cleanupActionForApplyFinding(actionSelection, finding)
	if action == "" && strings.TrimSpace(kind) == "probe_error" {
		return sarecord.ActionInspectEvidence, nil
	}
	if action == "" {
		action = strings.TrimSpace(finding.SuggestedAction)
	}
	if action == "" {
		action = legacyDefaultStorageCleanupAction(kind, deleteRepoOrphans, deleteStaleDuplicates, deleteBucketOnlyObjects, repairBrokenBucketMappings)
	}
	if action == "" {
		action = strings.TrimSpace(finding.DefaultAction)
	}
	if action == "" {
		action = sarecord.RepairPolicyForKind(kind).DefaultAction
	}
	if action == sarecord.ActionInspectEvidence {
		return action, nil
	}
	if !storageRepairActionAllowed(kind, action) {
		return "", fmt.Errorf("cleanup action %q is not supported for finding kind %q", action, kind)
	}
	return action, nil
}

func knownStorageRepairKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "bucket_only_object", "bucket_syfon_no_git",
		"repo_orphan_live_object", "repo_orphan_stale_record",
		"stale_duplicate_record", "live_duplicate_conflict",
		"broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping",
		"storage_validation_mismatch", "git_syfon_metadata_mismatch",
		"storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object",
		"git_only_no_syfon", "storage_probe_error", "probe_error":
		return true
	default:
		return false
	}
}

func legacyDefaultStorageCleanupAction(kind string, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool) string {
	switch strings.TrimSpace(kind) {
	case "repo_orphan_live_object", "repo_orphan_stale_record":
		if deleteRepoOrphans {
			return sarecord.RepairPolicyForKind(kind).DefaultAction
		}
	case "bucket_syfon_no_git":
		if deleteRepoOrphans {
			return sarecord.ActionDeleteBoth
		}
	case "stale_duplicate_record":
		if deleteStaleDuplicates {
			return sarecord.ActionDeleteSyfonRecord
		}
	case "bucket_only_object":
		if deleteBucketOnlyObjects {
			return sarecord.ActionDeleteBucketObject
		}
	case "broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping":
		if repairBrokenBucketMappings {
			return sarecord.ActionRemoveBrokenAccessURLs
		}
	}
	return ""
}

func storageRepairActionAllowed(kind string, action string) bool {
	for _, allowed := range sarecord.RepairPolicyForKind(kind).Actions {
		if action == allowed {
			return true
		}
	}
	return false
}
