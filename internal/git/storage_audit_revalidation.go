package git

import (
	"context"
	"fmt"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

const (
	cleanupExpectationPresent = "present"
	cleanupExpectationMissing = "missing"
)

type storageCleanupVerification struct {
	FindingKind    string
	NormalizedPath string
	ObjectURL      string
	Expectation    string
}

func appendStorageCleanupVerification(plan *storageCleanupApplyPlan, finding GitStorageCleanupApplyFinding, action string) error {
	if plan == nil || action == storageActionInspectEvidence {
		return nil
	}
	expectation := cleanupVerificationExpectation(finding.Kind, action)
	if expectation == "" {
		return nil
	}
	objectURLs := storageApplyFindingBucketObjectURLs(finding)
	if len(objectURLs) == 0 {
		return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs required for exact revalidation", finding.Kind, finding.NormalizedPath)
	}
	for _, objectURL := range objectURLs {
		plan.Verifications = append(plan.Verifications, storageCleanupVerification{
			FindingKind:    strings.TrimSpace(finding.Kind),
			NormalizedPath: strings.TrimSpace(finding.NormalizedPath),
			ObjectURL:      strings.TrimSpace(objectURL),
			Expectation:    expectation,
		})
	}
	return nil
}

func cleanupVerificationExpectation(kind string, action string) string {
	switch action {
	case storageActionDeleteBucketObject, storageActionDeleteBoth:
		return cleanupExpectationPresent
	case storageActionDeleteSyfonRecord:
		switch strings.TrimSpace(kind) {
		case "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
			return cleanupExpectationMissing
		}
	}
	return ""
}

func (service *StorageAnalyticsService) verifyStorageCleanupApplyPlan(ctx context.Context, authorizationHeader string, plan *storageCleanupApplyPlan) error {
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
	results, err := service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, items)
	if err != nil {
		return fmt.Errorf("revalidate cleanup storage evidence: %w", err)
	}
	if len(results) != len(items) {
		return fmt.Errorf("revalidate cleanup storage evidence: expected %d results, received %d", len(items), len(results))
	}
	for index, result := range results {
		if cleanupObjectVanishedAfterAudit(verifications[index], result) {
			plan.DeleteBucketObjects = differenceStrings(plan.DeleteBucketObjects, []string{verifications[index].ObjectURL})
			plan.SkippedPaths = append(plan.SkippedPaths, verifications[index].NormalizedPath)
			continue
		}
		if err := validateStorageCleanupResult(verifications[index], result); err != nil {
			return err
		}
	}
	return nil
}

func cleanupObjectVanishedAfterAudit(verification storageCleanupVerification, result gintegrationsyfon.BulkStorageProbeResult) bool {
	if verification.Expectation != cleanupExpectationPresent {
		return false
	}
	if strings.TrimSpace(result.Status) != "not_found" || strings.TrimSpace(result.ErrorKind) != "object_not_found" {
		return false
	}
	switch strings.TrimSpace(verification.FindingKind) {
	case "bucket_only_object", "bucket_syfon_no_git":
		return true
	default:
		return false
	}
}

func uniqueStorageCleanupVerifications(input []storageCleanupVerification) []storageCleanupVerification {
	seen := make(map[string]struct{}, len(input))
	out := make([]storageCleanupVerification, 0, len(input))
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

func validateStorageCleanupResult(verification storageCleanupVerification, result gintegrationsyfon.BulkStorageProbeResult) error {
	status := strings.TrimSpace(result.Status)
	switch verification.Expectation {
	case cleanupExpectationPresent:
		if status == "present" && result.Exists {
			return nil
		}
	case cleanupExpectationMissing:
		if status == "not_found" && strings.TrimSpace(result.ErrorKind) == "object_not_found" {
			return nil
		}
	}
	return fmt.Errorf(
		"revalidate cleanup finding %q for %q: expected %s, got status=%q error_kind=%q error=%q",
		verification.FindingKind,
		verification.ObjectURL,
		verification.Expectation,
		status,
		strings.TrimSpace(result.ErrorKind),
		strings.TrimSpace(result.Error),
	)
}
