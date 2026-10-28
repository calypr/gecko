package cleanup

import (
	"context"
	"fmt"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

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
