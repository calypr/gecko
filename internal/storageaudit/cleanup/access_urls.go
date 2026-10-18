package cleanup

import (
	"strings"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

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
