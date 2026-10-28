package chain

import (
	"strings"
	"unicode/utf8"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/storageaudit/model"
)

// Grouping keys use the same replacement of invalid UTF-8 bytes as JSON encoding.
func storageChainAuditJSONString(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	return string([]rune(value))
}

func filterStorageChainInventory(inventory []model.RepoInventoryFile, gitSubpath string) []model.RepoInventoryFile {
	pathPrefix := sarecord.NormalizeRepoSubpath(gitSubpath)
	if pathPrefix == "" {
		return append([]model.RepoInventoryFile(nil), inventory...)
	}
	filtered := make([]model.RepoInventoryFile, 0)
	for _, file := range inventory {
		path := sarecord.NormalizeRepoSubpath(file.RepoPath)
		if path == pathPrefix || strings.HasPrefix(path, pathPrefix+"/") {
			filtered = append(filtered, file)
		}
	}
	return filtered
}

func storageChainAuditResponseForSubpathView(base model.ChainAuditResponse, gitSubpath string, inventory []model.RepoInventoryFile) model.ChainAuditResponse {
	pathPrefix := sarecord.NormalizeRepoSubpath(gitSubpath)
	filteredFindings := make([]model.ChainFinding, 0, len(base.Findings))
	for _, finding := range base.Findings {
		if storageChainFindingMatchesSubpath(finding, pathPrefix) {
			filteredFindings = append(filteredFindings, finding)
		}
	}

	summary := base.Summary
	countsByKind := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		countsByKind[kind] = 0
	}
	issueGitPaths := make(map[string]struct{})
	objectIDs := make(map[string]struct{})
	bucketObjects := make(map[string]struct{})
	for _, finding := range filteredFindings {
		countsByKind[finding.Kind]++
		if storageChainFindingHasGitPath(finding.Kind) {
			for _, sourcePath := range finding.SourcePaths {
				if storagePathMatchesRepoSubpath(sourcePath, pathPrefix) && !strings.Contains(sourcePath, "://") {
					issueGitPaths[sarecord.NormalizeRepoSubpath(sourcePath)] = struct{}{}
				}
			}
			if len(finding.SourcePaths) == 0 && storagePathMatchesRepoSubpath(finding.NormalizedPath, pathPrefix) && !strings.Contains(finding.NormalizedPath, "://") {
				issueGitPaths[sarecord.NormalizeRepoSubpath(finding.NormalizedPath)] = struct{}{}
			}
		}
		for _, objectID := range finding.ObjectIDs {
			if trimmed := strings.TrimSpace(objectID); trimmed != "" {
				objectIDs[trimmed] = struct{}{}
			}
		}
		if trimmed := strings.TrimSpace(finding.BucketObjectURL); trimmed != "" {
			bucketObjects[trimmed] = struct{}{}
		}
		for _, accessURL := range finding.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				bucketObjects[trimmed] = struct{}{}
			}
		}
	}
	gitTrackedFileCount := len(inventory)
	completeCount := gitTrackedFileCount - len(issueGitPaths)
	if completeCount < 0 {
		completeCount = 0
	}
	countsByKind["bucket_syfon_git_complete"] = completeCount
	summary.CountsByKind = countsByKind
	summary.TotalFindings = len(filteredFindings)
	summary.GitTrackedFileCount = gitTrackedFileCount
	summary.SyfonRecordCount = completeCount + len(objectIDs)
	summary.BucketObjectCount = completeCount + len(bucketObjects)

	summary.ReturnedFindings = len(filteredFindings)
	summary.FindingLimit = -1
	summary.FindingsTruncated = false
	projected := base
	projected.Findings = filteredFindings
	projected.Groups = summarizeChainIssueGroups(filteredFindings)
	projected.Summary = summary
	projected.PathPrefix = pathPrefix
	projected.BucketPathPrefix = ""
	return projected
}

func storageChainFindingHasGitPath(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "git_only_no_syfon", "syfon_git_no_bucket", "git_syfon_metadata_mismatch", "probe_error":
		return true
	}
	return false
}

func projectStorageChainAuditResponse(base model.ChainAuditResponse, findingKind string, findingLimit int) *model.ChainAuditResponse {
	filteredFindings := filterStorageChainFindingsByKind(base.Findings, findingKind)
	summary := filterStorageChainSummary(base.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, findingLimit, findingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = findingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	response := base
	response.Findings = findings
	response.Groups = summarizeChainIssueGroups(filteredFindings)
	response.Summary = summary
	cloned := cloneStorageChainAuditResponse(response)
	return &cloned
}

func storageChainFindingMatchesSubpath(finding model.ChainFinding, pathPrefix string) bool {
	if pathPrefix == "" {
		return true
	}
	candidates := []string{finding.NormalizedPath, finding.BucketObjectURL, finding.ResolvedKey}
	candidates = append(candidates, finding.SourcePaths...)
	candidates = append(candidates, finding.AccessURLs...)
	for _, record := range finding.Records {
		candidates = append(candidates, record.NormalizedPath)
		candidates = append(candidates, record.AccessURLs...)
		for _, accessMethod := range record.AccessMethods {
			candidates = append(candidates, accessMethod.URL)
		}
		for _, probe := range record.AccessProbes {
			candidates = append(candidates, probe.URL, probe.Path, probe.Key)
		}
	}
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.SourcePaths...)
		candidates = append(candidates, finding.Evidence.AccessURLs...)
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.Keys...)
	}
	for _, candidate := range candidates {
		if storagePathMatchesRepoSubpath(candidate, pathPrefix) {
			return true
		}
	}
	return false
}

func storagePathMatchesRepoSubpath(raw string, pathPrefix string) bool {
	pathPrefix = sarecord.NormalizeRepoSubpath(pathPrefix)
	if pathPrefix == "" {
		return true
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return false
	}
	if _, key, ok := sarecord.ParseStorageURL(value); ok {
		key = sarecord.NormalizeRepoSubpath(key)
		return key == pathPrefix || strings.HasPrefix(key, pathPrefix+"/") || strings.Contains("/"+key, "/"+pathPrefix+"/")
	}
	normalized := sarecord.NormalizeRepoSubpath(value)
	return normalized == pathPrefix || strings.HasPrefix(normalized, pathPrefix+"/") || strings.Contains("/"+normalized, "/"+pathPrefix+"/")
}

func filterStorageChainFindingsByKind(findings []model.ChainFinding, kind string) []model.ChainFinding {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return findings
	}
	filtered := make([]model.ChainFinding, 0, len(findings))
	for _, finding := range findings {
		if storageChainAuditJSONString(finding.Kind) == kind {
			filtered = append(filtered, finding)
		}
	}
	return filtered
}

func filterStorageChainSummary(summary model.ChainSummary, findings []model.ChainFinding) model.ChainSummary {
	filteredCounts := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		filteredCounts[storageChainAuditJSONString(kind)] = 0
	}
	filteredCounts["bucket_syfon_git_complete"] = summary.CountsByKind["bucket_syfon_git_complete"]
	for _, finding := range findings {
		filteredCounts[storageChainAuditJSONString(finding.Kind)]++
	}
	summary.CountsByKind = filteredCounts
	summary.TotalFindings = len(findings)
	return summary
}

func limitStorageChainFindings(findings []model.ChainFinding, limit int, kind string) []model.ChainFinding {
	if limit <= 0 || limit >= len(findings) {
		return append([]model.ChainFinding(nil), findings...)
	}
	if strings.TrimSpace(kind) == "" {
		limited := make([]model.ChainFinding, 0, len(findings))
		countsByKind := make(map[string]int)
		for _, finding := range findings {
			kind := storageChainAuditJSONString(finding.Kind)
			if countsByKind[kind] >= limit {
				continue
			}
			limited = append(limited, finding)
			countsByKind[kind]++
		}
		return limited
	}
	return append([]model.ChainFinding(nil), findings[:limit]...)
}
