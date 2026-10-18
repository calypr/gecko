package chain

import (
	"fmt"
	"strings"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/storageaudit/model"
)

func buildSyfonOriginChainFindings(index sarecord.ChainIndex, acc *chainAuditAccumulator, countCompleteFromSyfon bool) {
	for _, record := range index.AllRecords {
		gitPaths := sarecord.UniqueStrings(index.RepoPathsByChecksum[sarecord.NormalizeAnalyticsChecksum(record.Checksum)])
		bucketMatches := sarecord.MatchedBucketObjectURLs(record, index.BucketObjectsByURL)
		classification := sarecord.ClassifyStorageFinding(record, index.BucketObjectsByURL)
		if len(gitPaths) == 0 && len(bucketMatches) > 0 && (classification == sarecord.FindingNone || classification == sarecord.FindingValidationMismatch) {
			if samePathGitPaths := repoPathsForBucketURLs(index.RepoPathsByBucketURL, bucketMatches); len(samePathGitPaths) > 0 {
				findings := buildSamePathChecksumConflictFindings(index, record, samePathGitPaths, bucketMatches)
				acc.add("git_syfon_metadata_mismatch", findings...)
				continue
			}
		}
		switch classification {
		case sarecord.FindingBrokenBucketMap:
			findingRecord := sarecord.RepairableBrokenAccessRecord(record)
			if len(findingRecord.AccessProbes) == 0 {
				findingRecord = record
			}
			findings := buildChainRecordFindingsWithOptions("syfon_broken_bucket_mapping", findingRecord, gitPaths, bucketMatches, "Syfon access URL did not resolve through a configured bucket mapping.", true)
			acc.findings = append(acc.findings, findings...)
			acc.addCount("syfon_broken_bucket_mapping", len(findings))
		case sarecord.FindingObjectMissing:
			if len(gitPaths) > 0 {
				logMissingBucketCandidateDebug(record, gitPaths, bucketMatches, index.BucketObjectsByURL, acc)
				findings := buildChainRecordFindings("syfon_git_no_bucket", record, gitPaths, bucketMatches, "Git and Syfon matched, but the mapped bucket object does not exist.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("syfon_git_no_bucket", len(gitPaths))
				continue
			}
			acc.add("syfon_missing_bucket_object", buildChainRecordFindings("syfon_missing_bucket_object", record, nil, bucketMatches, "Syfon record points to a mapped bucket location, but the object does not exist.")...)
		case sarecord.FindingValidationMismatch:
			if len(gitPaths) > 0 {
				findings := buildChainRecordFindings("git_syfon_metadata_mismatch", record, gitPaths, bucketMatches, "Mapped bucket object exists, but object evidence disagrees with the Syfon/Git record.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("git_syfon_metadata_mismatch", len(gitPaths))
				continue
			}
			acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
		case sarecord.FindingBrokenAccessURL, sarecord.FindingProbeError:
			findings := buildChainRecordFindings("probe_error", record, gitPaths, bucketMatches, "Storage probes could not confirm this mapped object. Gecko does not treat it as missing; after review, you may delete the Syfon record.")
			acc.findings = append(acc.findings, findings...)
			acc.addCount("probe_error", sarecord.ChainPathCount(gitPaths))
		case sarecord.FindingNone:
			if countCompleteFromSyfon && len(gitPaths) > 0 && recordHasResolvedStorage(record, bucketMatches, index.BucketObjectsByURL) {
				acc.addCount("bucket_syfon_git_complete", len(gitPaths))
				continue
			}
			if len(gitPaths) == 0 && len(bucketMatches) > 0 {
				acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
			}
		}
	}
}

func repoPathsForBucketURLs(repoPathsByBucketURL map[string][]string, bucketURLs []string) []string {
	paths := make([]string, 0, len(bucketURLs))
	for _, bucketURL := range sarecord.UniqueStrings(bucketURLs) {
		paths = append(paths, repoPathsByBucketURL[bucketURL]...)
	}
	return sarecord.UniqueStrings(paths)
}

func hasSyfonRecordForBucketURLs(recordsByBucketURL map[string][]sarecord.State, bucketURLs []string) bool {
	for _, bucketURL := range sarecord.UniqueStrings(bucketURLs) {
		if len(recordsByBucketURL[bucketURL]) > 0 {
			return true
		}
	}
	return false
}

func buildSamePathChecksumConflictFindings(index sarecord.ChainIndex, record sarecord.State, gitPaths []string, bucketMatches []string) []model.ChainFinding {
	findings := buildChainRecordFindings(
		"git_syfon_metadata_mismatch",
		record,
		gitPaths,
		bucketMatches,
		"Git and Syfon map to the same bucket object path, but their checksums disagree. Do not delete this object automatically.",
	)
	for findingIndex := range findings {
		findings[findingIndex].Actionability = sarecord.ActionabilityInspectOnly
		findings[findingIndex].AvailableActions = []string{sarecord.ActionInspectEvidence}
		findings[findingIndex].DefaultAction = sarecord.ActionInspectEvidence
		findings[findingIndex].SupportsDryRun = false
		gitChecksum := index.RepoChecksumsByPath[sarecord.NormalizeRepoSubpath(findings[findingIndex].NormalizedPath)]
		findings[findingIndex].Error = "Git LFS and Syfon map to the same bucket object path but have different SHA-256 values."
		findings[findingIndex].SuggestedFix = fmt.Sprintf(
			"Git LFS pointer SHA-256 is %s; Syfon record SHA-256 is %s. Recompute the bucket object's SHA-256 and reconcile the stale side manually before changing Git, Syfon, or the bucket object.",
			gitChecksum,
			strings.TrimSpace(record.Checksum),
		)
	}
	return findings
}
