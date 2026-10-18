package chain

import (
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func buildGitOriginChainFindings(index sarecord.ChainIndex, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, scopes []domain.StorageBucketScope, organization string, project string, acc *chainAuditAccumulator) {
	for _, item := range index.Inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if len(allProjectRecords[checksum]) > 0 || len(recordsByChecksum[checksum]) > 0 {
			continue
		}
		bucketURLs := sarecord.ProjectScopeRepoPathObjectURLs([]string{item.RepoPath}, scopes, organization, project)
		if hasSyfonRecordForBucketURLs(index.RecordsByBucketURL, bucketURLs) {
			// The Syfon-origin pass reports an exact-path checksum conflict as a
			// metadata mismatch. Do not also offer this Git pointer for Syfon
			// creation just because its checksum differs.
			continue
		}
		bucketObjectURL := ""
		bucketSizeBytes := int64(0)
		bucketEvaluation := "not_evaluated"
		if len(bucketURLs) == 1 {
			bucketObjectURL = bucketURLs[0]
			if bucketObject, ok := index.BucketObjectsByURL[bucketObjectURL]; ok {
				bucketSizeBytes = bucketObject.SizeBytes
				bucketEvaluation = "present"
			} else {
				bucketEvaluation = "not_found"
			}
		} else if len(bucketURLs) > 1 {
			bucketEvaluation = "ambiguous_mapping"
		}
		actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("git_only_no_syfon", "verified")
		recommendedAction := "Git checksum has no matching Syfon record. Bucket presence is not claimed by this finding."
		if bucketEvaluation == "present" && bucketSizeBytes == item.Size {
			recommendedAction = "Git checksum has no matching Syfon record. The mapped bucket object exists and its byte size matches the Git LFS pointer; create a Syfon record after reviewing that RGW did not provide a SHA-256 for verification."
		} else if bucketEvaluation == "present" {
			recommendedAction = "Git checksum has no matching Syfon record. The mapped bucket object exists but its byte size differs from the Git LFS pointer, so it is not safe to create a Syfon record."
		}
		acc.add("git_only_no_syfon", model.ChainFinding{
			Kind:              "git_only_no_syfon",
			EvidenceStatus:    "verified",
			NormalizedPath:    item.RepoPath,
			Checksum:          checksum,
			SourcePaths:       []string{item.RepoPath},
			ObjectIDs:         []string{},
			RecordCount:       0,
			SizeBytes:         item.Size,
			BucketObjectURL:   bucketObjectURL,
			BucketSizeBytes:   bucketSizeBytes,
			RecommendedAction: recommendedAction,
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence: &model.Evidence{
				Checksum:         checksum,
				SourcePaths:      []string{item.RepoPath},
				ObjectIDs:        []string{},
				BucketObjectURLs: bucketURLs,
				BucketEvaluation: bucketEvaluation,
			},
		})
	}
}
