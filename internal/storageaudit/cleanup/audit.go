package cleanup

import (
	"sort"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func BuildStorageCleanupAudit(gitSubpath string, inventory []model.RepoInventoryFile, storageView *sarecord.StorageView, selectedRepoPaths []string, checkStorage bool) (*model.CleanupAuditResponse, *AuditModel) {
	auditModel := buildCleanupAuditModel(gitSubpath, inventory, storageView.RecordsByChecksum, storageView.AllProjectRecords, storageView.BucketObjectsByURL, selectedRepoPaths, checkStorage)
	return &model.CleanupAuditResponse{
		Findings:             auditModel.PublicFindings,
		Summary:              auditModel.Summary,
		ExpectedPathCount:    auditModel.ExpectedPathCount,
		IncludesRepoManifest: auditModel.IncludesRepoManifest,
		PathPrefix:           auditModel.PathPrefix,
	}, auditModel
}

func buildCleanupAuditModel(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, selectedRepoPaths []string, checkStorage bool) *AuditModel {
	allowed := make(map[string]struct{}, len(selectedRepoPaths))
	for _, path := range selectedRepoPaths {
		if normalized := sarecord.NormalizeCleanupSelectionKey(path); normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	includePath := func(paths ...string) bool {
		if len(allowed) == 0 {
			return true
		}
		for _, path := range paths {
			if _, ok := allowed[sarecord.NormalizeCleanupSelectionKey(path)]; ok {
				return true
			}
		}
		return false
	}
	findings := make([]FindingModel, 0)
	countsByKind := map[string]int{
		"bucket_only_object":          0,
		"stale_duplicate_record":      0,
		"live_duplicate_conflict":     0,
		"broken_access_url_error":     0,
		"broken_bucket_mapping":       0,
		"repo_orphan_live_object":     0,
		"repo_orphan_stale_record":    0,
		"storage_object_missing":      0,
		"storage_validation_mismatch": 0,
		"storage_probe_error":         0,
		"unknown":                     0,
	}
	repoChecksums := make(map[string]struct{}, len(inventory))
	referencedBucketURLs := map[string]struct{}{}
	for _, item := range inventory {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		repoChecksums[normalizedChecksum] = struct{}{}
		matches := recordsByChecksum[normalizedChecksum]
		if !includePath(item.RepoPath) || len(matches) == 0 {
			continue
		}
		if len(matches) > 1 {
			sortedMatches := append([]sarecord.State(nil), matches...)
			sort.SliceStable(sortedMatches, func(i, j int) bool {
				return sarecord.CompareRecordState(sortedMatches[i], sortedMatches[j]) > 0
			})
			if sarecord.CompareRecordState(sortedMatches[0], sortedMatches[1]) == 0 {
				public := sarecord.BuildCleanupFinding("live_duplicate_conflict", item.RepoPath, sortedMatches, false, "record", "Manual review required for live duplicate records.")
				findings = append(findings, FindingModel{Public: public, Manual: true})
				countsByKind["live_duplicate_conflict"]++
			} else {
				deleteIDs := make([]string, 0, len(sortedMatches)-1)
				for _, record := range sortedMatches[1:] {
					deleteIDs = append(deleteIDs, record.ObjectID)
				}
				public := sarecord.BuildCleanupFinding("stale_duplicate_record", item.RepoPath, sortedMatches, true, "record", "Delete stale duplicate records")
				findings = append(findings, FindingModel{Public: public, DeleteObjectIDs: deleteIDs})
				countsByKind["stale_duplicate_record"]++
			}
			continue
		}
		if checkStorage {
			for _, bucketURL := range sarecord.MatchedBucketObjectURLs(matches[0], bucketObjectsByURL) {
				referencedBucketURLs[bucketURL] = struct{}{}
			}
			switch storageFindingKind := sarecord.ClassifyStorageFinding(matches[0], bucketObjectsByURL); storageFindingKind {
			case sarecord.FindingBrokenAccessURL:
				public := sarecord.BuildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Manual review required for broken access URLs")
				findings = append(findings, FindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			case sarecord.FindingBrokenBucketMap:
				repairMatches := make([]sarecord.State, 0, len(matches))
				for _, match := range matches {
					repairMatches = append(repairMatches, sarecord.RepairableBrokenAccessRecord(match))
				}
				public := sarecord.BuildCleanupFinding(string(storageFindingKind), item.RepoPath, repairMatches, false, "access_url", "Syfon access URL did not resolve through a configured bucket mapping.")
				repairDeleteIDs, repairUpdates := sarecord.BrokenBucketMappingRepairPlan(repairMatches)
				findings = append(findings, FindingModel{
					Public:              public,
					DeleteObjectIDs:     repairDeleteIDs,
					UpdateAccessMethods: repairUpdates,
					Manual:              len(repairDeleteIDs) == 0 && len(repairUpdates) == 0,
				})
				countsByKind[string(storageFindingKind)]++
			case sarecord.FindingObjectMissing:
				public := sarecord.BuildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Storage object is missing for this Syfon access URL")
				findings = append(findings, FindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			case sarecord.FindingValidationMismatch:
				public := sarecord.BuildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Mapped bucket object exists, but object evidence disagrees with the Syfon/Git record.")
				findings = append(findings, FindingModel{
					Public:              public,
					DeleteObjectIDs:     sarecord.RecordObjectIDs(matches),
					DeleteBucketObjects: sarecord.CleanupMatchedBucketObjectURLs(matches, bucketObjectsByURL),
					Manual:              true,
				})
				countsByKind[string(storageFindingKind)]++
			case sarecord.FindingProbeError:
				public := sarecord.BuildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Manual review required for storage probe errors")
				findings = append(findings, FindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			}
		}
	}
	for checksum, matches := range allProjectRecords {
		if len(matches) == 0 {
			continue
		}
		if _, ok := repoChecksums[checksum]; ok {
			continue
		}
		displayPath := sarecord.OrphanDisplayPath(checksum, sarecord.RecordSourcePaths(matches))
		if !includePath(sarecord.CleanupSelectionCandidatesForRecords(checksum, displayPath, matches)...) {
			continue
		}
		kind := "repo_orphan_stale_record"
		if checkStorage && sarecord.RecordsReferenceBucketObject(matches, bucketObjectsByURL) {
			kind = "repo_orphan_live_object"
		} else if sarecord.RecordsContainLiveUsage(matches) {
			kind = "repo_orphan_live_object"
		}
		if checkStorage {
			for _, match := range matches {
				for _, bucketURL := range sarecord.MatchedBucketObjectURLs(match, bucketObjectsByURL) {
					referencedBucketURLs[bucketURL] = struct{}{}
				}
			}
		}
		public := sarecord.BuildCleanupFinding(kind, displayPath, matches, true, "record", sarecord.RepoOrphanAction(kind))
		findings = append(findings, FindingModel{
			Public:            public,
			DeleteObjectIDs:   sarecord.RecordObjectIDs(matches),
			DeleteStorageData: kind == "repo_orphan_live_object",
		})
		countsByKind[kind]++
	}
	if checkStorage {
		bucketURLs := make([]string, 0, len(bucketObjectsByURL))
		for objectURL := range bucketObjectsByURL {
			bucketURLs = append(bucketURLs, objectURL)
		}
		sort.Strings(bucketURLs)
		for _, objectURL := range bucketURLs {
			if _, ok := referencedBucketURLs[objectURL]; ok {
				continue
			}
			if !includePath(sarecord.CleanupSelectionCandidatesForBucketObject(objectURL, bucketObjectsByURL[objectURL])...) {
				continue
			}
			public := sarecord.BuildBucketOnlyFinding(bucketObjectsByURL[objectURL])
			findings = append(findings, FindingModel{
				Public:              public,
				DeleteBucketObjects: []string{objectURL},
			})
			countsByKind["bucket_only_object"]++
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		return findings[i].Public.NormalizedPath < findings[j].Public.NormalizedPath
	})
	publicFindings := make([]model.CleanupFinding, 0, len(findings))
	repoDeleteCandidateCount := 0
	manualFindingCount := 0
	repoOrphanCount := 0
	staleDuplicateCount := 0
	for _, finding := range findings {
		publicFindings = append(publicFindings, finding.Public)
		if finding.Public.RepoDeleteCandidate {
			repoDeleteCandidateCount++
		}
		if finding.Manual {
			manualFindingCount++
		}
		if finding.Public.Kind == "repo_orphan_live_object" || finding.Public.Kind == "repo_orphan_stale_record" {
			repoOrphanCount++
		}
		if finding.Public.Kind == "stale_duplicate_record" {
			staleDuplicateCount++
		}
	}
	return &AuditModel{
		Findings:       findings,
		PublicFindings: publicFindings,
		Summary: model.CleanupAuditSummary{
			CountsByKind:             countsByKind,
			TotalFindings:            len(publicFindings),
			ManualFindingCount:       manualFindingCount,
			RepoDeleteCandidateCount: repoDeleteCandidateCount,
			StaleDuplicateCount:      staleDuplicateCount,
			RepoOrphanCount:          repoOrphanCount,
		},
		ExpectedPathCount:    len(inventory),
		IncludesRepoManifest: true,
		PathPrefix:           sarecord.NormalizeRepoSubpath(gitSubpath),
	}
}
