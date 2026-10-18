package chain

import (
	"fmt"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"log"
	"path"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
)

func buildStorageChainIndex(inventory []model.RepoInventoryFile, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string) sarecord.ChainIndex {
	repoPathsByChecksum := make(map[string][]string, len(inventory))
	repoPathsByBucketURL := make(map[string][]string, len(inventory))
	repoChecksumsByPath := make(map[string]string, len(inventory))
	for _, item := range inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		repoPath := sarecord.NormalizeRepoSubpath(item.RepoPath)
		if repoPath == "" {
			continue
		}
		repoPathsByChecksum[checksum] = append(repoPathsByChecksum[checksum], repoPath)
		repoChecksumsByPath[repoPath] = checksum
		for _, bucketURL := range sarecord.ProjectScopeRepoPathObjectURLs([]string{repoPath}, scopes, organization, project) {
			repoPathsByBucketURL[bucketURL] = append(repoPathsByBucketURL[bucketURL], repoPath)
		}
	}
	allRecords := sarecord.FlattenRecordStates(allProjectRecords)
	recordsByBucketURL := make(map[string][]sarecord.State)
	for _, record := range allRecords {
		for _, bucketURL := range sarecord.RecordBucketURLs(record) {
			recordsByBucketURL[bucketURL] = append(recordsByBucketURL[bucketURL], record)
		}
	}
	return sarecord.ChainIndex{
		Inventory:            inventory,
		AllRecords:           allRecords,
		BucketObjectsByURL:   bucketObjectsByURL,
		RepoPathsByChecksum:  repoPathsByChecksum,
		RepoPathsByBucketURL: repoPathsByBucketURL,
		RepoChecksumsByPath:  repoChecksumsByPath,
		RecordsByBucketURL:   recordsByBucketURL,
		EquivalentRecordKeys: buildEquivalentRecordKeyIndex(allRecords),
	}
}

func buildStorageChainAuditModel(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string, includeBucketOrigin bool) *chainAuditModel {
	index := buildStorageChainIndex(inventory, allProjectRecords, bucketObjectsByURL, scopes, organization, project)
	acc := chainAuditAccumulator{
		findings: make([]model.ChainFinding, 0),
		summary:  newChainSummary(len(bucketObjectsByURL), len(index.AllRecords), len(inventory)),
	}
	if includeBucketOrigin {
		buildBucketOriginChainFindings(index, &acc)
	}
	buildSyfonOriginChainFindings(index, &acc, !includeBucketOrigin)
	buildGitOriginChainFindings(index, recordsByChecksum, allProjectRecords, scopes, organization, project, &acc)
	return finalizeChainFindings(gitSubpath, acc)
}

func buildBucketOriginChainFindings(index sarecord.ChainIndex, acc *chainAuditAccumulator) {
	bucketURLs := make([]string, 0, len(index.BucketObjectsByURL))
	for bucketURL := range index.BucketObjectsByURL {
		bucketURLs = append(bucketURLs, bucketURL)
	}
	sort.Strings(bucketURLs)
	for _, bucketURL := range bucketURLs {
		item := index.BucketObjectsByURL[bucketURL]
		if sarecord.BucketObjectHasCompleteChain(index.RecordsByBucketURL[bucketURL], index.RepoPathsByChecksum, index.BucketObjectsByURL) {
			acc.addCount("bucket_syfon_git_complete", 1)
			continue
		}
		if len(index.RecordsByBucketURL[bucketURL]) > 0 {
			// An exact Syfon-to-bucket URL match is not a bucket orphan, even
			// when the Git and Syfon checksums disagree. The Syfon-origin pass
			// reports that conflict without offering the bucket for deletion.
			continue
		}
		if bucketObjectHasEquivalentSyfonRecord(item, index.EquivalentRecordKeys) {
			continue
		}
		acc.add("bucket_only_object", buildChainBucketOnlyFinding(item))
	}
}

func buildEquivalentRecordKeyIndex(records []sarecord.State) sarecord.EquivalentRecordKeyIndex {
	index := sarecord.EquivalentRecordKeyIndex{
		ByName:            make(map[string]struct{}),
		ByNameSize:        make(map[string]map[int64]struct{}),
		UnknownSizeByName: make(map[string]struct{}),
	}
	for _, record := range records {
		for _, accessURL := range sarecord.RecordStorageCandidateURLs(record) {
			_, key, ok := sarecord.ParseStorageURL(accessURL)
			if !ok {
				continue
			}
			name := strings.TrimSpace(path.Base(key))
			if name == "" {
				continue
			}
			index.ByName[name] = struct{}{}
			if record.Size <= 0 {
				index.UnknownSizeByName[name] = struct{}{}
				continue
			}
			if index.ByNameSize[name] == nil {
				index.ByNameSize[name] = make(map[int64]struct{})
			}
			index.ByNameSize[name][record.Size] = struct{}{}
		}
	}
	return index
}

func bucketObjectHasEquivalentSyfonRecord(item gintegrationsyfon.ProjectBucketObject, index sarecord.EquivalentRecordKeyIndex) bool {
	itemName := strings.TrimSpace(path.Base(strings.TrimSpace(item.Key)))
	if itemName == "" {
		return false
	}
	if _, ok := index.ByName[itemName]; !ok {
		return false
	}
	if item.SizeBytes <= 0 {
		return true
	}
	if _, ok := index.UnknownSizeByName[itemName]; ok {
		return true
	}
	if sizes := index.ByNameSize[itemName]; sizes != nil {
		_, ok := sizes[item.SizeBytes]
		return ok
	}
	return false
}

func recordHasResolvedStorage(record sarecord.State, bucketMatches []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(bucketMatches) > 0 {
		return true
	}
	return sarecord.ResolveRecordStorage(record, bucketObjectsByURL).HasPresentProbe
}

func logMissingBucketCandidateDebug(record sarecord.State, gitPaths []string, bucketMatches []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, acc *chainAuditAccumulator) {
	if acc == nil || acc.missingBucketDebugLogCount >= storageChainValidationDebugLimit {
		return
	}
	resolution := sarecord.ResolveRecordStorage(record, bucketObjectsByURL)
	log.Printf(
		"INFO: storage_chain_missing_bucket_candidates object_id=%s checksum=%s name=%q git_paths=%q raw_access_urls=%q canonical_access_urls=%q candidate_status=%q bucket_matches=%q access_probe_status=%q",
		strings.TrimSpace(record.ObjectID),
		strings.TrimSpace(record.Checksum),
		strings.TrimSpace(record.Name),
		strings.Join(sarecord.UniqueStrings(gitPaths), ","),
		strings.Join(sarecord.RawAccessURLsForRecord(record), ","),
		strings.Join(sarecord.UniqueStrings(record.CanonicalAccessURLs), ","),
		strings.Join(storageCandidateStatuses(resolution.CandidateURLs, bucketObjectsByURL), ","),
		strings.Join(sarecord.UniqueStrings(bucketMatches), ","),
		strings.Join(storageProbeDebugStatuses(record.AccessProbes), ","),
	)
	acc.missingBucketDebugLogCount++
}

func storageCandidateStatuses(candidateURLs []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	out := make([]string, 0, len(candidateURLs))
	for _, candidate := range sarecord.UniqueStrings(candidateURLs) {
		status := "missing"
		if _, ok := bucketObjectsByURL[candidate]; ok {
			status = "found"
		}
		out = append(out, candidate+"="+status)
	}
	return out
}

func storageProbeDebugStatuses(probes []gintegrationsyfon.BulkStorageProbeResult) []string {
	out := make([]string, 0, len(probes))
	for _, probe := range probes {
		objectURL := sarecord.SyfonProbeObjectURL(probe)
		if objectURL == "" {
			objectURL = strings.TrimSpace(probe.ObjectURL)
		}
		status := strings.TrimSpace(probe.Status)
		if status == "" {
			status = "unknown"
		}
		validation := strings.TrimSpace(probe.ValidationStatus)
		if validation != "" {
			status += "/" + validation
		}
		if errKind := strings.TrimSpace(probe.ErrorKind); errKind != "" {
			status += "/" + errKind
		}
		out = append(out, objectURL+"="+status)
	}
	return out
}

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

type storageAuditBaseInputs struct {
	index             *repoindex.Index
	inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

func newChainSummary(bucketObjectCount, syfonRecordCount, gitTrackedFileCount int) model.ChainSummary {
	return model.ChainSummary{
		CountsByKind: map[string]int{
			"bucket_only_object":          0,
			"bucket_syfon_no_git":         0,
			"bucket_syfon_git_complete":   0,
			"syfon_broken_bucket_mapping": 0,
			"syfon_missing_bucket_object": 0,
			"syfon_git_no_bucket":         0,
			"git_only_no_syfon":           0,
			"git_syfon_metadata_mismatch": 0,
			"probe_error":                 0,
		},
		BucketObjectCount:        bucketObjectCount,
		SyfonRecordCount:         syfonRecordCount,
		GitTrackedFileCount:      gitTrackedFileCount,
		BucketInventoryAvailable: true,
	}
}

func (acc *chainAuditAccumulator) add(kind string, findings ...model.ChainFinding) {
	acc.findings = append(acc.findings, findings...)
	acc.summary.CountsByKind[kind] += len(findings)
}

func (acc *chainAuditAccumulator) addCount(kind string, count int) {
	acc.summary.CountsByKind[kind] += count
}

func finalizeChainFindings(gitSubpath string, acc chainAuditAccumulator) *chainAuditModel {
	sort.Slice(acc.findings, func(i, j int) bool {
		if acc.findings[i].Kind == acc.findings[j].Kind {
			return acc.findings[i].NormalizedPath < acc.findings[j].NormalizedPath
		}
		return acc.findings[i].Kind < acc.findings[j].Kind
	})
	acc.summary.TotalFindings = len(acc.findings)
	return &chainAuditModel{
		Findings:   acc.findings,
		Summary:    acc.summary,
		PathPrefix: sarecord.NormalizeRepoSubpath(gitSubpath),
	}
}

func summarizeChainIssueGroups(findings []model.ChainFinding) []model.ChainIssueGroup {
	groups := make(map[string]*model.ChainIssueGroup)
	groupPaths := make(map[string]map[string]struct{})
	groupObjects := make(map[string]map[string]struct{})
	for _, finding := range findings {
		group := groups[finding.Kind]
		if group == nil {
			group = &model.ChainIssueGroup{Kind: finding.Kind}
			groups[finding.Kind] = group
			groupPaths[finding.Kind] = map[string]struct{}{}
			groupObjects[finding.Kind] = map[string]struct{}{}
		}
		group.FindingCount++
		group.RecordCount += finding.RecordCount
		group.TotalBytes += finding.SizeBytes
		groupPaths[finding.Kind][finding.NormalizedPath] = struct{}{}
		for _, objectID := range finding.ObjectIDs {
			groupObjects[finding.Kind][objectID] = struct{}{}
		}
	}
	kinds := make([]string, 0, len(groups))
	for kind := range groups {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	out := make([]model.ChainIssueGroup, 0, len(kinds))
	for _, kind := range kinds {
		group := *groups[kind]
		group.PathCount = len(groupPaths[kind])
		group.ObjectCount = len(groupObjects[kind])
		out = append(out, group)
	}
	return out
}

func buildChainBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) model.ChainFinding {
	objectURL := sarecord.CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence("bucket_only_object", "verified")
	return model.ChainFinding{
		Kind:              "bucket_only_object",
		EvidenceStatus:    "verified",
		NormalizedPath:    objectURL,
		ObjectIDs:         []string{},
		AccessURLs:        []string{objectURL},
		BucketObjectURL:   objectURL,
		ResolvedBucket:    strings.TrimSpace(item.Bucket),
		ResolvedKey:       strings.TrimSpace(item.Key),
		ProbeStatus:       "enumerated",
		RecordCount:       0,
		SizeBytes:         item.SizeBytes,
		RecommendedAction: "Bucket object exists, but no Syfon record matched it.",
		Actionability:     actionability,
		AvailableActions:  availableActions,
		DefaultAction:     defaultAction,
		SupportsDryRun:    supportsDryRun,
		Evidence: &model.Evidence{
			AccessURLs:        []string{objectURL},
			BucketObjectURLs:  []string{objectURL},
			Buckets:           sarecord.UniqueStrings([]string{item.Bucket}),
			Keys:              sarecord.UniqueStrings([]string{item.Key}),
			StorageOperations: []string{model.StorageChainValidationModeInventory},
			ProbeStatuses:     []string{"enumerated"},
			BucketEvaluation:  "enumerated",
		},
	}
}

func storageChainActionSupportForEvidence(kind string, evidenceStatus string) (string, []string, string, bool) {
	if strings.TrimSpace(kind) == "syfon_broken_bucket_mapping" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionRemoveBrokenAccessURLs, sarecord.ActionInspectEvidence},
			sarecord.ActionInspectEvidence,
			true
	}
	if strings.TrimSpace(kind) == "git_only_no_syfon" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionCreateSyfonRecord, sarecord.ActionInspectEvidence},
			sarecord.ActionCreateSyfonRecord,
			false
	}
	if strings.TrimSpace(kind) == "git_syfon_metadata_mismatch" && strings.TrimSpace(evidenceStatus) == "verified" {
		return sarecord.ActionabilityManualChoice,
			[]string{sarecord.ActionDeleteSyfonRecord, sarecord.ActionDeleteBucketObject, sarecord.ActionDeleteBoth, sarecord.ActionInspectEvidence},
			"",
			true
	}
	repair := sarecord.RepairPolicyForKind(kind)
	if repair.Actionability == sarecord.ActionabilityAutoRepair && !sarecord.AllowsAutomaticRepair(kind, sarecord.EvidenceStatus(evidenceStatus)) {
		repair = sarecord.InspectOnlyRepairPolicy()
	}
	return repair.Actionability, append([]string(nil), repair.Actions...), repair.DefaultAction, repair.SupportsDryRun
}

func buildChainRecordFindings(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string) []model.ChainFinding {
	return buildChainRecordFindingsWithOptions(kind, record, gitPaths, bucketObjectURLs, action, false)
}

func buildChainRecordFindingsWithOptions(kind string, record sarecord.State, gitPaths []string, bucketObjectURLs []string, action string, preferSyfonRecordPath bool) []model.ChainFinding {
	paths := sarecord.UniqueStrings(gitPaths)
	if len(paths) == 0 {
		displayPath := sarecord.OrphanDisplayPath(strings.TrimSpace(record.Checksum), append(bucketObjectURLs, record.AccessURLs...))
		if preferSyfonRecordPath {
			displayPath = sarecord.SyfonRecordDisplayPath(record)
		}
		if displayPath == "" {
			displayPath = strings.TrimSpace(record.Checksum)
		}
		paths = []string{displayPath}
	}
	objectIDs := sarecord.UniqueStrings([]string{record.ObjectID})
	accessURLs := sarecord.UniqueStrings(record.AccessURLs)
	evidence := sarecord.BuildFindingEvidence(strings.TrimSpace(record.Checksum), gitPaths, []sarecord.State{record}, "enumerated_and_probed")
	if evidence != nil {
		evidence.BucketObjectURLs = sarecord.UniqueStrings(append(evidence.BucketObjectURLs, bucketObjectURLs...))
	}
	evidenceStatus := storageEvidenceStatus(record, len(bucketObjectURLs) > 0)
	actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupportForEvidence(kind, evidenceStatus)
	primaryProbe := selectChainProbe(record, bucketObjectURLs)
	suggestedFix := suggestedFixForChainFinding(kind, record)
	suggestedAction := suggestedActionForChainFinding(kind, record)
	if suggestedAction != "" && !stringSliceContains(availableActions, suggestedAction) {
		availableActions = append([]string(nil), availableActions...)
		availableActions = append(availableActions, suggestedAction)
	}
	findings := make([]model.ChainFinding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, model.ChainFinding{
			Kind:              kind,
			EvidenceStatus:    evidenceStatus,
			NormalizedPath:    path,
			Checksum:          strings.TrimSpace(record.Checksum),
			SourcePaths:       sarecord.UniqueStrings(gitPaths),
			ObjectIDs:         objectIDs,
			Records:           []model.CleanupRecordAudit{sarecord.CleanupRecordAuditForRecord(path, "access_url", record)},
			AccessURLs:        accessURLs,
			BucketObjectURL:   primaryProbe.bucketObjectURL,
			ResolvedBucket:    primaryProbe.probe.Bucket,
			ResolvedKey:       primaryProbe.probe.Key,
			ProbeStatus:       primaryProbe.probe.Status,
			ErrorKind:         primaryProbe.probe.ErrorKind,
			Error:             chainFindingError(kind, record, primaryProbe.probe),
			RecordCount:       1,
			SizeBytes:         record.Size,
			RecommendedAction: action,
			SuggestedFix:      suggestedFix,
			SuggestedAction:   suggestedAction,
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence:          evidence,
		})
	}
	return findings
}

func suggestedActionForChainFinding(kind string, record sarecord.State) string {
	return ""
}

func suggestedFixForChainFinding(kind string, record sarecord.State) string {
	normalizedKind := strings.TrimSpace(kind)
	switch normalizedKind {
	case "git_syfon_metadata_mismatch":
	case "syfon_broken_bucket_mapping":
		return ""
	default:
		return ""
	}
	mismatches := make([]string, 0, 2)
	if probe, ok := firstAccessProbeMismatch(record, "size_mismatch"); ok {
		bucketSize := int64(0)
		if probe.SizeBytes != nil {
			bucketSize = *probe.SizeBytes
		}
		mismatches = append(mismatches, fmt.Sprintf("Syfon record size is %s, bucket inventory reports %s", formatAuditSize(record.Size), formatAuditSize(bucketSize)))
	}
	if len(mismatches) == 0 {
		return "Bucket object exists, but its storage evidence does not match the Syfon/Git record. Review the record and bucket object before applying a destructive fix."
	}
	return strings.Join(mismatches, ". ") + ". Because the byte lengths differ, the bucket object cannot have the current Git/Syfon SHA-256. Recompute the bucket object's SHA-256 before manually reconciling Git and Syfon."
}

func firstAccessProbeMismatch(record sarecord.State, mismatch string) (model.CleanupAccessProbe, bool) {
	for _, probe := range sarecord.AccessProbesForRecord(record) {
		if accessProbeHasMismatch(probe, mismatch) {
			return probe, true
		}
	}
	return model.CleanupAccessProbe{}, false
}

func accessProbeHasMismatch(probe model.CleanupAccessProbe, mismatch string) bool {
	for _, item := range probe.ValidationMismatches {
		if strings.TrimSpace(item) == mismatch {
			return true
		}
	}
	return false
}

func formatAuditSize(size int64) string {
	if size <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%d B", size)
}

type chainProbeSelection struct {
	bucketObjectURL string
	probe           model.CleanupAccessProbe
}

func selectChainProbe(record sarecord.State, bucketObjectURLs []string) chainProbeSelection {
	probes := sarecord.AccessProbesForRecord(record)
	if len(bucketObjectURLs) > 0 {
		targets := map[string]struct{}{}
		for _, bucketURL := range bucketObjectURLs {
			targets[strings.TrimSpace(bucketURL)] = struct{}{}
		}
		for _, probe := range probes {
			objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if _, ok := targets[objectURL]; ok {
				return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
			}
		}
	}
	for _, probe := range probes {
		operation := strings.TrimSpace(probe.Operation)
		if operation != model.StorageChainValidationModeMetadata && operation != model.StorageChainValidationModeList {
			continue
		}
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	for _, probe := range probes {
		objectURL := sarecord.CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	return chainProbeSelection{}
}

func chainFindingError(kind string, record sarecord.State, probe model.CleanupAccessProbe) string {
	if trimmed := strings.TrimSpace(probe.Error); trimmed != "" {
		return trimmed
	}
	switch kind {
	case "syfon_broken_bucket_mapping":
		return "Syfon access URL did not resolve through a configured bucket mapping"
	case "syfon_missing_bucket_object", "syfon_git_no_bucket":
		return "mapped bucket object was not found"
	case "git_syfon_metadata_mismatch":
		return "mapped bucket object exists, but object evidence disagrees with the Syfon/Git record"
	case "probe_error":
		return sarecord.AccessErrorForRecord(record)
	default:
		return ""
	}
}

func storageEvidenceStatus(record sarecord.State, bucketObserved bool) string {
	return string(sarecord.AssessStorageRecordEvidence(record, bucketObserved).Status)
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
