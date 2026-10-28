package chain

import (
	"log"
	"path"
	"sort"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
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

func buildStorageChainAuditModel(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string, includeBucketOrigin bool, countCompleteFromSyfon bool) *chainAuditModel {
	index := buildStorageChainIndex(inventory, allProjectRecords, bucketObjectsByURL, scopes, organization, project)
	acc := chainAuditAccumulator{
		findings: make([]model.ChainFinding, 0),
		summary:  newChainSummary(len(bucketObjectsByURL), len(index.AllRecords), len(inventory)),
	}
	if includeBucketOrigin {
		buildBucketOriginChainFindings(index, &acc)
	}
	buildSyfonOriginChainFindings(index, &acc, countCompleteFromSyfon)
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
		kind := storageChainAuditJSONString(finding.Kind)
		group := groups[kind]
		if group == nil {
			group = &model.ChainIssueGroup{Kind: kind}
			groups[kind] = group
			groupPaths[kind] = map[string]struct{}{}
			groupObjects[kind] = map[string]struct{}{}
		}
		group.FindingCount++
		group.RecordCount += finding.RecordCount
		group.TotalBytes += finding.SizeBytes
		groupPaths[kind][storageChainAuditJSONString(finding.NormalizedPath)] = struct{}{}
		for _, objectID := range finding.ObjectIDs {
			groupObjects[kind][storageChainAuditJSONString(objectID)] = struct{}{}
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

type chainAuditAccumulator struct {
	findings                   []model.ChainFinding
	summary                    model.ChainSummary
	missingBucketDebugLogCount int
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
