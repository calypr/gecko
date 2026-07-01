package git

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

type storageAuditBaseInputs struct {
	index             *repoAnalyticsIndex
	inventory         []RepoInventoryFile
	recordsByChecksum map[string][]projectRecordState
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type storageAuditRecordSet struct {
	recordsByChecksum map[string][]projectRecordState
	allProjectRecords map[string][]projectRecordState
}

type storageAuditStorageView struct {
	scopes                   []domain.StorageBucketScope
	bucketObjects            []gintegrationsyfon.ProjectBucketObject
	bucketObjectsByURL       map[string]gintegrationsyfon.ProjectBucketObject
	recordsByChecksum        map[string][]projectRecordState
	allProjectRecords        map[string][]projectRecordState
	bucketInventoryAvailable bool
	bucketInventoryError     string
}

type storageChainIndex struct {
	inventory           []RepoInventoryFile
	allRecords          []projectRecordState
	bucketObjectsByURL  map[string]gintegrationsyfon.ProjectBucketObject
	repoPathsByChecksum map[string][]string
	recordsByBucketURL  map[string][]projectRecordState
}

func (service *StorageAnalyticsService) loadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]RepoInventoryFile, error) {
	index, err := loadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, err
	}
	return filterRepoInventoryFiles(index, gitSubpath)
}

type storageFindingKind string

const (
	storageFindingNone               storageFindingKind = ""
	storageFindingBrokenAccessURL    storageFindingKind = "broken_access_url_error"
	storageFindingBrokenBucketMap    storageFindingKind = "broken_bucket_mapping"
	storageFindingObjectMissing      storageFindingKind = "storage_object_missing"
	storageFindingValidationMismatch storageFindingKind = "storage_validation_mismatch"
	storageFindingProbeError         storageFindingKind = "storage_probe_error"
)

type chainAuditAccumulator struct {
	findings []GitStorageChainFinding
	summary  GitStorageChainAuditSummary
}

func newChainSummary(bucketObjectCount, syfonRecordCount, gitTrackedFileCount int) GitStorageChainAuditSummary {
	return GitStorageChainAuditSummary{
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

func (acc *chainAuditAccumulator) add(kind string, findings ...GitStorageChainFinding) {
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
		PathPrefix: normalizeRepoSubpath(gitSubpath),
	}
}

func summarizeChainIssueGroups(findings []GitStorageChainFinding) []GitStorageChainIssueGroup {
	groups := make(map[string]*GitStorageChainIssueGroup)
	groupPaths := make(map[string]map[string]struct{})
	groupObjects := make(map[string]map[string]struct{})
	for _, finding := range findings {
		group := groups[finding.Kind]
		if group == nil {
			group = &GitStorageChainIssueGroup{Kind: finding.Kind}
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
	out := make([]GitStorageChainIssueGroup, 0, len(kinds))
	for _, kind := range kinds {
		group := *groups[kind]
		group.PathCount = len(groupPaths[kind])
		group.ObjectCount = len(groupObjects[kind])
		out = append(out, group)
	}
	return out
}

func (service *StorageAnalyticsService) loadStorageAuditBaseInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*storageAuditBaseInputs, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	return &storageAuditBaseInputs{
		index:             index,
		inventory:         inventory,
		recordsByChecksum: recordsByChecksum,
		usageByObjectID:   usageByObjectID,
	}, nil
}

func (service *StorageAnalyticsService) loadScopedProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string, base *storageAuditBaseInputs) (*storageAuditRecordSet, error) {
	allProjectRecords, err := service.listProjectRecordStates(ctx, authorizationHeader, organization, project, base.usageByObjectID)
	if err != nil {
		return nil, err
	}
	return &storageAuditRecordSet{
		recordsByChecksum: base.recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}, nil
}

func (service *StorageAnalyticsService) loadProjectScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return service.loadProjectStorageScopes(ctx, authorizationHeader, organization, project)
}

func (service *StorageAnalyticsService) loadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	scopes, err := service.storage.ListProjectScopes(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project scopes: %w", err)
	}
	sort.SliceStable(scopes, func(i, j int) bool {
		iProject := strings.TrimSpace(scopes[i].ProjectID)
		jProject := strings.TrimSpace(scopes[j].ProjectID)
		if iProject == "" && jProject != "" {
			return true
		}
		if iProject != "" && jProject == "" {
			return false
		}
		if scopes[i].Bucket != scopes[j].Bucket {
			return scopes[i].Bucket < scopes[j].Bucket
		}
		return scopes[i].Path < scopes[j].Path
	})
	return scopes, nil
}

func applyScopeCanonicalization(recordSet *storageAuditRecordSet, scopes []domain.StorageBucketScope) *storageAuditRecordSet {
	if recordSet == nil {
		return nil
	}
	recordsByChecksum, allProjectRecords := applyScopedStorageMappings(recordSet.recordsByChecksum, recordSet.allProjectRecords, scopes)
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}
}

func (service *StorageAnalyticsService) loadProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string) (*storageAuditRecordSet, error) {
	projectRecords, err := service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	recordsByChecksum := make(map[string][]projectRecordState)
	for _, record := range projectRecords {
		normalizedChecksum := normalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], projectRecordState{
			ProjectRecord: record,
		})
	}
	allProjectRecords := cloneRecordStateMap(recordsByChecksum)
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}, nil
}

func (service *StorageAnalyticsService) loadProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, error) {
	bucketObjects, err := service.storage.ListProjectBucketObjects(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, nil, fmt.Errorf("list syfon project bucket objects: %w", err)
	}
	bucketObjectsByURL := make(map[string]gintegrationsyfon.ProjectBucketObject, len(bucketObjects))
	for _, item := range bucketObjects {
		if objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL); objectURL != "" {
			bucketObjectsByURL[objectURL] = item
		}
	}
	return bucketObjects, bucketObjectsByURL, nil
}

func synthesizeBucketInventoryFromProbes(allProjectRecords map[string][]projectRecordState) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	if len(allProjectRecords) == 0 {
		return []gintegrationsyfon.ProjectBucketObject{}, map[string]gintegrationsyfon.ProjectBucketObject{}
	}
	bucketObjectsByURL := make(map[string]gintegrationsyfon.ProjectBucketObject)
	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, probe := range record.AccessProbes {
				if !strings.EqualFold(strings.TrimSpace(probe.Status), "present") {
					continue
				}
				objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL)
				if objectURL == "" {
					continue
				}
				if _, ok := bucketObjectsByURL[objectURL]; ok {
					continue
				}
				bucketObjectsByURL[objectURL] = gintegrationsyfon.ProjectBucketObject{
					ObjectURL:    objectURL,
					Provider:     strings.TrimSpace(probe.Provider),
					Bucket:       strings.TrimSpace(probe.Bucket),
					Key:          strings.TrimSpace(probe.Key),
					Path:         strings.TrimSpace(probe.Path),
					SizeBytes:    derefInt64(probe.SizeBytes),
					MetaSHA256:   strings.TrimSpace(probe.MetaSHA256),
					ETag:         strings.TrimSpace(probe.ETag),
					LastModified: strings.TrimSpace(probe.LastModified),
				}
			}
		}
	}
	bucketObjects := make([]gintegrationsyfon.ProjectBucketObject, 0, len(bucketObjectsByURL))
	for _, item := range bucketObjectsByURL {
		bucketObjects = append(bucketObjects, item)
	}
	sort.Slice(bucketObjects, func(i, j int) bool {
		return bucketObjects[i].ObjectURL < bucketObjects[j].ObjectURL
	})
	return bucketObjects, bucketObjectsByURL
}

func shouldDegradeBucketInventory(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "project-bucket") &&
		(strings.Contains(message, "status 403") ||
			strings.Contains(message, "status 409") ||
			strings.Contains(message, "permission denied") ||
			strings.Contains(message, "provider denied list access") ||
			strings.Contains(message, "bucket inventory request") ||
			strings.Contains(message, "bucket target may be missing or inaccessible"))
}

func (service *StorageAnalyticsService) attachProjectStorageProbes(ctx context.Context, authorizationHeader string, recordSet *storageAuditRecordSet) (*storageAuditRecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.attachStorageProbes(ctx, authorizationHeader, recordSet.recordsByChecksum, recordSet.allProjectRecords)
	if err != nil {
		return nil, err
	}
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}, nil
}

func (service *StorageAnalyticsService) loadStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *storageAuditRecordSet) (*storageAuditStorageView, error) {
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes)
	view := &storageAuditStorageView{
		scopes:                   scopes,
		recordsByChecksum:        recordSet.recordsByChecksum,
		allProjectRecords:        recordSet.allProjectRecords,
		bucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		bucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		bucketInventoryAvailable: true,
	}
	bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project)
	if err != nil {
		if !shouldDegradeBucketInventory(err) {
			return nil, err
		}
		view.bucketInventoryAvailable = false
		view.bucketInventoryError = strings.TrimSpace(err.Error())
		probedRecordSet, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
		if probeErr != nil {
			return nil, probeErr
		}
		view.recordsByChecksum = probedRecordSet.recordsByChecksum
		view.allProjectRecords = probedRecordSet.allProjectRecords
		view.bucketObjects, view.bucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.allProjectRecords)
		return view, nil
	}
	view.bucketObjects = bucketObjects
	view.bucketObjectsByURL = bucketObjectsByURL

	probeCandidates := selectTargetedProbeRecordSet(recordSet, bucketObjectsByURL)
	if probeCandidates != nil {
		probedSubset, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, probeCandidates)
		if probeErr != nil {
			return nil, probeErr
		}
		merged := mergeRecordSetProbes(recordSet, probedSubset)
		view.recordsByChecksum = merged.recordsByChecksum
		view.allProjectRecords = merged.allProjectRecords
	}
	return view, nil
}

func (service *StorageAnalyticsService) loadStorageAuditStorageView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *storageAuditRecordSet, includeBucketInventory bool, includeProbes bool) (*storageAuditStorageView, error) {
	scopes, err := service.loadProjectScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes)
	if includeProbes {
		recordSet, err = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
		if err != nil {
			return nil, err
		}
	}
	view := &storageAuditStorageView{
		scopes:                   scopes,
		recordsByChecksum:        recordSet.recordsByChecksum,
		allProjectRecords:        recordSet.allProjectRecords,
		bucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		bucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		bucketInventoryAvailable: includeBucketInventory,
	}
	if includeBucketInventory {
		bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project)
		if err != nil {
			if !includeProbes || !shouldDegradeBucketInventory(err) {
				return nil, err
			}
			view.bucketInventoryAvailable = false
			view.bucketInventoryError = strings.TrimSpace(err.Error())
			bucketObjects, bucketObjectsByURL = synthesizeBucketInventoryFromProbes(recordSet.allProjectRecords)
		}
		view.bucketObjects = bucketObjects
		view.bucketObjectsByURL = bucketObjectsByURL
	}
	return view, nil
}

func cloneRecordStateMap(input map[string][]projectRecordState) map[string][]projectRecordState {
	out := make(map[string][]projectRecordState, len(input))
	for checksum, group := range input {
		states := make([]projectRecordState, 0, len(group))
		for _, record := range group {
			clone := record
			clone.CanonicalAccessURLs = append([]string(nil), record.CanonicalAccessURLs...)
			clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
			states = append(states, clone)
		}
		out[checksum] = states
	}
	return out
}

func selectTargetedProbeRecordSet(recordSet *storageAuditRecordSet, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) *storageAuditRecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]projectRecordState)
	for checksum, group := range recordSet.allProjectRecords {
		for _, record := range group {
			if !recordNeedsTargetedProbe(record, bucketObjectsByURL) {
				continue
			}
			selected[checksum] = append(selected[checksum], record)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &storageAuditRecordSet{
		recordsByChecksum: cloneRecordStateMap(selected),
		allProjectRecords: cloneRecordStateMap(selected),
	}
}

func recordNeedsTargetedProbe(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(matchedBucketObjectURLs(record, bucketObjectsByURL)) == 0 {
		return true
	}
	if !sameStringSet(rawAccessURLsForRecord(record), accessURLsForStorage(record)) {
		return true
	}
	checksum := normalizeAnalyticsChecksum(record.Checksum)
	if checksum == "" {
		return false
	}
	matches := matchedBucketObjectURLs(record, bucketObjectsByURL)
	for _, objectURL := range matches {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if normalizeAnalyticsChecksum(item.MetaSHA256) != "" {
			return false
		}
	}
	return true
}

func sameStringSet(left []string, right []string) bool {
	a := uniqueStrings(left)
	b := uniqueStrings(right)
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, value := range a {
		set[strings.TrimSpace(value)] = struct{}{}
	}
	for _, value := range b {
		if _, ok := set[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func mergeRecordSetProbes(base *storageAuditRecordSet, probed *storageAuditRecordSet) *storageAuditRecordSet {
	if base == nil {
		return probed
	}
	if probed == nil {
		return base
	}
	probesByObjectID := make(map[string][]gintegrationsyfon.BulkStorageProbeResult)
	for _, group := range probed.allProjectRecords {
		for _, record := range group {
			probesByObjectID[strings.TrimSpace(record.ObjectID)] = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
		}
	}
	attach := func(input map[string][]projectRecordState) map[string][]projectRecordState {
		out := make(map[string][]projectRecordState, len(input))
		for checksum, group := range input {
			states := make([]projectRecordState, 0, len(group))
			for _, record := range group {
				clone := record
				if probes, ok := probesByObjectID[strings.TrimSpace(record.ObjectID)]; ok {
					clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), probes...)
				}
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return &storageAuditRecordSet{
		recordsByChecksum: attach(base.recordsByChecksum),
		allProjectRecords: attach(base.allProjectRecords),
	}
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func buildStorageChainIndex(inventory []RepoInventoryFile, allProjectRecords map[string][]projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) storageChainIndex {
	repoPathsByChecksum := make(map[string][]string, len(inventory))
	for _, item := range inventory {
		checksum := normalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		repoPathsByChecksum[checksum] = append(repoPathsByChecksum[checksum], item.RepoPath)
	}
	allRecords := flattenRecordStates(allProjectRecords)
	recordsByBucketURL := make(map[string][]projectRecordState)
	for _, record := range allRecords {
		for _, bucketURL := range recordBucketURLs(record) {
			recordsByBucketURL[bucketURL] = append(recordsByBucketURL[bucketURL], record)
		}
	}
	return storageChainIndex{
		inventory:           inventory,
		allRecords:          allRecords,
		bucketObjectsByURL:  bucketObjectsByURL,
		repoPathsByChecksum: repoPathsByChecksum,
		recordsByBucketURL:  recordsByBucketURL,
	}
}

func buildStorageChainAuditModel(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) *chainAuditModel {
	index := buildStorageChainIndex(inventory, allProjectRecords, bucketObjectsByURL)
	acc := chainAuditAccumulator{
		findings: make([]GitStorageChainFinding, 0),
		summary:  newChainSummary(len(bucketObjectsByURL), len(index.allRecords), len(inventory)),
	}
	buildBucketOriginChainFindings(index, &acc)
	buildSyfonOriginChainFindings(index, &acc)
	buildGitOriginChainFindings(index, recordsByChecksum, allProjectRecords, &acc)
	return finalizeChainFindings(gitSubpath, acc)
}

func buildBucketOriginChainFindings(index storageChainIndex, acc *chainAuditAccumulator) {
	bucketURLs := make([]string, 0, len(index.bucketObjectsByURL))
	for bucketURL := range index.bucketObjectsByURL {
		bucketURLs = append(bucketURLs, bucketURL)
	}
	sort.Strings(bucketURLs)
	for _, bucketURL := range bucketURLs {
		item := index.bucketObjectsByURL[bucketURL]
		if bucketObjectHasCompleteChain(index.recordsByBucketURL[bucketURL], index.repoPathsByChecksum, index.bucketObjectsByURL) {
			acc.addCount("bucket_syfon_git_complete", 1)
			continue
		}
		acc.add("bucket_only_object", buildChainBucketOnlyFinding(item))
	}
}

func buildSyfonOriginChainFindings(index storageChainIndex, acc *chainAuditAccumulator) {
	for _, record := range index.allRecords {
		gitPaths := uniqueStrings(index.repoPathsByChecksum[normalizeAnalyticsChecksum(record.Checksum)])
		bucketMatches := matchedBucketObjectURLs(record, index.bucketObjectsByURL)
		switch classifyStorageFinding(record, index.bucketObjectsByURL) {
		case storageFindingBrokenBucketMap:
			findings := buildChainRecordFindings("syfon_broken_bucket_mapping", record, gitPaths, bucketMatches, "Syfon record exists, but its access URL does not resolve to a configured bucket mapping.")
			acc.findings = append(acc.findings, findings...)
			acc.addCount("syfon_broken_bucket_mapping", chainPathCount(gitPaths))
		case storageFindingObjectMissing:
			if len(gitPaths) > 0 {
				findings := buildChainRecordFindings("syfon_git_no_bucket", record, gitPaths, bucketMatches, "Git and Syfon matched, but the mapped bucket object does not exist.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("syfon_git_no_bucket", len(gitPaths))
				continue
			}
			acc.add("syfon_missing_bucket_object", buildChainRecordFindings("syfon_missing_bucket_object", record, nil, bucketMatches, "Syfon record points to a mapped bucket location, but the object does not exist.")...)
		case storageFindingValidationMismatch:
			if len(gitPaths) > 0 {
				findings := buildChainRecordFindings("git_syfon_metadata_mismatch", record, gitPaths, bucketMatches, "Bucket object exists, but its metadata does not match what Syfon expects.")
				acc.findings = append(acc.findings, findings...)
				acc.addCount("git_syfon_metadata_mismatch", len(gitPaths))
				continue
			}
			acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
		case storageFindingBrokenAccessURL, storageFindingProbeError:
			findings := buildChainRecordFindings("probe_error", record, gitPaths, bucketMatches, "Bucket verification failed before Gecko could classify this record cleanly.")
			acc.findings = append(acc.findings, findings...)
			acc.addCount("probe_error", chainPathCount(gitPaths))
		case storageFindingNone:
			if len(gitPaths) == 0 && len(bucketMatches) > 0 {
				acc.add("bucket_syfon_no_git", buildChainRecordFindings("bucket_syfon_no_git", record, nil, bucketMatches, "Bucket object and Syfon record matched, but no Git-tracked file matched this checksum.")...)
			}
		}
	}
}

func buildGitOriginChainFindings(index storageChainIndex, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, acc *chainAuditAccumulator) {
	for _, item := range index.inventory {
		checksum := normalizeAnalyticsChecksum(item.Checksum)
		if len(allProjectRecords[checksum]) > 0 || len(recordsByChecksum[checksum]) > 0 {
			continue
		}
		acc.add("git_only_no_syfon", GitStorageChainFinding{
			Kind:              "git_only_no_syfon",
			NormalizedPath:    item.RepoPath,
			Checksum:          checksum,
			SourcePaths:       []string{item.RepoPath},
			ObjectIDs:         []string{},
			RecordCount:       0,
			SizeBytes:         item.Size,
			RecommendedAction: "Git checksum has no matching Syfon record. Bucket presence is not claimed by this finding.",
			Evidence: &GitAuditEvidence{
				Checksum:         checksum,
				SourcePaths:      []string{item.RepoPath},
				ObjectIDs:        []string{},
				BucketEvaluation: "not_evaluated",
			},
		})
	}
}
