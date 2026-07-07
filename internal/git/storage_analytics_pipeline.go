package git

import (
	"context"
	"fmt"
	"log"
	"path"
	"sort"
	"strings"
	"time"

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
	inventory            []RepoInventoryFile
	allRecords           []projectRecordState
	bucketObjectsByURL   map[string]gintegrationsyfon.ProjectBucketObject
	repoPathsByChecksum  map[string][]string
	recordsByBucketURL   map[string][]projectRecordState
	equivalentRecordKeys equivalentRecordKeyIndex
}

type equivalentRecordKeyIndex struct {
	byName            map[string]struct{}
	byNameSize        map[string]map[int64]struct{}
	unknownSizeByName map[string]struct{}
}

func (service *StorageAnalyticsService) loadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]RepoInventoryFile, error) {
	index, err := loadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, err
	}
	return filterRepoInventoryFiles(index, gitSubpath)
}

type storageChainInputs struct {
	inventory          []RepoInventoryFile
	recordSet          *storageAuditRecordSet
	scopes             []domain.StorageBucketScope
	bucketSummary      *gintegrationsyfon.ProjectBucketSummary
	bucketObjects      []gintegrationsyfon.ProjectBucketObject
	bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
	bucketInventoryErr error
}

func (service *StorageAnalyticsService) loadStorageChainInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, bucketMode string, bucketPathPrefix string, timings *StorageChainAuditTimings) (*storageChainInputs, error) {
	type inventoryResult struct {
		inventory []RepoInventoryFile
		err       error
	}
	type recordResult struct {
		recordSet *storageAuditRecordSet
		err       error
	}
	type scopeResult struct {
		scopes []domain.StorageBucketScope
		err    error
	}
	type bucketResult struct {
		bucketSummary      *gintegrationsyfon.ProjectBucketSummary
		bucketObjects      []gintegrationsyfon.ProjectBucketObject
		bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
		err                error
	}

	inventoryCh := make(chan inventoryResult, 1)
	recordCh := make(chan recordResult, 1)
	scopeCh := make(chan scopeResult, 1)
	bucketCh := make(chan bucketResult, 1)

	go func() {
		start := time.Now()
		timings.StageStart("repo_index")
		inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
		timings.Record("repo_index", time.Since(start))
		timings.RecordMemory("repo_index", "git_files", len(inventory))
		logStorageChainInputResult("repo_index", len(inventory), err)
		inventoryCh <- inventoryResult{inventory: inventory, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_records")
		recordSet, err := service.loadCachedProjectAuditRecordSet(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_records", time.Since(start))
		recordCount := 0
		if recordSet != nil {
			recordCount = countRecordStates(recordSet.allProjectRecords)
		}
		timings.RecordMemory("syfon_project_records", "syfon_records", recordCount)
		logStorageChainInputResult("syfon_project_records", recordCount, err)
		recordCh <- recordResult{recordSet: recordSet, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_scopes")
		scopes, err := service.loadCachedProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_scopes", time.Since(start))
		logStorageChainInputResult("syfon_project_scopes", len(scopes), err)
		scopeCh <- scopeResult{scopes: scopes, err: err}
	}()
	if bucketMode == StorageChainBucketModeValidate {
		bucketCh <- bucketResult{
			bucketObjects:      []gintegrationsyfon.ProjectBucketObject{},
			bucketObjectsByURL: map[string]gintegrationsyfon.ProjectBucketObject{},
		}
		timings.Record("syfon_bucket_inventory_skipped", 0)
		logStorageChainInputResult("syfon_bucket_inventory_skipped validate_mode", 0, nil)
	} else {
		go func() {
			start := time.Now()
			timings.StageStart("syfon_bucket_inventory")
			bucketObjects, bucketObjectsByURL, err := service.loadCachedProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
			timings.Record("syfon_bucket_inventory", time.Since(start))
			timings.RecordMemory("syfon_bucket_inventory", "bucket_objects", len(bucketObjects), "bucket_lookup", len(bucketObjectsByURL))
			logStorageChainInputResult("syfon_bucket_items", len(bucketObjects), err)
			bucketCh <- bucketResult{bucketObjects: bucketObjects, bucketObjectsByURL: bucketObjectsByURL, err: err}
		}()
	}

	inventory := <-inventoryCh
	recordSet := <-recordCh
	scopes := <-scopeCh
	bucketObjects := <-bucketCh
	if inventory.err != nil {
		return nil, inventory.err
	}
	if recordSet.err != nil {
		return nil, recordSet.err
	}
	if scopes.err != nil {
		return nil, scopes.err
	}
	return &storageChainInputs{
		inventory:          inventory.inventory,
		recordSet:          recordSet.recordSet,
		scopes:             scopes.scopes,
		bucketSummary:      bucketObjects.bucketSummary,
		bucketObjects:      bucketObjects.bucketObjects,
		bucketObjectsByURL: bucketObjects.bucketObjectsByURL,
		bucketInventoryErr: bucketObjects.err,
	}, nil
}

func logStorageChainInputResult(stage string, count int, err error) {
	if err != nil {
		log.Printf("INFO: storage_chain_input_done stage=%s count=%d error=%q", strings.TrimSpace(stage), count, err.Error())
		return
	}
	log.Printf("INFO: storage_chain_input_done stage=%s count=%d", strings.TrimSpace(stage), count)
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
	if len(scopes) == 0 {
		scopes, err = service.loadProjectStorageScopes(ctx, authorizationHeader, organization, project)
		if err != nil {
			return nil, err
		}
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

func applyScopeCanonicalization(recordSet *storageAuditRecordSet, scopes []domain.StorageBucketScope, organization string, project string) *storageAuditRecordSet {
	if recordSet == nil {
		return nil
	}
	recordsByChecksum, allProjectRecords := applyScopedStorageMappings(recordSet.recordsByChecksum, recordSet.allProjectRecords, scopes, organization, project)
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}
}

func (service *StorageAnalyticsService) loadProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (*storageAuditRecordSet, error) {
	projectRecords, err := service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, pathPrefix)
	if err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	return buildProjectAuditRecordSet(projectRecords), nil
}

func (service *StorageAnalyticsService) loadCachedProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string) (*storageAuditRecordSet, error) {
	projectRecords, err := service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	return buildProjectAuditRecordSet(projectRecords), nil
}

func (service *StorageAnalyticsService) loadCachedProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project)
	summary, err := service.storage.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("get syfon project metrics summary: %w", err)
	}
	if summary != nil {
		validator := projectAuditRecordValidatorFromSummary(*summary)
		service.chainInputMu.RLock()
		cached, ok := service.projectAuditCache[cacheKey]
		service.chainInputMu.RUnlock()
		if ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			return cached.records, nil
		}

		workKey := projectAuditRecordWorkKey(cacheKey, validator)
		service.chainInputMu.Lock()
		if cached, ok := service.projectAuditCache[cacheKey]; ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			service.chainInputMu.Unlock()
			return cached.records, nil
		}
		if inflight, ok := service.projectAuditWork[workKey]; ok {
			service.chainInputMu.Unlock()
			<-inflight.done
			if inflight.err != nil {
				return nil, inflight.err
			}
			return inflight.records, nil
		}
		inflight := &inflightProjectAuditRecordState{
			done:      make(chan struct{}),
			validator: validator,
		}
		service.projectAuditWork[workKey] = inflight
		service.chainInputMu.Unlock()
		defer func() {
			service.chainInputMu.Lock()
			delete(service.projectAuditWork, workKey)
			close(inflight.done)
			service.chainInputMu.Unlock()
		}()

		projectRecords, err := service.storage.ListProjectAuditRecords(context.WithoutCancel(ctx), authorizationHeader, organization, project, "")
		if err != nil {
			inflight.err = fmt.Errorf("list syfon project audit records: %w", err)
			return nil, inflight.err
		}
		copiedRecords := append([]gintegrationsyfon.ProjectRecord(nil), projectRecords...)
		service.chainInputMu.Lock()
		service.projectAuditCache[cacheKey] = cachedProjectAuditRecordState{
			records:   copiedRecords,
			validator: validator,
			cachedAt:  time.Now(),
		}
		service.chainInputMu.Unlock()
		inflight.records = copiedRecords
		return copiedRecords, nil
	}
	projectRecords, err := service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, "")
	if err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	return projectRecords, nil
}

func projectAuditRecordWorkKey(cacheKey string, validator projectAuditRecordValidator) string {
	return fmt.Sprintf("%s::records::%d::%s::%s", cacheKey, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
}

func buildProjectAuditRecordSet(projectRecords []gintegrationsyfon.ProjectRecord) *storageAuditRecordSet {
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
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: recordsByChecksum,
	}
}

func projectAuditRecordValidatorFromSummary(summary gintegrationsyfon.ProjectMetricsSummary) projectAuditRecordValidator {
	return projectAuditRecordValidator{
		RecordCount:             summary.RecordCount,
		RecordLatestUpdatedTime: strings.TrimSpace(summary.RecordLatestUpdatedTime),
		RecordRevision:          strings.TrimSpace(summary.RecordRevision),
	}
}

func filterProjectAuditRecordSet(recordSet *storageAuditRecordSet, pathPrefix string, scopes []domain.StorageBucketScope, organization string, project string) *storageAuditRecordSet {
	if recordSet == nil || normalizeRepoSubpath(pathPrefix) == "" {
		return recordSet
	}
	filtered := make(map[string][]projectRecordState, len(recordSet.allProjectRecords))
	for checksum, group := range recordSet.allProjectRecords {
		for _, record := range group {
			if projectAuditRecordMatchesPathPrefix(record.ProjectRecord, pathPrefix, scopes, organization, project) {
				filtered[checksum] = append(filtered[checksum], record)
			}
		}
	}
	return &storageAuditRecordSet{
		recordsByChecksum: cloneRecordStateMap(filtered),
		allProjectRecords: cloneRecordStateMap(filtered),
	}
}

func projectAuditRecordMatchesPathPrefix(record gintegrationsyfon.ProjectRecord, pathPrefix string, scopes []domain.StorageBucketScope, organization string, project string) bool {
	normalizedPrefix := normalizeRepoSubpath(pathPrefix)
	if normalizedPrefix == "" {
		return true
	}
	for _, accessURL := range projectAuditRecordPathURLs(record, scopes, organization, project) {
		_, key, ok := parseStorageURL(accessURL)
		if !ok {
			continue
		}
		key = normalizeRepoSubpath(key)
		if key == normalizedPrefix || strings.HasPrefix(key, normalizedPrefix+"/") {
			return true
		}
	}
	return false
}

func projectAuditRecordPathURLs(record gintegrationsyfon.ProjectRecord, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(record.AccessURLs)+len(record.AccessMethods)*2)
	out = append(out, record.AccessURLs...)
	for _, method := range record.AccessMethods {
		if trimmed := strings.TrimSpace(method.URL); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	out = append(out, canonicalizeRecordAccessURLs(out, scopes, organization, project)...)
	return uniqueStrings(out)
}

func (service *StorageAnalyticsService) loadProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, error) {
	bucketObjects, err := service.storage.ListProjectBucketObjects(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list syfon project bucket objects: %w", err)
	}
	objects, lookup := buildBucketObjectLookup(bucketObjects)
	return objects, lookup, nil
}

func (service *StorageAnalyticsService) loadProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, error) {
	bucketObjects, err := service.storage.ListProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list syfon project bucket inventory: %w", err)
	}
	objects, lookup := buildBucketObjectLookup(bucketObjects)
	return objects, lookup, nil
}

func (service *StorageAnalyticsService) loadCachedProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project)
	service.chainInputMu.RLock()
	cached, ok := service.chainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) && cached.projectScopes != nil {
		return append([]domain.StorageBucketScope(nil), cached.projectScopes...), nil
	}
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	service.updateChainInputCache(cacheKey, func(state *cachedChainInputState) {
		state.projectScopes = append([]domain.StorageBucketScope(nil), scopes...)
	})
	return scopes, nil
}

func (service *StorageAnalyticsService) loadCachedProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project) + "::bucket-items::" + normalizeRepoSubpath(bucketPathPrefix)
	service.chainInputMu.RLock()
	cached, ok := service.chainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) && cached.bucketObjects != nil && cached.bucketObjectsByURL != nil {
		objects, lookup := cloneBucketInventory(cached.bucketObjects, cached.bucketObjectsByURL)
		return objects, lookup, nil
	}
	bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, err
	}
	service.updateChainInputCache(cacheKey, func(state *cachedChainInputState) {
		state.bucketObjects, state.bucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	})
	objects, lookup := cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	return objects, lookup, nil
}

func (service *StorageAnalyticsService) loadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project) + "::bucket-validation-inventory::" + normalizeRepoSubpath(bucketPathPrefix)
	service.chainInputMu.RLock()
	cached, ok := service.chainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) && cached.bucketObjects != nil && cached.bucketObjectsByURL != nil {
		objects, lookup := cloneBucketInventory(cached.bucketObjects, cached.bucketObjectsByURL)
		return objects, lookup, nil
	}
	bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
	if err != nil {
		return nil, nil, err
	}
	service.updateChainInputCache(cacheKey, func(state *cachedChainInputState) {
		state.bucketObjects, state.bucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	})
	objects, lookup := cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	return objects, lookup, nil
}

func (service *StorageAnalyticsService) loadCachedProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*gintegrationsyfon.ProjectBucketSummary, error) {
	cacheKey := service.projectChainInputCacheKey(organization, project) + "::bucket-summary::" + strings.TrimSpace(mode)
	service.chainInputMu.RLock()
	cached, ok := service.chainInputCache[cacheKey]
	service.chainInputMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) && cached.bucketSummary != nil {
		summary := *cached.bucketSummary
		return &summary, nil
	}
	summary, err := service.storage.ListProjectBucketSummary(ctx, authorizationHeader, organization, project, mode)
	if err != nil {
		return nil, err
	}
	service.updateChainInputCache(cacheKey, func(state *cachedChainInputState) {
		if summary == nil {
			state.bucketSummary = nil
			return
		}
		copy := *summary
		state.bucketSummary = &copy
	})
	if summary == nil {
		return nil, nil
	}
	copy := *summary
	return &copy, nil
}

func (service *StorageAnalyticsService) projectChainInputCacheKey(organization string, project string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project)
}

func (service *StorageAnalyticsService) updateChainInputCache(cacheKey string, update func(*cachedChainInputState)) {
	service.chainInputMu.Lock()
	defer service.chainInputMu.Unlock()
	state := service.chainInputCache[cacheKey]
	if time.Now().After(state.expiresAt) {
		state = cachedChainInputState{}
	}
	state.expiresAt = time.Now().Add(chainInputCacheTTL)
	update(&state)
	service.chainInputCache[cacheKey] = state
}

func cloneBucketInventory(bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	objects := append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...)
	lookup := make(map[string]gintegrationsyfon.ProjectBucketObject, len(bucketObjectsByURL))
	for objectURL, item := range bucketObjectsByURL {
		lookup[objectURL] = item
	}
	return objects, lookup
}

func buildBucketObjectLookup(bucketObjects []gintegrationsyfon.ProjectBucketObject) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject) {
	bucketObjectsByURL := make(map[string]gintegrationsyfon.ProjectBucketObject, len(bucketObjects))
	for _, item := range bucketObjects {
		if objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL); objectURL != "" {
			bucketObjectsByURL[objectURL] = item
		}
	}
	return append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...), bucketObjectsByURL
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

func (service *StorageAnalyticsService) attachProjectStorageListValidations(ctx context.Context, authorizationHeader string, recordSet *storageAuditRecordSet) (*storageAuditRecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.attachStorageListValidations(ctx, authorizationHeader, recordSet.recordsByChecksum, recordSet.allProjectRecords)
	if err != nil {
		return nil, err
	}
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}, nil
}

func attachProjectStorageInventoryValidations(recordSet *storageAuditRecordSet, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string) (*storageAuditRecordSet, error) {
	if recordSet == nil {
		return nil, nil
	}
	inventoryBuckets := projectInventoryBuckets(bucketObjectsByURL)
	attach := func(input map[string][]projectRecordState) (map[string][]projectRecordState, error) {
		out := make(map[string][]projectRecordState, len(input))
		for checksum, group := range input {
			states := make([]projectRecordState, 0, len(group))
			for _, record := range group {
				clone := record
				probes, err := inventoryValidationProbesForRecord(record, bucketObjectsByURL, inventoryBuckets, scopes, organization, project)
				if err != nil {
					return nil, err
				}
				clone.AccessProbes = probes
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out, nil
	}
	recordsByChecksum, err := attach(recordSet.recordsByChecksum)
	if err != nil {
		return nil, err
	}
	allProjectRecords, err := attach(recordSet.allProjectRecords)
	if err != nil {
		return nil, err
	}
	return &storageAuditRecordSet{
		recordsByChecksum: recordsByChecksum,
		allProjectRecords: allProjectRecords,
	}, nil
}

func inventoryValidationProbesForRecord(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, inventoryBuckets map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0)
	seen := make(map[string]struct{})
	accessURLs, err := canonicalizeRecordAccessURLsForProjectInventory(record.AccessURLs, scopes, organization, project)
	if err != nil {
		return nil, err
	}
	for _, accessURL := range accessURLs {
		objectURL := canonicalStorageURL("", "", accessURL)
		if objectURL == "" {
			continue
		}
		bucket, _, _ := parseStorageURL(objectURL)
		if len(inventoryBuckets) > 0 {
			if _, ok := inventoryBuckets[bucket]; !ok {
				return nil, fmt.Errorf("storage access URL mapped to %q, but bucket %q is not present in project bucket inventory for project %s/%s", objectURL, bucket, strings.TrimSpace(organization), strings.TrimSpace(project))
			}
		}
		if _, ok := seen[objectURL]; ok {
			continue
		}
		seen[objectURL] = struct{}{}
		expectedName := expectedStorageObjectNameForListValidation(objectURL, record.Name)
		if item, ok := bucketObjectsByURL[objectURL]; ok {
			probes = append(probes, inventoryPresentProbe(record, objectURL, expectedName, item))
			continue
		}
		probes = append(probes, inventoryMissingProbe(record, objectURL, expectedName))
	}
	return probes, nil
}

func projectInventoryBuckets(bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) map[string]struct{} {
	out := make(map[string]struct{})
	for objectURL, item := range bucketObjectsByURL {
		if bucket := strings.TrimSpace(item.Bucket); bucket != "" {
			out[bucket] = struct{}{}
			continue
		}
		bucket, _, ok := parseStorageURL(objectURL)
		if ok {
			out[bucket] = struct{}{}
		}
	}
	return out
}

func inventoryPresentProbe(record projectRecordState, objectURL string, expectedName string, item gintegrationsyfon.ProjectBucketObject) gintegrationsyfon.BulkStorageProbeResult {
	size := item.SizeBytes
	sizeMatch := item.SizeBytes == record.Size
	nameMatch := true
	if expectedName != "" {
		nameMatch = path.Base(strings.Trim(strings.TrimSpace(item.Key), "/")) == expectedName
	}
	mismatches := make([]string, 0, 2)
	if !sizeMatch {
		mismatches = append(mismatches, "size_mismatch")
	}
	if !nameMatch {
		mismatches = append(mismatches, "name_mismatch")
	}
	validationStatus := "matched"
	if len(mismatches) > 0 {
		validationStatus = "mismatched"
	}
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   storageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            StorageChainValidationModeList,
		ObjectURL:            objectURL,
		Provider:             strings.TrimSpace(item.Provider),
		Bucket:               strings.TrimSpace(item.Bucket),
		Key:                  strings.Trim(strings.TrimSpace(item.Key), "/"),
		Path:                 strings.TrimSpace(item.Path),
		Exists:               true,
		Status:               "present",
		SizeBytes:            &size,
		MetaSHA256:           strings.TrimSpace(item.MetaSHA256),
		ETag:                 strings.TrimSpace(item.ETag),
		LastModified:         strings.TrimSpace(item.LastModified),
		ValidationStatus:     validationStatus,
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: mismatches,
	}
}

func inventoryMissingProbe(record projectRecordState, objectURL string, expectedName string) gintegrationsyfon.BulkStorageProbeResult {
	bucket, key, _ := parseStorageURL(objectURL)
	sizeMatch := false
	nameMatch := false
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   storageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            StorageChainValidationModeList,
		ObjectURL:            objectURL,
		Provider:             "s3",
		Bucket:               bucket,
		Key:                  key,
		Path:                 path.Base(key),
		Exists:               false,
		Status:               "not_found",
		Error:                fmt.Sprintf("object %q was not found in project bucket inventory", objectURL),
		ErrorKind:            "object_not_found",
		ValidationStatus:     "unverifiable",
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: []string{},
	}
}

func (service *StorageAnalyticsService) loadStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *storageAuditRecordSet) (*storageAuditStorageView, error) {
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	view := &storageAuditStorageView{
		scopes:                   scopes,
		recordsByChecksum:        recordSet.recordsByChecksum,
		allProjectRecords:        recordSet.allProjectRecords,
		bucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		bucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		bucketInventoryAvailable: true,
	}
	bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, "")
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

func (service *StorageAnalyticsService) buildStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *storageAuditRecordSet, scopes []domain.StorageBucketScope, bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, bucketInventoryErr error, bucketMode string, validationMode string, timings *StorageChainAuditTimings) (*storageAuditStorageView, error) {
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	view := &storageAuditStorageView{
		scopes:                   append([]domain.StorageBucketScope(nil), scopes...),
		recordsByChecksum:        recordSet.recordsByChecksum,
		allProjectRecords:        recordSet.allProjectRecords,
		bucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		bucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		bucketInventoryAvailable: true,
	}
	if bucketMode == StorageChainBucketModeValidate {
		if bucketInventoryErr != nil {
			view.bucketInventoryAvailable = false
			view.bucketInventoryError = strings.TrimSpace(bucketInventoryErr.Error())
		}
		if validationMode == StorageChainValidationModeInventory {
			return view, nil
		}
		if validationMode == StorageChainValidationModeList {
			inventoryStart := time.Now()
			validationObjects, validationObjectsByURL, err := service.loadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, "")
			timings.Record("syfon_bucket_validation_inventory", time.Since(inventoryStart))
			timings.RecordMemory("syfon_bucket_validation_inventory", "bucket_objects", len(validationObjects), "bucket_lookup", len(validationObjectsByURL))
			if err != nil {
				return nil, err
			}
			view.bucketObjects, view.bucketObjectsByURL = cloneBucketInventory(validationObjects, validationObjectsByURL)
			validateStart := time.Now()
			probedRecordSet, err := attachProjectStorageInventoryValidations(recordSet, view.bucketObjectsByURL, scopes, organization, project)
			if err != nil {
				return nil, err
			}
			timings.Record("inventory_list_validation", time.Since(validateStart))
			timings.RecordMemory("inventory_list_validation", "syfon_records", countRecordStates(probedRecordSet.allProjectRecords), "bucket_objects", len(view.bucketObjectsByURL))
			view.recordsByChecksum = probedRecordSet.recordsByChecksum
			view.allProjectRecords = probedRecordSet.allProjectRecords
			return view, nil
		}
		probeStart := time.Now()
		var (
			probedRecordSet *storageAuditRecordSet
			probeErr        error
			stage           string
		)
		switch validationMode {
		case StorageChainValidationModeMetadata:
			probedRecordSet, probeErr = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
			stage = "bulk_metadata_validation"
		default:
			probedRecordSet, probeErr = service.attachProjectStorageListValidations(ctx, authorizationHeader, recordSet)
			stage = "bulk_list_validation"
		}
		timings.Record(stage, time.Since(probeStart))
		if probedRecordSet != nil {
			timings.RecordMemory(stage, "syfon_records", countRecordStates(probedRecordSet.allProjectRecords))
		}
		if probeErr != nil {
			return nil, probeErr
		}
		view.recordsByChecksum = probedRecordSet.recordsByChecksum
		view.allProjectRecords = probedRecordSet.allProjectRecords
		view.bucketObjects, view.bucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.allProjectRecords)
		return view, nil
	}
	if bucketInventoryErr != nil {
		return nil, bucketInventoryErr
	}
	view.bucketObjects, view.bucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	if validationMode == StorageChainValidationModeInventory || validationMode == StorageChainValidationModeList {
		return view, nil
	}

	probeSelectStart := time.Now()
	probeCandidates := selectTargetedProbeRecordSet(recordSet, view.bucketObjectsByURL)
	timings.Record("targeted_probe_selection", time.Since(probeSelectStart))
	if probeCandidates == nil {
		return view, nil
	}
	probeStart := time.Now()
	probedSubset, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, probeCandidates)
	timings.Record("bulk_probe", time.Since(probeStart))
	if probedSubset != nil {
		timings.RecordMemory("bulk_probe", "probe_records", countRecordStates(probedSubset.allProjectRecords))
	}
	if probeErr != nil {
		return nil, probeErr
	}
	merged := mergeRecordSetProbes(recordSet, probedSubset)
	view.recordsByChecksum = merged.recordsByChecksum
	view.allProjectRecords = merged.allProjectRecords
	return view, nil
}

func (service *StorageAnalyticsService) loadStorageAuditStorageView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *storageAuditRecordSet, includeBucketInventory bool, includeProbes bool) (*storageAuditStorageView, error) {
	scopes, err := service.loadProjectScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
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
		bucketObjects, bucketObjectsByURL, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, "")
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
		inventory:            inventory,
		allRecords:           allRecords,
		bucketObjectsByURL:   bucketObjectsByURL,
		repoPathsByChecksum:  repoPathsByChecksum,
		recordsByBucketURL:   recordsByBucketURL,
		equivalentRecordKeys: buildEquivalentRecordKeyIndex(allRecords),
	}
}

func buildStorageChainAuditModel(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, includeBucketOrigin bool) *chainAuditModel {
	index := buildStorageChainIndex(inventory, allProjectRecords, bucketObjectsByURL)
	acc := chainAuditAccumulator{
		findings: make([]GitStorageChainFinding, 0),
		summary:  newChainSummary(len(bucketObjectsByURL), len(index.allRecords), len(inventory)),
	}
	if includeBucketOrigin {
		buildBucketOriginChainFindings(index, &acc)
	}
	buildSyfonOriginChainFindings(index, &acc, !includeBucketOrigin)
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
		if bucketObjectHasEquivalentSyfonRecord(item, index.equivalentRecordKeys) {
			continue
		}
		acc.add("bucket_only_object", buildChainBucketOnlyFinding(item))
	}
}

func buildEquivalentRecordKeyIndex(records []projectRecordState) equivalentRecordKeyIndex {
	index := equivalentRecordKeyIndex{
		byName:            make(map[string]struct{}),
		byNameSize:        make(map[string]map[int64]struct{}),
		unknownSizeByName: make(map[string]struct{}),
	}
	for _, record := range records {
		for _, accessURL := range accessURLsForStorage(record) {
			_, key, ok := parseStorageURL(accessURL)
			if !ok {
				continue
			}
			name := strings.TrimSpace(path.Base(key))
			if name == "" {
				continue
			}
			index.byName[name] = struct{}{}
			if record.Size <= 0 {
				index.unknownSizeByName[name] = struct{}{}
				continue
			}
			if index.byNameSize[name] == nil {
				index.byNameSize[name] = make(map[int64]struct{})
			}
			index.byNameSize[name][record.Size] = struct{}{}
		}
	}
	return index
}

func bucketObjectHasEquivalentSyfonRecord(item gintegrationsyfon.ProjectBucketObject, index equivalentRecordKeyIndex) bool {
	itemName := strings.TrimSpace(path.Base(strings.TrimSpace(item.Key)))
	if itemName == "" {
		return false
	}
	if _, ok := index.byName[itemName]; !ok {
		return false
	}
	if item.SizeBytes <= 0 {
		return true
	}
	if _, ok := index.unknownSizeByName[itemName]; ok {
		return true
	}
	if sizes := index.byNameSize[itemName]; sizes != nil {
		_, ok := sizes[item.SizeBytes]
		return ok
	}
	return false
}

func buildSyfonOriginChainFindings(index storageChainIndex, acc *chainAuditAccumulator, countCompleteFromSyfon bool) {
	for _, record := range index.allRecords {
		gitPaths := uniqueStrings(index.repoPathsByChecksum[normalizeAnalyticsChecksum(record.Checksum)])
		bucketMatches := matchedBucketObjectURLs(record, index.bucketObjectsByURL)
		switch classifyStorageFinding(record, index.bucketObjectsByURL) {
		case storageFindingBrokenBucketMap:
			findings := buildChainRecordFindingsWithOptions("syfon_broken_bucket_mapping", record, gitPaths, bucketMatches, "Syfon access URL did not resolve through a configured bucket mapping.", true)
			acc.findings = append(acc.findings, findings...)
			acc.addCount("syfon_broken_bucket_mapping", len(findings))
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
				findings := buildChainRecordFindings("git_syfon_metadata_mismatch", record, gitPaths, bucketMatches, "Mapped bucket object exists, but object evidence disagrees with the Syfon/Git record.")
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
			if countCompleteFromSyfon && len(gitPaths) > 0 && len(bucketMatches) > 0 {
				acc.addCount("bucket_syfon_git_complete", len(gitPaths))
				continue
			}
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
		actionability, availableActions, defaultAction, supportsDryRun := storageChainActionSupport("git_only_no_syfon")
		acc.add("git_only_no_syfon", GitStorageChainFinding{
			Kind:              "git_only_no_syfon",
			NormalizedPath:    item.RepoPath,
			Checksum:          checksum,
			SourcePaths:       []string{item.RepoPath},
			ObjectIDs:         []string{},
			RecordCount:       0,
			SizeBytes:         item.Size,
			RecommendedAction: "Git checksum has no matching Syfon record. Bucket presence is not claimed by this finding.",
			Actionability:     actionability,
			AvailableActions:  availableActions,
			DefaultAction:     defaultAction,
			SupportsDryRun:    supportsDryRun,
			Evidence: &GitAuditEvidence{
				Checksum:         checksum,
				SourcePaths:      []string{item.RepoPath},
				ObjectIDs:        []string{},
				BucketEvaluation: "not_evaluated",
			},
		})
	}
}
