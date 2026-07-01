package git

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

const cleanupInactiveDays = 30
const projectJoinCacheTTL = 45 * time.Second

type storageAnalyticsBackend interface {
	ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error)
	ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error)
	ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
	ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectBucketObject, error)
	BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error
	DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error)
	BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error
}

type StorageAnalyticsService struct {
	storage          storageAnalyticsBackend
	projectJoinMu    sync.RWMutex
	projectJoinCache map[string]cachedProjectJoinState
}

func NewStorageAnalyticsService(storage storageAnalyticsBackend) *StorageAnalyticsService {
	if storage == nil {
		return nil
	}
	return &StorageAnalyticsService{
		storage:          storage,
		projectJoinCache: map[string]cachedProjectJoinState{},
	}
}

type RepoInventoryFile struct {
	RepoPath string
	Name     string
	Checksum string
	Size     int64
}

type projectRecordState struct {
	gintegrationsyfon.ProjectRecord
	CanonicalAccessURLs []string
	Usage               gintegrationsyfon.FileUsage
	AccessProbes        []gintegrationsyfon.BulkStorageProbeResult
}

type storageAggregate struct {
	name           string
	path           string
	rowType        string
	fileCount      int
	recordCount    int
	totalBytes     int64
	downloadCount  int64
	lastDownload   *time.Time
	latestUpdate   *time.Time
	duplicateCount int
}

type projectDiffAuditModel struct {
	Findings   []GitProjectDiffFinding
	Summary    GitProjectDiffSummary
	PathPrefix string
}

type cleanupFindingModel struct {
	Public              GitStorageCleanupFinding
	DeleteObjectIDs     []string
	DeleteStorageData   bool
	DeleteBucketObjects []string
	UpdateAccessMethods map[string][]gintegrationsyfon.ProjectAccessMethod
	Manual              bool
}

type cleanupAuditModel struct {
	Findings             []cleanupFindingModel
	PublicFindings       []GitStorageCleanupFinding
	Summary              GitStorageCleanupAuditSummary
	ExpectedPathCount    int
	IncludesRepoManifest bool
	PathPrefix           string
}

type chainAuditModel struct {
	Findings   []GitStorageChainFinding
	Summary    GitStorageChainAuditSummary
	PathPrefix string
}

type cachedProjectJoinState struct {
	expiresAt         time.Time
	recordsByChecksum map[string][]projectRecordState
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

func BuildGitRepoInventory(ref string, gitSubpath string, repo *gogit.Repository, hash plumbing.Hash) ([]RepoInventoryFile, error) {
	index, err := buildRepoAnalyticsIndex(ref, repo, hash)
	if err != nil {
		return nil, err
	}
	return filterRepoInventoryFiles(index, gitSubpath)
}

func (service *StorageAnalyticsService) BuildStorageSummary(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*GitStorageSummaryResponse, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	directory, err := repoDirectoryAggregate(index, gitSubpath)
	if err != nil {
		return nil, err
	}
	summaryAgg := summarizeSubtree(gitSubpath, inventory, recordsByChecksum, usageByObjectID, directory.DirectChildCount)
	return &GitStorageSummaryResponse{
		Path:               summaryAgg.path,
		FileCount:          summaryAgg.fileCount,
		RecordCount:        summaryAgg.recordCount,
		DirectChildCount:   directory.DirectChildCount,
		TotalBytes:         summaryAgg.totalBytes,
		DownloadCount:      summaryAgg.downloadCount,
		LastDownloadTime:   formatOptionalTime(summaryAgg.lastDownload),
		LatestUpdateTime:   formatOptionalTime(summaryAgg.latestUpdate),
		DuplicatePathCount: summaryAgg.duplicateCount,
	}, nil
}

func (service *StorageAnalyticsService) BuildStorageChildren(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string) (*GitStorageChildrenResponse, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	directory, err := repoDirectoryAggregate(index, gitSubpath)
	if err != nil {
		return nil, err
	}
	aggregates := aggregateImmediateChildren(gitSubpath, inventory, recordsByChecksum, usageByObjectID, cloneDirectoryChildren(directory.Children))
	sortStorageAggregates(aggregates, sortBy, sortOrder)
	if limit > 0 && len(aggregates) > limit {
		aggregates = aggregates[:limit]
	}
	items := make([]GitStorageChildResponseItem, 0, len(aggregates))
	for _, agg := range aggregates {
		items = append(items, GitStorageChildResponseItem{
			Name:             agg.name,
			Path:             agg.path,
			Type:             agg.rowType,
			FileCount:        agg.fileCount,
			RecordCount:      agg.recordCount,
			TotalBytes:       agg.totalBytes,
			DownloadCount:    agg.downloadCount,
			LastDownloadTime: formatOptionalTime(agg.lastDownload),
			LatestUpdateTime: formatOptionalTime(agg.latestUpdate),
		})
	}
	return &GitStorageChildrenResponse{Items: items}, nil
}

func (service *StorageAnalyticsService) BuildProjectDiffAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*GitProjectDiffAuditResponse, error) {
	_, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	allProjectRecords, err := service.listProjectRecordStates(ctx, authorizationHeader, organization, project, usageByObjectID)
	if err != nil {
		return nil, err
	}
	model := buildProjectDiffAuditModel(gitSubpath, inventory, recordsByChecksum, allProjectRecords)
	return &GitProjectDiffAuditResponse{
		Findings:   model.Findings,
		Summary:    model.Summary,
		PathPrefix: model.PathPrefix,
	}, nil
}

func (service *StorageAnalyticsService) BuildStorageCleanupAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, selectedRepoPaths []string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, checkStorage bool) (*GitStorageCleanupAuditResponse, *cleanupAuditModel, error) {
	baseInputs, err := service.loadStorageAuditBaseInputs(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, nil, err
	}
	recordSet, err := service.loadScopedProjectRecords(ctx, authorizationHeader, organization, project, baseInputs)
	if err != nil {
		return nil, nil, err
	}
	storageView, err := service.loadStorageAuditStorageView(ctx, authorizationHeader, organization, project, recordSet, checkStorage, checkStorage)
	if err != nil {
		return nil, nil, err
	}
	model := buildCleanupAuditModel(gitSubpath, baseInputs.inventory, storageView.recordsByChecksum, storageView.allProjectRecords, storageView.bucketObjectsByURL, selectedRepoPaths, checkStorage)
	return &GitStorageCleanupAuditResponse{
		Findings:             model.PublicFindings,
		Summary:              model.Summary,
		ExpectedPathCount:    model.ExpectedPathCount,
		IncludesRepoManifest: model.IncludesRepoManifest,
		PathPrefix:           model.PathPrefix,
	}, model, nil
}

func (service *StorageAnalyticsService) BuildStorageChainAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*GitStorageChainAuditResponse, error) {
	inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	recordSet, err := service.loadProjectAuditRecordSet(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	storageView, err := service.loadStorageChainView(ctx, authorizationHeader, organization, project, recordSet)
	if err != nil {
		return nil, err
	}
	model := buildStorageChainAuditModel(gitSubpath, inventory, storageView.recordsByChecksum, storageView.allProjectRecords, storageView.bucketObjectsByURL)
	model.Summary.BucketInventoryAvailable = storageView.bucketInventoryAvailable
	model.Summary.BucketInventoryError = storageView.bucketInventoryError
	return &GitStorageChainAuditResponse{
		Findings:   append([]GitStorageChainFinding(nil), model.Findings...),
		Groups:     summarizeChainIssueGroups(model.Findings),
		Summary:    model.Summary,
		PathPrefix: model.PathPrefix,
	}, nil
}

func (service *StorageAnalyticsService) ApplyStorageCleanup(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, selectedRepoPaths []string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, checkStorage bool, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool, dryRun bool) (*GitStorageCleanupApplyResponse, error) {
	_, model, err := service.BuildStorageCleanupAudit(ctx, authorizationHeader, organization, project, ref, gitSubpath, selectedRepoPaths, mirrorPath, repo, hash, checkStorage)
	if err != nil {
		return nil, err
	}
	toDelete := make([]string, 0)
	toDeleteWithStorage := make([]string, 0)
	toDeleteBucketObjects := make([]string, 0)
	toUpdate := make(map[string][]gintegrationsyfon.ProjectAccessMethod)
	repoDeletePaths := make([]string, 0)
	deletedBucketObjectURLs := make([]string, 0)
	updatedRecordIDs := make([]string, 0)
	manualPaths := make([]string, 0)
	skippedPaths := make([]string, 0)
	for _, finding := range model.Findings {
		switch finding.Public.Kind {
		case "repo_orphan_live_object", "repo_orphan_stale_record":
			if deleteRepoOrphans {
				toDelete = append(toDelete, finding.DeleteObjectIDs...)
				if finding.DeleteStorageData {
					toDeleteWithStorage = append(toDeleteWithStorage, finding.DeleteObjectIDs...)
				}
				repoDeletePaths = append(repoDeletePaths, finding.Public.NormalizedPath)
			} else {
				skippedPaths = append(skippedPaths, finding.Public.NormalizedPath)
			}
		case "stale_duplicate_record":
			if deleteStaleDuplicates {
				toDelete = append(toDelete, finding.DeleteObjectIDs...)
			} else {
				skippedPaths = append(skippedPaths, finding.Public.NormalizedPath)
			}
		case "broken_bucket_mapping":
			if repairBrokenBucketMappings {
				toDelete = append(toDelete, finding.DeleteObjectIDs...)
				for objectID, methods := range finding.UpdateAccessMethods {
					if trimmed := strings.TrimSpace(objectID); trimmed != "" {
						toUpdate[trimmed] = append([]gintegrationsyfon.ProjectAccessMethod(nil), methods...)
						updatedRecordIDs = append(updatedRecordIDs, trimmed)
					}
				}
			} else {
				skippedPaths = append(skippedPaths, finding.Public.NormalizedPath)
			}
		case "bucket_only_object":
			if deleteBucketOnlyObjects {
				toDeleteBucketObjects = append(toDeleteBucketObjects, finding.DeleteBucketObjects...)
			} else {
				skippedPaths = append(skippedPaths, finding.Public.NormalizedPath)
			}
		default:
			manualPaths = append(manualPaths, finding.Public.NormalizedPath)
		}
	}
	toDelete = uniqueStrings(toDelete)
	toDeleteWithStorage = uniqueStrings(toDeleteWithStorage)
	toDeleteMetadataOnly := differenceStrings(toDelete, toDeleteWithStorage)
	toDeleteBucketObjects = uniqueStrings(toDeleteBucketObjects)
	repoDeletePaths = uniqueStrings(repoDeletePaths)
	deletedBucketObjectURLs = uniqueStrings(deletedBucketObjectURLs)
	updatedRecordIDs = uniqueStrings(updatedRecordIDs)
	manualPaths = uniqueStrings(manualPaths)
	skippedPaths = uniqueStrings(skippedPaths)
	purgeResults := make([]GitStorageCleanupPurgeResult, 0, len(toDelete)+len(toDeleteBucketObjects))
	if dryRun {
		for _, objectID := range toDelete {
			purgeResults = append(purgeResults, GitStorageCleanupPurgeResult{
				ObjectID: objectID,
				Success:  nil,
				Status:   "dry_run",
			})
		}
		return &GitStorageCleanupApplyResponse{
			DeletedRecordIDs:        toDelete,
			DeletedBucketObjectURLs: toDeleteBucketObjects,
			UpdatedRecordIDs:        updatedRecordIDs,
			PurgeResults:            purgeResults,
			RepoDeletePaths:         repoDeletePaths,
			ManualPaths:             manualPaths,
			SkippedPaths:            skippedPaths,
			DryRun:                  true,
		}, nil
	}
	if len(toUpdate) > 0 {
		if err := service.storage.BulkUpdateAccessMethods(ctx, authorizationHeader, toUpdate); err != nil {
			return nil, fmt.Errorf("update syfon access methods: %w", err)
		}
	}
	if len(toDeleteMetadataOnly) > 0 {
		if err := service.storage.BulkDeleteObjects(ctx, authorizationHeader, toDeleteMetadataOnly, false); err != nil {
			return nil, fmt.Errorf("delete syfon objects: %w", err)
		}
	}
	if len(toDeleteWithStorage) > 0 {
		if err := service.storage.BulkDeleteObjects(ctx, authorizationHeader, toDeleteWithStorage, true); err != nil {
			return nil, fmt.Errorf("delete syfon objects: %w", err)
		}
	}
	if len(toDeleteBucketObjects) > 0 {
		results, err := service.storage.DeleteProjectBucketObjects(ctx, authorizationHeader, organization, project, toDeleteBucketObjects)
		if err != nil {
			return nil, fmt.Errorf("delete syfon project bucket objects: %w", err)
		}
		for _, result := range results {
			if strings.EqualFold(strings.TrimSpace(result.Status), "deleted") {
				deletedBucketObjectURLs = append(deletedBucketObjectURLs, result.ObjectURL)
				continue
			}
			purgeResults = append(purgeResults, GitStorageCleanupPurgeResult{
				ObjectID: result.ObjectURL,
				Success:  boolPtr(false),
				Status:   strings.TrimSpace(result.Status),
				Error:    strings.TrimSpace(result.Error),
			})
		}
		deletedBucketObjectURLs = uniqueStrings(deletedBucketObjectURLs)
	}
	if len(toUpdate) > 0 || len(toDelete) > 0 {
		service.evictProjectJoinCache(organization, project)
	}
	for _, objectID := range toDelete {
		success := true
		purgeResults = append(purgeResults, GitStorageCleanupPurgeResult{
			ObjectID: objectID,
			Success:  &success,
			Status:   "deleted",
		})
	}
	return &GitStorageCleanupApplyResponse{
		DeletedRecordIDs:        toDelete,
		DeletedBucketObjectURLs: deletedBucketObjectURLs,
		UpdatedRecordIDs:        updatedRecordIDs,
		PurgeResults:            purgeResults,
		RepoDeletePaths:         repoDeletePaths,
		ManualPaths:             manualPaths,
		SkippedPaths:            skippedPaths,
		DryRun:                  false,
	}, nil
}

func (service *StorageAnalyticsService) loadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*repoAnalyticsIndex, []RepoInventoryFile, map[string][]projectRecordState, map[string]gintegrationsyfon.FileUsage, error) {
	index, err := loadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	inventory, err := filterRepoInventoryFiles(index, gitSubpath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	recordsByChecksum, usageByObjectID, err := service.loadProjectJoinCache(ctx, authorizationHeader, organization, project, hash, index.sidecar.Files)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return index, inventory, recordsByChecksum, usageByObjectID, nil
}

func (service *StorageAnalyticsService) loadProjectJoinCache(ctx context.Context, authorizationHeader string, organization string, project string, hash plumbing.Hash, inventory []RepoInventoryFile) (map[string][]projectRecordState, map[string]gintegrationsyfon.FileUsage, error) {
	cacheKey := service.projectJoinCacheKey(organization, project, hash.String())
	service.projectJoinMu.RLock()
	cached, ok := service.projectJoinCache[cacheKey]
	service.projectJoinMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.recordsByChecksum, cached.usageByObjectID, nil
	}
	checksums := make([]string, 0, len(inventory))
	for _, item := range inventory {
		checksums = append(checksums, item.Checksum)
	}
	recordsByChecksumRaw, err := service.storage.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, checksums)
	if err != nil {
		return nil, nil, fmt.Errorf("lookup syfon project records by checksum: %w", err)
	}
	usageByObjectID, err := service.storage.ListProjectFileUsage(ctx, authorizationHeader, organization, project, cleanupInactiveDays)
	if err != nil {
		return nil, nil, fmt.Errorf("list syfon project file usage: %w", err)
	}
	recordsByChecksum := make(map[string][]projectRecordState, len(recordsByChecksumRaw))
	for _, records := range recordsByChecksumRaw {
		for _, record := range records {
			normalizedChecksum := normalizeAnalyticsChecksum(record.Checksum)
			if normalizedChecksum == "" {
				continue
			}
			record.Checksum = normalizedChecksum
			recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], projectRecordState{
				ProjectRecord: record,
				Usage:         usageByObjectID[record.ObjectID],
			})
		}
	}
	service.projectJoinMu.Lock()
	service.projectJoinCache[cacheKey] = cachedProjectJoinState{
		expiresAt:         time.Now().Add(projectJoinCacheTTL),
		recordsByChecksum: recordsByChecksum,
		usageByObjectID:   usageByObjectID,
	}
	service.projectJoinMu.Unlock()
	return recordsByChecksum, usageByObjectID, nil
}

func (service *StorageAnalyticsService) projectJoinCacheKey(organization string, project string, commitHash string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::" + strings.TrimSpace(commitHash)
}

func (service *StorageAnalyticsService) evictProjectJoinCache(organization string, project string) {
	prefix := strings.TrimSpace(organization) + "/" + strings.TrimSpace(project) + "::"
	service.projectJoinMu.Lock()
	defer service.projectJoinMu.Unlock()
	for key := range service.projectJoinCache {
		if strings.HasPrefix(key, prefix) {
			delete(service.projectJoinCache, key)
		}
	}
}

func (service *StorageAnalyticsService) listProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]projectRecordState, error) {
	projectRecords, err := service.storage.ListProjectRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project records: %w", err)
	}
	out := make(map[string][]projectRecordState)
	for _, record := range projectRecords {
		normalizedChecksum := normalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		out[normalizedChecksum] = append(out[normalizedChecksum], projectRecordState{
			ProjectRecord: record,
			Usage:         usageByObjectID[record.ObjectID],
		})
	}
	return out, nil
}

func (service *StorageAnalyticsService) loadProjectStorageScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	buckets, err := service.storage.ListBuckets(ctx, authorizationHeader)
	if err != nil {
		return nil, fmt.Errorf("list syfon buckets: %w", err)
	}
	bucketNames := make([]string, 0, len(buckets))
	for bucket := range buckets {
		bucketNames = append(bucketNames, bucket)
	}
	sort.Strings(bucketNames)
	scopes := make([]domain.StorageBucketScope, 0)
	for _, bucket := range bucketNames {
		items, err := service.storage.ListBucketScopes(ctx, authorizationHeader, bucket)
		if err != nil {
			return nil, fmt.Errorf("list syfon bucket scopes for %s: %w", bucket, err)
		}
		for _, scope := range items {
			if !strings.EqualFold(strings.TrimSpace(scope.Organization), organization) {
				continue
			}
			scopeProject := strings.TrimSpace(scope.ProjectID)
			if scopeProject != "" && !strings.EqualFold(scopeProject, project) {
				continue
			}
			scopes = append(scopes, scope)
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

func applyScopedStorageMappings(recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, scopes []domain.StorageBucketScope) (map[string][]projectRecordState, map[string][]projectRecordState) {
	attach := func(input map[string][]projectRecordState) map[string][]projectRecordState {
		out := make(map[string][]projectRecordState, len(input))
		for checksum, group := range input {
			states := make([]projectRecordState, 0, len(group))
			for _, record := range group {
				clone := record
				clone.CanonicalAccessURLs = canonicalizeRecordAccessURLs(record.AccessURLs, scopes)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return attach(recordsByChecksum), attach(allProjectRecords)
}

func (service *StorageAnalyticsService) attachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState) (map[string][]projectRecordState, map[string][]projectRecordState, error) {
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemKeys := map[string]string{}
	recordProbeKeysByObjectID := map[string][]string{}

	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, accessURL := range probeAccessURLsForRecord(record) {
				normalizedURL := strings.TrimSpace(accessURL)
				if normalizedURL == "" {
					continue
				}
				key := storageProbeRequestKey(normalizedURL, record.Size, record.Checksum)
				recordProbeKeysByObjectID[record.ObjectID] = append(recordProbeKeysByObjectID[record.ObjectID], key)
				if _, ok := itemKeys[key]; ok {
					continue
				}
				itemKeys[key] = key
				expectedSize := record.Size
				items = append(items, gintegrationsyfon.BulkStorageProbeItem{
					ID:                key,
					ObjectURL:         normalizedURL,
					ExpectedSizeBytes: &expectedSize,
					ExpectedSHA256:    strings.TrimSpace(record.Checksum),
				})
			}
		}
	}

	resultsByKey := map[string]gintegrationsyfon.BulkStorageProbeResult{}
	if len(items) > 0 {
		results, err := service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, items)
		if err != nil {
			return nil, nil, fmt.Errorf("probe syfon storage objects: %w", err)
		}
		for _, result := range results {
			resultsByKey[strings.TrimSpace(result.ID)] = result
		}
	}

	attach := func(input map[string][]projectRecordState) map[string][]projectRecordState {
		out := make(map[string][]projectRecordState, len(input))
		for checksum, group := range input {
			states := make([]projectRecordState, 0, len(group))
			for _, record := range group {
				clone := record
				keys := uniqueStrings(recordProbeKeysByObjectID[record.ObjectID])
				probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(keys))
				for _, key := range keys {
					if result, ok := resultsByKey[key]; ok {
						probes = append(probes, result)
					}
				}
				clone.AccessProbes = probes
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}

	return attach(recordsByChecksum), attach(allProjectRecords), nil
}

func summarizeSubtree(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, usageByObjectID map[string]gintegrationsyfon.FileUsage, directChildCount int) storageAggregate {
	agg := storageAggregate{
		path:    normalizeRepoSubpath(gitSubpath),
		rowType: "directory",
	}
	for _, item := range inventory {
		agg.fileCount++
		agg.totalBytes += item.Size
		matches := recordsByChecksum[normalizeAnalyticsChecksum(item.Checksum)]
		agg.recordCount += len(matches)
		if len(matches) > 1 {
			agg.duplicateCount++
		}
		for _, record := range matches {
			applyUsage(&agg, record)
		}
	}
	_ = usageByObjectID
	_ = directChildCount
	return agg
}

func aggregateImmediateChildren(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, usageByObjectID map[string]gintegrationsyfon.FileUsage, aggregates []storageAggregate) []storageAggregate {
	root := normalizeRepoSubpath(gitSubpath)
	if aggregates == nil {
		aggregates = make([]storageAggregate, 0)
	}
	aggregateLookup := make(map[string]*storageAggregate, len(aggregates))
	for index := range aggregates {
		aggregateLookup[aggregates[index].path] = &aggregates[index]
	}
	for _, item := range inventory {
		childName, childPath, childType := immediateChild(root, item.RepoPath)
		if childPath == "" {
			continue
		}
		wasPrecomputed := true
		agg := aggregateLookup[childPath]
		if agg == nil {
			wasPrecomputed = false
			agg = &storageAggregate{
				name:    childName,
				path:    childPath,
				rowType: childType,
			}
			aggregates = append(aggregates, *agg)
			agg = &aggregates[len(aggregates)-1]
			aggregateLookup[childPath] = agg
		}
		if !wasPrecomputed {
			agg.fileCount++
			agg.totalBytes += item.Size
		}
		matches := recordsByChecksum[normalizeAnalyticsChecksum(item.Checksum)]
		agg.recordCount += len(matches)
		if len(matches) > 1 {
			agg.duplicateCount++
		}
		for _, record := range matches {
			applyUsage(agg, record)
		}
	}
	_ = usageByObjectID
	return aggregates
}

func buildProjectDiffAuditModel(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState) *projectDiffAuditModel {
	findings := make([]GitProjectDiffFinding, 0)
	countsByKind := map[string]int{
		"duplicate_syfon_paths": 0,
		"syfon_missing_in_repo": 0,
		"repo_missing_in_syfon": 0,
		"unknown":               0,
	}
	matchedPathCount := 0
	scannedRecordCount := 0
	repoChecksums := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		normalizedChecksum := normalizeAnalyticsChecksum(item.Checksum)
		repoChecksums[normalizedChecksum] = struct{}{}
		matches := recordsByChecksum[normalizedChecksum]
		scannedRecordCount += len(matches)
		if len(matches) > 0 {
			matchedPathCount++
		}
		if len(matches) == 0 {
			evidence := buildFindingEvidence(item.Checksum, []string{item.RepoPath}, nil, "not_checked")
			findings = append(findings, GitProjectDiffFinding{
				Kind:              "repo_missing_in_syfon",
				NormalizedPath:    item.RepoPath,
				Checksum:          item.Checksum,
				SourcePaths:       []string{item.RepoPath},
				ObjectIDs:         []string{},
				RecordCount:       0,
				SizeBytes:         item.Size,
				RecommendedAction: "No Syfon record matched this Git-tracked checksum. Bucket presence is not part of this check; review ingest or metadata mapping for this path.",
				Evidence:          evidence,
			})
			countsByKind["repo_missing_in_syfon"]++
			continue
		}
		if len(matches) > 1 {
			evidence := buildFindingEvidence(item.Checksum, []string{item.RepoPath}, matches, "not_checked")
			findings = append(findings, GitProjectDiffFinding{
				Kind:              "duplicate_syfon_paths",
				NormalizedPath:    item.RepoPath,
				Checksum:          item.Checksum,
				SourcePaths:       recordSourcePaths(matches),
				ObjectIDs:         recordObjectIDs(matches),
				RecordCount:       len(matches),
				SizeBytes:         aggregateMatchedSize(matches, item.Size),
				DownloadCount:     aggregateMatchedDownloads(matches),
				LastDownload:      formatOptionalTime(latestMatchedDownload(matches)),
				RecommendedAction: "Review duplicate Syfon records before deleting anything.",
				Evidence:          evidence,
			})
			countsByKind["duplicate_syfon_paths"]++
		}
	}
	seenOrphanChecksums := map[string]struct{}{}
	for checksum, matches := range allProjectRecords {
		if len(matches) == 0 {
			continue
		}
		if _, ok := repoChecksums[checksum]; ok {
			continue
		}
		if _, ok := seenOrphanChecksums[checksum]; ok {
			continue
		}
		seenOrphanChecksums[checksum] = struct{}{}
		sourcePaths := recordSourcePaths(matches)
		evidence := buildFindingEvidence(checksum, nil, matches, "not_checked")
		findings = append(findings, GitProjectDiffFinding{
			Kind:              "syfon_missing_in_repo",
			NormalizedPath:    orphanDisplayPath(checksum, sourcePaths),
			Checksum:          checksum,
			SourcePaths:       sourcePaths,
			ObjectIDs:         recordObjectIDs(matches),
			RecordCount:       len(matches),
			SizeBytes:         aggregateMatchedSize(matches, 0),
			DownloadCount:     aggregateMatchedDownloads(matches),
			LastDownload:      formatOptionalTime(latestMatchedDownload(matches)),
			RecommendedAction: "Prepare delete to verify storage before removing Syfon-only records.",
			Evidence:          evidence,
		})
		countsByKind["syfon_missing_in_repo"]++
	}
	return &projectDiffAuditModel{
		Findings: findings,
		Summary: GitProjectDiffSummary{
			CountsByKind:         countsByKind,
			TotalFindings:        len(findings),
			IndexedPathCount:     matchedPathCount,
			ExpectedPathCount:    len(inventory),
			MatchedPathCount:     matchedPathCount,
			IncludesRepoManifest: true,
			ScannedRecordCount:   scannedRecordCount,
		},
		PathPrefix: normalizeRepoSubpath(gitSubpath),
	}
}

func buildCleanupAuditModel(gitSubpath string, inventory []RepoInventoryFile, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, selectedRepoPaths []string, checkStorage bool) *cleanupAuditModel {
	allowed := make(map[string]struct{}, len(selectedRepoPaths))
	for _, path := range selectedRepoPaths {
		if normalized := normalizeRepoSubpath(path); normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	includePath := func(path string) bool {
		if len(allowed) == 0 {
			return true
		}
		_, ok := allowed[normalizeRepoSubpath(path)]
		return ok
	}
	findings := make([]cleanupFindingModel, 0)
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
		normalizedChecksum := normalizeAnalyticsChecksum(item.Checksum)
		repoChecksums[normalizedChecksum] = struct{}{}
		matches := recordsByChecksum[normalizedChecksum]
		if !includePath(item.RepoPath) || len(matches) == 0 {
			continue
		}
		if len(matches) > 1 {
			sortedMatches := append([]projectRecordState(nil), matches...)
			sort.SliceStable(sortedMatches, func(i, j int) bool {
				return compareRecordState(sortedMatches[i], sortedMatches[j]) > 0
			})
			if compareRecordState(sortedMatches[0], sortedMatches[1]) == 0 {
				public := buildCleanupFinding("live_duplicate_conflict", item.RepoPath, sortedMatches, false, "record", "Manual review required for live duplicate records.")
				findings = append(findings, cleanupFindingModel{Public: public, Manual: true})
				countsByKind["live_duplicate_conflict"]++
			} else {
				candidates := make([]projectRecordState, 0, len(sortedMatches)-1)
				deleteIDs := make([]string, 0, len(sortedMatches)-1)
				for _, record := range sortedMatches[1:] {
					candidates = append(candidates, record)
					deleteIDs = append(deleteIDs, record.ObjectID)
				}
				public := buildCleanupFinding("stale_duplicate_record", item.RepoPath, sortedMatches, true, "record", "Delete stale duplicate records")
				findings = append(findings, cleanupFindingModel{Public: public, DeleteObjectIDs: deleteIDs})
				countsByKind["stale_duplicate_record"]++
			}
			continue
		}
		if checkStorage {
			for _, bucketURL := range matchedBucketObjectURLs(matches[0], bucketObjectsByURL) {
				referencedBucketURLs[bucketURL] = struct{}{}
			}
			switch storageFindingKind := classifyStorageFinding(matches[0], bucketObjectsByURL); storageFindingKind {
			case storageFindingBrokenAccessURL:
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Manual review required for broken access URLs")
				findings = append(findings, cleanupFindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			case storageFindingBrokenBucketMap:
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Fix or remove the Syfon access URL because no bucket mapping is configured for it")
				repairDeleteIDs, repairUpdates := brokenBucketMappingRepairPlan(matches)
				findings = append(findings, cleanupFindingModel{
					Public:              public,
					DeleteObjectIDs:     repairDeleteIDs,
					UpdateAccessMethods: repairUpdates,
					Manual:              len(repairDeleteIDs) == 0 && len(repairUpdates) == 0,
				})
				countsByKind[string(storageFindingKind)]++
			case storageFindingObjectMissing:
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Storage object is missing for this Syfon access URL")
				findings = append(findings, cleanupFindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			case storageFindingValidationMismatch:
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Storage metadata does not match the Syfon record")
				findings = append(findings, cleanupFindingModel{Public: public, Manual: true})
				countsByKind[string(storageFindingKind)]++
			case storageFindingProbeError:
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Manual review required for storage probe errors")
				findings = append(findings, cleanupFindingModel{Public: public, Manual: true})
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
		displayPath := orphanDisplayPath(checksum, recordSourcePaths(matches))
		if !includePath(displayPath) {
			continue
		}
		kind := "repo_orphan_stale_record"
		if checkStorage && recordsReferenceBucketObject(matches, bucketObjectsByURL) {
			kind = "repo_orphan_live_object"
		} else if recordsContainLiveUsage(matches) {
			kind = "repo_orphan_live_object"
		}
		if checkStorage {
			for _, match := range matches {
				for _, bucketURL := range matchedBucketObjectURLs(match, bucketObjectsByURL) {
					referencedBucketURLs[bucketURL] = struct{}{}
				}
			}
		}
		public := buildCleanupFinding(kind, displayPath, matches, true, "record", repoOrphanAction(kind))
		findings = append(findings, cleanupFindingModel{
			Public:            public,
			DeleteObjectIDs:   recordObjectIDs(matches),
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
			if !includePath(objectURL) {
				continue
			}
			public := buildBucketOnlyFinding(bucketObjectsByURL[objectURL])
			findings = append(findings, cleanupFindingModel{
				Public:              public,
				DeleteBucketObjects: []string{objectURL},
			})
			countsByKind["bucket_only_object"]++
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		return findings[i].Public.NormalizedPath < findings[j].Public.NormalizedPath
	})
	publicFindings := make([]GitStorageCleanupFinding, 0, len(findings))
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
	return &cleanupAuditModel{
		Findings:       findings,
		PublicFindings: publicFindings,
		Summary: GitStorageCleanupAuditSummary{
			CountsByKind:             countsByKind,
			TotalFindings:            len(publicFindings),
			ManualFindingCount:       manualFindingCount,
			RepoDeleteCandidateCount: repoDeleteCandidateCount,
			StaleDuplicateCount:      staleDuplicateCount,
			RepoOrphanCount:          repoOrphanCount,
		},
		ExpectedPathCount:    len(inventory),
		IncludesRepoManifest: true,
		PathPrefix:           normalizeRepoSubpath(gitSubpath),
	}
}

func bucketObjectHasCompleteChain(matches []projectRecordState, repoPathsByChecksum map[string][]string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if classifyStorageFinding(match, bucketObjectsByURL) != storageFindingNone {
			continue
		}
		if len(uniqueStrings(repoPathsByChecksum[normalizeAnalyticsChecksum(match.Checksum)])) > 0 {
			return true
		}
	}
	return false
}

func buildCleanupFinding(kind string, normalizedPath string, matches []projectRecordState, repoDeleteCandidate bool, cleanupScope string, action string) GitStorageCleanupFinding {
	records := make([]GitStorageCleanupRecordAudit, 0, len(matches))
	var latestUpdate *time.Time
	var latestDownload *time.Time
	var totalBytes int64
	var totalDownloads int64
	checksum := ""
	for _, match := range matches {
		if checksum == "" {
			checksum = strings.TrimSpace(match.Checksum)
		}
		records = append(records, GitStorageCleanupRecordAudit{
			ObjectID:       match.ObjectID,
			Checksum:       strings.TrimSpace(match.Checksum),
			NormalizedPath: normalizedPath,
			CleanupScope:   cleanupScope,
			AccessProbes:   accessProbesForRecord(match),
			Status:         accessStatusForRecord(match),
			Error:          accessErrorForRecord(match),
			SizeBytes:      match.Size,
			LastUpdated:    formatOptionalTime(match.UpdatedAt),
			DownloadCount:  match.Usage.DownloadCount,
			LastDownload:   formatOptionalTime(match.Usage.LastDownloadTime),
		})
		totalBytes += match.Size
		totalDownloads += match.Usage.DownloadCount
		latestUpdate = laterTime(latestUpdate, match.UpdatedAt)
		latestDownload = laterTime(latestDownload, match.Usage.LastDownloadTime)
	}
	bucketEvaluation := "not_checked"
	if cleanupScope == "access_url" {
		bucketEvaluation = "probed"
	}
	return GitStorageCleanupFinding{
		Kind:                kind,
		NormalizedPath:      normalizedPath,
		Checksum:            checksum,
		ObjectIDs:           recordObjectIDs(matches),
		Records:             records,
		RecommendedAction:   action,
		RepoDeleteCandidate: repoDeleteCandidate,
		CleanupScope:        cleanupScope,
		SizeBytes:           totalBytes,
		LastUpdated:         formatOptionalTime(latestUpdate),
		DownloadCount:       totalDownloads,
		LastDownload:        formatOptionalTime(latestDownload),
		Evidence:            buildFindingEvidence(checksum, nil, matches, bucketEvaluation),
	}
}

func repoOrphanAction(kind string) string {
	if kind == "repo_orphan_live_object" {
		return "Delete Syfon record and purge storage object"
	}
	return "Delete stale Syfon record"
}

func compareRecordState(left projectRecordState, right projectRecordState) int {
	if left.Usage.DownloadCount != right.Usage.DownloadCount {
		if left.Usage.DownloadCount > right.Usage.DownloadCount {
			return 1
		}
		return -1
	}
	if compareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime) != 0 {
		return compareOptionalTime(left.Usage.LastDownloadTime, right.Usage.LastDownloadTime)
	}
	return compareOptionalTime(left.UpdatedAt, right.UpdatedAt)
}

func compareOptionalTime(left *time.Time, right *time.Time) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return -1
	case right == nil:
		return 1
	case left.After(*right):
		return 1
	case left.Before(*right):
		return -1
	default:
		return 0
	}
}

func recordHasBrokenAccess(record projectRecordState) bool {
	if len(record.AccessURLs) == 0 {
		return true
	}
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) != "" {
			return false
		}
	}
	return true
}

func recordsContainLiveUsage(matches []projectRecordState) bool {
	for _, match := range matches {
		if match.Usage.DownloadCount > 0 || match.Usage.UploadCount > 0 || match.Usage.LastAccessTime != nil || match.Usage.LastDownloadTime != nil || match.Usage.LastUploadTime != nil {
			return true
		}
	}
	return false
}

func classifyStorageFinding(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) storageFindingKind {
	if recordHasBrokenAccess(record) {
		return storageFindingBrokenAccessURL
	}
	if kind := classifyRawAccessURLFindings(record); kind != storageFindingNone {
		return kind
	}
	bucketMatches := matchedBucketObjectURLs(record, bucketObjectsByURL)
	if inventoryHasValidationMismatch(record, bucketMatches, bucketObjectsByURL) {
		return storageFindingValidationMismatch
	}
	if len(record.AccessProbes) == 0 {
		if len(bucketMatches) > 0 {
			return storageFindingNone
		}
		if len(bucketObjectsByURL) > 0 && hasCanonicalBucketURL(record) {
			return storageFindingProbeError
		}
		return storageFindingNone
	}
	hasPresent := false
	hasMissing := false
	hasBrokenBucketMapping := false
	hasProbeError := false
	hasBucketMatch := len(bucketMatches) > 0
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Status) == "present" {
			hasPresent = true
		}
		switch strings.TrimSpace(probe.ErrorKind) {
		case "credential_missing":
			hasBrokenBucketMapping = true
		}
		switch strings.TrimSpace(probe.Status) {
		case "not_found":
			hasMissing = true
		case "forbidden", "unsupported", "invalid", "error":
			hasProbeError = true
		}
		if strings.TrimSpace(probe.ValidationStatus) == "mismatched" {
			return storageFindingValidationMismatch
		}
	}
	if hasBucketMatch || hasPresent {
		return storageFindingNone
	}
	if hasBrokenBucketMapping {
		return storageFindingBrokenBucketMap
	}
	if hasMissing {
		return storageFindingObjectMissing
	}
	if hasProbeError {
		return storageFindingProbeError
	}
	return storageFindingNone
}

func inventoryHasValidationMismatch(record projectRecordState, bucketObjectURLs []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	if len(bucketObjectURLs) == 0 {
		return false
	}
	checksum := normalizeAnalyticsChecksum(record.Checksum)
	for _, objectURL := range bucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if record.Size > 0 && item.SizeBytes > 0 && item.SizeBytes != record.Size {
			return true
		}
		if checksum != "" {
			metaSHA := normalizeAnalyticsChecksum(item.MetaSHA256)
			if metaSHA != "" && metaSHA != checksum {
				return true
			}
		}
	}
	return false
}

func accessProbesForRecord(record projectRecordState) []GitStorageCleanupAccessProbe {
	if len(record.AccessProbes) > 0 {
		probes := make([]GitStorageCleanupAccessProbe, 0, len(record.AccessProbes))
		for _, probe := range record.AccessProbes {
			exists := probe.Exists
			probes = append(probes, GitStorageCleanupAccessProbe{
				URL:                  probe.ObjectURL,
				Provider:             probe.Provider,
				Bucket:               probe.Bucket,
				Key:                  probe.Key,
				Path:                 probe.Path,
				Exists:               &exists,
				Status:               probe.Status,
				Error:                probe.Error,
				ErrorKind:            probe.ErrorKind,
				SizeBytes:            probe.SizeBytes,
				MetaSHA256:           probe.MetaSHA256,
				ETag:                 probe.ETag,
				LastModified:         probe.LastModified,
				ValidationStatus:     probe.ValidationStatus,
				SizeMatch:            probe.SizeMatch,
				SHA256Match:          probe.SHA256Match,
				ValidationMismatches: append([]string(nil), probe.ValidationMismatches...),
			})
		}
		return probes
	}
	if len(record.AccessURLs) == 0 {
		return []GitStorageCleanupAccessProbe{{
			URL:       "",
			Status:    "missing",
			Error:     "no access URLs present",
			ErrorKind: "missing_access_url",
		}}
	}
	probes := make([]GitStorageCleanupAccessProbe, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) == "" {
			probes = append(probes, GitStorageCleanupAccessProbe{
				URL:       accessURL,
				Status:    "missing",
				Error:     "blank access URL",
				ErrorKind: "missing_access_url",
			})
			continue
		}
		probes = append(probes, GitStorageCleanupAccessProbe{
			URL:    accessURL,
			Status: "present",
		})
	}
	return probes
}

func accessStatusForRecord(record projectRecordState) string {
	switch classifyStorageFinding(record, nil) {
	case storageFindingBrokenAccessURL, storageFindingObjectMissing:
		return "missing"
	case storageFindingBrokenBucketMap, storageFindingProbeError:
		return "error"
	case storageFindingValidationMismatch:
		return "mismatched"
	}
	return "present"
}

func accessErrorForRecord(record projectRecordState) string {
	switch classifyStorageFinding(record, nil) {
	case storageFindingBrokenAccessURL:
		return "no usable access URL present"
	case storageFindingBrokenBucketMap:
		return "no Syfon bucket mapping is configured for this access URL"
	case storageFindingObjectMissing:
		return "storage object not found"
	case storageFindingValidationMismatch:
		return "storage metadata does not match the Syfon record"
	case storageFindingProbeError:
		return "storage probe failed"
	}
	return ""
}

func buildBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) GitStorageCleanupFinding {
	objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	return GitStorageCleanupFinding{
		Kind:                "bucket_only_object",
		NormalizedPath:      objectURL,
		ObjectIDs:           []string{},
		Records:             []GitStorageCleanupRecordAudit{},
		RecommendedAction:   "Review and delete bucket object that has no Syfon record",
		RepoDeleteCandidate: false,
		CleanupScope:        "bucket_object",
		SizeBytes:           item.SizeBytes,
		LastUpdated:         strings.TrimSpace(item.LastModified),
		Evidence: &GitAuditEvidence{
			AccessURLs:       []string{objectURL},
			Buckets:          uniqueStrings([]string{item.Bucket}),
			Keys:             uniqueStrings([]string{item.Key}),
			ProbeStatuses:    []string{"enumerated"},
			BucketEvaluation: "enumerated",
		},
	}
}

func buildChainBucketOnlyFinding(item gintegrationsyfon.ProjectBucketObject) GitStorageChainFinding {
	objectURL := canonicalStorageURL(item.Bucket, item.Key, item.ObjectURL)
	return GitStorageChainFinding{
		Kind:              "bucket_only_object",
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
		Evidence: &GitAuditEvidence{
			AccessURLs:       []string{objectURL},
			BucketObjectURLs: []string{objectURL},
			Buckets:          uniqueStrings([]string{item.Bucket}),
			Keys:             uniqueStrings([]string{item.Key}),
			ProbeStatuses:    []string{"enumerated"},
			BucketEvaluation: "enumerated",
		},
	}
}

func buildChainRecordFindings(kind string, record projectRecordState, gitPaths []string, bucketObjectURLs []string, action string) []GitStorageChainFinding {
	paths := uniqueStrings(gitPaths)
	if len(paths) == 0 {
		displayPath := orphanDisplayPath(strings.TrimSpace(record.Checksum), append(bucketObjectURLs, record.AccessURLs...))
		if displayPath == "" {
			displayPath = strings.TrimSpace(record.Checksum)
		}
		paths = []string{displayPath}
	}
	objectIDs := uniqueStrings([]string{record.ObjectID})
	accessURLs := uniqueStrings(record.AccessURLs)
	evidence := buildFindingEvidence(strings.TrimSpace(record.Checksum), gitPaths, []projectRecordState{record}, "enumerated_and_probed")
	if evidence != nil {
		evidence.BucketObjectURLs = uniqueStrings(append(evidence.BucketObjectURLs, bucketObjectURLs...))
	}
	primaryProbe := selectChainProbe(record, bucketObjectURLs)
	findings := make([]GitStorageChainFinding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, GitStorageChainFinding{
			Kind:              kind,
			NormalizedPath:    path,
			Checksum:          strings.TrimSpace(record.Checksum),
			SourcePaths:       uniqueStrings(gitPaths),
			ObjectIDs:         objectIDs,
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
			Evidence:          evidence,
		})
	}
	return findings
}

type chainProbeSelection struct {
	bucketObjectURL string
	probe           GitStorageCleanupAccessProbe
}

func selectChainProbe(record projectRecordState, bucketObjectURLs []string) chainProbeSelection {
	probes := accessProbesForRecord(record)
	if len(bucketObjectURLs) > 0 {
		targets := map[string]struct{}{}
		for _, bucketURL := range bucketObjectURLs {
			targets[strings.TrimSpace(bucketURL)] = struct{}{}
		}
		for _, probe := range probes {
			objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if _, ok := targets[objectURL]; ok {
				return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
			}
		}
	}
	for _, probe := range probes {
		objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if probe.Status != "" || probe.ErrorKind != "" || objectURL != "" || probe.URL != "" {
			return chainProbeSelection{bucketObjectURL: objectURL, probe: probe}
		}
	}
	return chainProbeSelection{}
}

func chainFindingError(kind string, record projectRecordState, probe GitStorageCleanupAccessProbe) string {
	if trimmed := strings.TrimSpace(probe.Error); trimmed != "" {
		return trimmed
	}
	switch kind {
	case "syfon_broken_bucket_mapping":
		return "no configured Syfon bucket mapping matched this access URL"
	case "syfon_missing_bucket_object", "syfon_git_no_bucket":
		return "mapped bucket object was not found"
	case "git_syfon_metadata_mismatch":
		return "bucket metadata did not match the Syfon record"
	case "probe_error":
		return accessErrorForRecord(record)
	default:
		return ""
	}
}

func recordsReferenceBucketObject(matches []projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if len(matchedBucketObjectURLs(match, bucketObjectsByURL)) > 0 {
			return true
		}
	}
	return false
}

func matchedBucketObjectURLs(record projectRecordState, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	if len(bucketObjectsByURL) == 0 {
		return nil
	}
	matches := make([]string, 0)
	for _, objectURL := range recordBucketURLs(record) {
		if _, ok := bucketObjectsByURL[objectURL]; ok {
			matches = append(matches, objectURL)
		}
	}
	return uniqueStrings(matches)
}

func hasCanonicalBucketURL(record projectRecordState) bool {
	return len(recordBucketURLs(record)) > 0
}

func recordBucketURLs(record projectRecordState) []string {
	out := make([]string, 0)
	for _, probe := range record.AccessProbes {
		if objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	for _, accessURL := range accessURLsForStorage(record) {
		if objectURL := canonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	return uniqueStrings(out)
}

func accessURLsForStorage(record projectRecordState) []string {
	if len(record.CanonicalAccessURLs) > 0 {
		return record.CanonicalAccessURLs
	}
	return record.AccessURLs
}

func probeAccessURLsForRecord(record projectRecordState) []string {
	return uniqueStrings(append(rawAccessURLsForRecord(record), accessURLsForStorage(record)...))
}

func rawAccessURLsForRecord(record projectRecordState) []string {
	out := make([]string, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return uniqueStrings(out)
}

func brokenBucketMappingRepairPlan(records []projectRecordState) ([]string, map[string][]gintegrationsyfon.ProjectAccessMethod) {
	deleteIDs := make([]string, 0)
	updateAccessMethods := make(map[string][]gintegrationsyfon.ProjectAccessMethod)
	for _, record := range records {
		remainingMethods, shouldDelete, ok := repairBrokenBucketMappingRecord(record)
		if !ok {
			continue
		}
		if shouldDelete {
			deleteIDs = append(deleteIDs, record.ObjectID)
			continue
		}
		updateAccessMethods[record.ObjectID] = remainingMethods
	}
	return uniqueStrings(deleteIDs), updateAccessMethods
}

func repairBrokenBucketMappingRecord(record projectRecordState) ([]gintegrationsyfon.ProjectAccessMethod, bool, bool) {
	if len(record.AccessMethods) == 0 || len(record.AccessProbes) == 0 {
		return nil, false, false
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probesByURL[strings.TrimSpace(probe.ObjectURL)] = append(probesByURL[strings.TrimSpace(probe.ObjectURL)], probe)
	}
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(record.AccessMethods))
	removedAny := false
	for _, method := range record.AccessMethods {
		if !accessURLHasBrokenBucketMapping(method.URL, probesByURL) {
			remaining = append(remaining, method)
			continue
		}
		removedAny = true
	}
	if !removedAny {
		return nil, false, false
	}
	if len(remaining) == 0 {
		return nil, true, true
	}
	return remaining, false, true
}

func accessURLHasBrokenBucketMapping(accessURL string, probesByURL map[string][]gintegrationsyfon.BulkStorageProbeResult) bool {
	probes := probesByURL[strings.TrimSpace(accessURL)]
	if len(probes) == 0 {
		return false
	}
	hasBrokenBucketMapping := false
	for _, probe := range probes {
		if strings.TrimSpace(probe.Status) == "present" {
			return false
		}
		if strings.TrimSpace(probe.ErrorKind) == "credential_missing" {
			hasBrokenBucketMapping = true
		}
	}
	return hasBrokenBucketMapping
}

func classifyRawAccessURLFindings(record projectRecordState) storageFindingKind {
	if len(record.AccessProbes) == 0 {
		return storageFindingNone
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probesByURL[strings.TrimSpace(probe.ObjectURL)] = append(probesByURL[strings.TrimSpace(probe.ObjectURL)], probe)
	}
	for _, accessURL := range rawAccessURLsForRecord(record) {
		probes := probesByURL[accessURL]
		if len(probes) == 0 {
			continue
		}
		hasPresent := false
		hasMissing := false
		hasBrokenBucketMapping := false
		hasProbeError := false
		for _, probe := range probes {
			if strings.TrimSpace(probe.ValidationStatus) == "mismatched" {
				return storageFindingValidationMismatch
			}
			if strings.TrimSpace(probe.Status) == "present" {
				hasPresent = true
			}
			switch strings.TrimSpace(probe.ErrorKind) {
			case "credential_missing":
				hasBrokenBucketMapping = true
			}
			switch strings.TrimSpace(probe.Status) {
			case "not_found":
				hasMissing = true
			case "forbidden", "unsupported", "invalid", "error":
				hasProbeError = true
			}
		}
		if hasPresent {
			continue
		}
		switch {
		case hasBrokenBucketMapping:
			return storageFindingBrokenBucketMap
		case hasMissing:
			return storageFindingObjectMissing
		case hasProbeError:
			return storageFindingProbeError
		}
	}
	return storageFindingNone
}

func canonicalizeRecordAccessURLs(accessURLs []string, scopes []domain.StorageBucketScope) []string {
	out := make([]string, 0, len(accessURLs))
	for _, accessURL := range accessURLs {
		if objectURL := canonicalizeScopedStorageURL(accessURL, scopes); objectURL != "" {
			out = append(out, objectURL)
			continue
		}
		if objectURL := canonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	return uniqueStrings(out)
}

func canonicalizeScopedStorageURL(accessURL string, scopes []domain.StorageBucketScope) string {
	if len(scopes) == 0 {
		return ""
	}
	_, key, ok := parseStorageURL(accessURL)
	if !ok {
		return ""
	}
	targetBucket := ""
	prefixes := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		bucket, prefix, ok := parseStorageScopePath(scope.Path)
		if !ok {
			continue
		}
		if strings.TrimSpace(bucket) != "" {
			targetBucket = strings.TrimSpace(bucket)
		}
		if strings.TrimSpace(prefix) != "" {
			prefixes = append(prefixes, strings.Trim(strings.TrimSpace(prefix), "/"))
		}
	}
	if targetBucket == "" {
		return ""
	}
	normalizedKey := normalizeScopedStorageKeyForGecko(key, prefixes)
	if normalizedKey == "" {
		return ""
	}
	return canonicalStorageURL(targetBucket, normalizedKey, "")
}

func parseStorageScopePath(raw string) (string, string, bool) {
	return parseStorageURL(raw)
}

func parseStorageURL(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	rest = strings.TrimLeft(rest, "/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	bucket := strings.TrimSpace(parts[0])
	key := strings.Trim(strings.TrimSpace(parts[1]), "/")
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}

func normalizeScopedStorageKeyForGecko(key string, prefixes []string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	normalizedPrefixes := normalizedScopePrefixesForGecko(prefixes)
	remainder := key
	for _, prefix := range normalizedPrefixes {
		remainder = trimLeadingStoragePrefixForGecko(remainder, prefix)
	}
	composedPrefix := strings.Join(normalizedPrefixes, "/")
	switch {
	case composedPrefix == "":
		return remainder
	case remainder == "":
		return composedPrefix
	default:
		return path.Join(composedPrefix, remainder)
	}
}

func normalizedScopePrefixesForGecko(prefixes []string) []string {
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if prefix == "" {
			continue
		}
		if len(out) == 0 {
			out = append(out, prefix)
			continue
		}
		last := out[len(out)-1]
		switch {
		case prefix == last:
			continue
		case strings.HasPrefix(prefix, last+"/"):
			out[len(out)-1] = prefix
		case strings.HasPrefix(last, prefix+"/"):
			continue
		default:
			out = append(out, prefix)
		}
	}
	return out
}

func trimLeadingStoragePrefixForGecko(key, prefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if key == "" || prefix == "" {
		return key
	}
	if key == prefix {
		return ""
	}
	if strings.HasPrefix(key, prefix+"/") {
		return strings.TrimPrefix(key, prefix+"/")
	}
	return key
}

func canonicalStorageURL(bucket string, key string, objectURL string) string {
	cleanBucket := strings.TrimSpace(bucket)
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	if cleanBucket != "" && cleanKey != "" {
		return "s3://" + cleanBucket + "/" + cleanKey
	}
	trimmed := strings.TrimSpace(objectURL)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return ""
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	if strings.HasPrefix(rest, "/") {
		rest = strings.TrimLeft(rest, "/")
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	return "s3://" + strings.TrimSpace(parts[0]) + "/" + strings.Trim(strings.TrimSpace(parts[1]), "/")
}

func normalizeAnalyticsChecksum(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "sha256:")
	trimmed = strings.TrimPrefix(trimmed, "SHA256:")
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

func applyUsage(agg *storageAggregate, record projectRecordState) {
	agg.downloadCount += record.Usage.DownloadCount
	agg.lastDownload = laterTime(agg.lastDownload, record.Usage.LastDownloadTime)
	agg.latestUpdate = laterTime(agg.latestUpdate, record.UpdatedAt)
}

func aggregateMatchedSize(matches []projectRecordState, fallback int64) int64 {
	var total int64
	for _, match := range matches {
		if match.Size > 0 {
			total += match.Size
		}
	}
	if total == 0 {
		return fallback
	}
	return total
}

func aggregateMatchedDownloads(matches []projectRecordState) int64 {
	var total int64
	for _, match := range matches {
		total += match.Usage.DownloadCount
	}
	return total
}

func latestMatchedDownload(matches []projectRecordState) *time.Time {
	var latest *time.Time
	for _, match := range matches {
		latest = laterTime(latest, match.Usage.LastDownloadTime)
	}
	return latest
}

func laterTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		copyTime := candidate.UTC()
		return &copyTime
	}
	return current
}

func immediateChild(root string, repoPath string) (string, string, string) {
	normalizedPath := normalizeRepoSubpath(repoPath)
	if root != "" {
		prefix := root + "/"
		if !strings.HasPrefix(normalizedPath, prefix) {
			return "", "", ""
		}
		normalizedPath = strings.TrimPrefix(normalizedPath, prefix)
	}
	if normalizedPath == "" {
		return "", "", ""
	}
	parts := strings.Split(normalizedPath, "/")
	childName := parts[0]
	if len(parts) == 1 {
		if root == "" {
			return childName, childName, "file"
		}
		return childName, root + "/" + childName, "file"
	}
	if root == "" {
		return childName, childName, "directory"
	}
	return childName, root + "/" + childName, "directory"
}

func normalizeRepoSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func recordObjectIDs(matches []projectRecordState) []string {
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		if trimmed := strings.TrimSpace(match.ObjectID); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return uniqueStrings(out)
}

func buildFindingEvidence(checksum string, sourcePaths []string, matches []projectRecordState, bucketEvaluation string) *GitAuditEvidence {
	evidence := &GitAuditEvidence{
		Checksum:         strings.TrimSpace(checksum),
		SourcePaths:      uniqueStrings(sourcePaths),
		ObjectIDs:        []string{},
		AccessURLs:       []string{},
		BucketObjectURLs: []string{},
		Buckets:          []string{},
		Keys:             []string{},
		ProbeStatuses:    []string{},
		ValidationStates: []string{},
		ErrorKinds:       []string{},
		Errors:           []string{},
		BucketEvaluation: strings.TrimSpace(bucketEvaluation),
	}
	for _, match := range matches {
		if objectID := strings.TrimSpace(match.ObjectID); objectID != "" {
			evidence.ObjectIDs = append(evidence.ObjectIDs, objectID)
		}
		evidence.AccessURLs = append(evidence.AccessURLs, match.AccessURLs...)
		if len(match.AccessProbes) == 0 {
			continue
		}
		for _, probe := range match.AccessProbes {
			if objectURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
				evidence.BucketObjectURLs = append(evidence.BucketObjectURLs, objectURL)
			}
			if bucket := strings.TrimSpace(probe.Bucket); bucket != "" {
				evidence.Buckets = append(evidence.Buckets, bucket)
			}
			if key := strings.TrimSpace(probe.Key); key != "" {
				evidence.Keys = append(evidence.Keys, key)
			}
			if status := strings.TrimSpace(probe.Status); status != "" {
				evidence.ProbeStatuses = append(evidence.ProbeStatuses, status)
			}
			if validation := strings.TrimSpace(probe.ValidationStatus); validation != "" {
				evidence.ValidationStates = append(evidence.ValidationStates, validation)
			}
			if kind := strings.TrimSpace(probe.ErrorKind); kind != "" {
				evidence.ErrorKinds = append(evidence.ErrorKinds, kind)
			}
			if err := strings.TrimSpace(probe.Error); err != "" {
				evidence.Errors = append(evidence.Errors, err)
			}
		}
	}
	evidence.ObjectIDs = uniqueStrings(evidence.ObjectIDs)
	evidence.AccessURLs = uniqueStrings(evidence.AccessURLs)
	evidence.BucketObjectURLs = uniqueStrings(evidence.BucketObjectURLs)
	evidence.Buckets = uniqueStrings(evidence.Buckets)
	evidence.Keys = uniqueStrings(evidence.Keys)
	evidence.ProbeStatuses = uniqueStrings(evidence.ProbeStatuses)
	evidence.ValidationStates = uniqueStrings(evidence.ValidationStates)
	evidence.ErrorKinds = uniqueStrings(evidence.ErrorKinds)
	evidence.Errors = uniqueStrings(evidence.Errors)
	if evidence.Checksum == "" &&
		len(evidence.SourcePaths) == 0 &&
		len(evidence.ObjectIDs) == 0 &&
		len(evidence.AccessURLs) == 0 &&
		len(evidence.BucketObjectURLs) == 0 &&
		len(evidence.Buckets) == 0 &&
		len(evidence.Keys) == 0 &&
		len(evidence.ProbeStatuses) == 0 &&
		len(evidence.ValidationStates) == 0 &&
		len(evidence.ErrorKinds) == 0 &&
		len(evidence.Errors) == 0 &&
		evidence.BucketEvaluation == "" {
		return nil
	}
	return evidence
}

func flattenRecordStates(recordsByChecksum map[string][]projectRecordState) []projectRecordState {
	out := make([]projectRecordState, 0)
	seen := map[string]struct{}{}
	for _, group := range recordsByChecksum {
		for _, record := range group {
			objectID := strings.TrimSpace(record.ObjectID)
			if objectID == "" {
				continue
			}
			if _, ok := seen[objectID]; ok {
				continue
			}
			seen[objectID] = struct{}{}
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObjectID < out[j].ObjectID
	})
	return out
}

func chainPathCount(gitPaths []string) int {
	if len(gitPaths) == 0 {
		return 1
	}
	return len(gitPaths)
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func storageProbeRequestKey(objectURL string, size int64, checksum string) string {
	return strings.TrimSpace(objectURL) + "|" + fmt.Sprintf("%d", size) + "|" + strings.TrimSpace(checksum)
}

func sortStorageAggregates(items []storageAggregate, sortBy string, sortOrder string) {
	desc := !strings.EqualFold(strings.TrimSpace(sortOrder), "asc")
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		sort.Slice(items, func(i, j int) bool {
			left := strings.ToLower(items[i].name)
			right := strings.ToLower(items[j].name)
			if desc {
				return left > right
			}
			return left < right
		})
	default:
		sort.Slice(items, func(i, j int) bool {
			if items[i].totalBytes != items[j].totalBytes {
				if desc {
					return items[i].totalBytes > items[j].totalBytes
				}
				return items[i].totalBytes < items[j].totalBytes
			}
			if items[i].rowType != items[j].rowType {
				return items[i].rowType == "directory"
			}
			return strings.ToLower(items[i].name) < strings.ToLower(items[j].name)
		})
	}
}

func orphanDisplayPath(checksum string, sourcePaths []string) string {
	if len(sourcePaths) > 0 && strings.TrimSpace(sourcePaths[0]) != "" {
		return strings.TrimSpace(sourcePaths[0])
	}
	return "sha256/" + strings.TrimSpace(checksum)
}

func recordSourcePaths(matches []projectRecordState) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, match := range matches {
		for _, accessURL := range match.AccessURLs {
			normalized := strings.TrimSpace(accessURL)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			out = append(out, normalized)
		}
	}
	sort.Strings(out)
	return out
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func boolPtr(value bool) *bool {
	return &value
}

func differenceStrings(values []string, toRemove []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	removeSet := make(map[string]struct{}, len(toRemove))
	for _, value := range toRemove {
		removeSet[value] = struct{}{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}
