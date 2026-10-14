package git

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

const cleanupInactiveDays = 30
const projectJoinCacheTTL = 45 * time.Second
const chainInputCacheTTL = 45 * time.Second
const chainProjectRecordCacheMaxAge = 30 * time.Minute
const projectFileUsageBulkChunkSize = 5000
const storageChainValidationDebugSampleLimit = 20
const StorageFolderSummaryModeExact = "exact"
const StorageFolderSummarySourceGitIndex = "git_index"
const StorageFolderSummarySourceExactJoin = "exact_join"

const (
	storageActionabilityAutoRepair   = "auto_repair"
	storageActionabilityManualChoice = "manual_choice"
	storageActionabilityInspectOnly  = "inspect_only"

	storageActionRemoveBrokenAccessURLs = "remove_broken_access_urls"
	storageActionDeleteSyfonRecord      = "delete_syfon_record"
	storageActionDeleteBucketObject     = "delete_bucket_object"
	storageActionDeleteBoth             = "delete_both"
	storageActionInspectEvidence        = "inspect_evidence"
	storageActionCreateSyfonRecord      = "create_syfon_record"
)

type storageRepairPolicy struct {
	actionability  string
	actions        []string
	defaultAction  string
	supportsDryRun bool
}

func storageRepairPolicyForKind(kind string) storageRepairPolicy {
	switch strings.TrimSpace(kind) {
	case "bucket_only_object":
		return autoRepairPolicy(storageActionDeleteBucketObject, storageActionInspectEvidence)
	case "bucket_syfon_no_git":
		// Absence from Git is not sufficient to delete storage automatically, but
		// a user may explicitly remove a verified Bucket + Syfon orphan.
		return storageRepairPolicy{
			actionability: storageActionabilityManualChoice,
			actions: []string{
				storageActionDeleteBoth,
				storageActionDeleteSyfonRecord,
				storageActionDeleteBucketObject,
				storageActionInspectEvidence,
			},
			defaultAction:  storageActionDeleteBoth,
			supportsDryRun: true,
		}
	case "repo_orphan_live_object":
		return autoRepairPolicy(storageActionDeleteBoth, storageActionDeleteSyfonRecord, storageActionDeleteBucketObject, storageActionInspectEvidence)
	case "repo_orphan_stale_record", "stale_duplicate_record", "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
		return autoRepairPolicy(storageActionDeleteSyfonRecord, storageActionInspectEvidence)
	case "broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping":
		return autoRepairPolicy(storageActionRemoveBrokenAccessURLs, storageActionDeleteSyfonRecord, storageActionInspectEvidence)
	case "storage_validation_mismatch":
		return storageRepairPolicy{
			actionability: storageActionabilityManualChoice,
			actions: []string{
				storageActionRemoveBrokenAccessURLs,
				storageActionDeleteSyfonRecord,
				storageActionDeleteBucketObject,
				storageActionDeleteBoth,
				storageActionInspectEvidence,
			},
			supportsDryRun: true,
		}
	case "git_syfon_metadata_mismatch":
		// A mismatch cannot establish which of Git, Syfon, or the bucket is
		// authoritative. Requiring an explicit out-of-band reconciliation keeps
		// a stale checksum from becoming a destructive cleanup candidate.
		return inspectOnlyStorageRepairPolicy()
	case "probe_error":
		// Probe errors do not prove that the object is absent, but the audit user
		// may explicitly discard the affected Syfon records after reviewing them.
		return storageRepairPolicy{
			actionability: storageActionabilityManualChoice,
			actions: []string{
				storageActionDeleteSyfonRecord,
				storageActionInspectEvidence,
			},
			defaultAction:  storageActionDeleteSyfonRecord,
			supportsDryRun: true,
		}
	default:
		return inspectOnlyStorageRepairPolicy()
	}
}

func inspectOnlyStorageRepairPolicy() storageRepairPolicy {
	return storageRepairPolicy{
		actionability:  storageActionabilityInspectOnly,
		actions:        []string{storageActionInspectEvidence},
		defaultAction:  storageActionInspectEvidence,
		supportsDryRun: false,
	}
}

func autoRepairPolicy(defaultAction string, extraActions ...string) storageRepairPolicy {
	actions := append([]string{defaultAction}, extraActions...)
	return storageRepairPolicy{
		actionability:  storageActionabilityAutoRepair,
		actions:        uniqueStrings(actions),
		defaultAction:  defaultAction,
		supportsDryRun: true,
	}
}

type storageAnalyticsBackend interface {
	ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error)
	ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error)
	ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error)
	GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error)
	ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
	ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
	ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
	ListProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*gintegrationsyfon.ProjectBucketSummary, error)
	BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkListStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error
	DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error)
	BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error
	RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error)
}

type StorageAnalyticsService struct {
	storage                 storageAnalyticsBackend
	projectJoinMu           sync.RWMutex
	projectJoinCache        map[string]cachedProjectJoinState
	projectJoinWork         map[string]*inflightProjectJoinState
	chainInputMu            sync.RWMutex
	chainInputCache         map[string]cachedChainInputState
	projectAuditCache       map[string]cachedProjectAuditRecordState
	projectAuditWork        map[string]*inflightProjectAuditRecordState
	chainAuditRefreshMu     sync.Mutex
	chainAuditRefreshWork   map[string]*inflightStorageChainAuditRefresh
	chainAuditResponseCache storageChainAuditResponseCache
	exactProjectJoinCache   storageExactProjectJoinCache
	projectBucketCache      projectBucketInventoryCache
}

type StorageFolderTimings struct {
	DebugPrefix string
	Logf        func(format string, args ...any)
}

func (timings *StorageFolderTimings) Record(stage string, duration time.Duration) {
	if timings == nil || timings.Logf == nil {
		return
	}
	timings.Logf("storage_folder_stage %s stage=%s duration_ms=%d", timings.DebugPrefix, stage, duration.Milliseconds())
}

func NewStorageAnalyticsService(storage storageAnalyticsBackend) *StorageAnalyticsService {
	if storage == nil {
		return nil
	}
	return &StorageAnalyticsService{
		storage:               storage,
		projectJoinCache:      map[string]cachedProjectJoinState{},
		projectJoinWork:       map[string]*inflightProjectJoinState{},
		chainInputCache:       map[string]cachedChainInputState{},
		projectAuditCache:     map[string]cachedProjectAuditRecordState{},
		projectAuditWork:      map[string]*inflightProjectAuditRecordState{},
		chainAuditRefreshWork: map[string]*inflightStorageChainAuditRefresh{},
	}
}

func (service *StorageAnalyticsService) EnableStorageChainAuditResponseCacheFromEnv() {
	if service == nil {
		return
	}
	service.chainAuditResponseCache = NewStorageChainAuditResponseCacheFromEnv()
	service.exactProjectJoinCache = NewStorageExactProjectJoinCacheFromEnv()
	service.projectBucketCache = NewProjectBucketInventoryCacheFromEnv()
}

type RepoInventoryFile struct {
	RepoPath string
	Name     string
	Checksum string
	Size     int64
}

type projectRecordState struct {
	gintegrationsyfon.ProjectRecord
	CanonicalAccessURLs     []string
	CanonicalAccessURLByRaw map[string]string
	Usage                   gintegrationsyfon.FileUsage
	AccessProbes            []gintegrationsyfon.BulkStorageProbeResult
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

type storageCleanupApplyPlan struct {
	DeleteObjectIDs        []string
	DeleteStorageObjectIDs []string
	DeleteBucketObjects    []string
	UpdateAccessMethods    map[string][]gintegrationsyfon.ProjectAccessMethod
	UpdatedRecordIDs       []string
	RepoDeletePaths        []string
	ManualPaths            []string
	SkippedPaths           []string
	Verifications          []storageCleanupVerification
	AccessMethodChanges    []storageaudit.AccessMethodChange
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

type inflightProjectJoinState struct {
	done              chan struct{}
	recordsByChecksum map[string][]projectRecordState
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
	err               error
}

type cachedChainInputState struct {
	expiresAt          time.Time
	projectRecords     []gintegrationsyfon.ProjectRecord
	projectScopes      []domain.StorageBucketScope
	bucketSummary      *gintegrationsyfon.ProjectBucketSummary
	bucketObjects      []gintegrationsyfon.ProjectBucketObject
	bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
	bucketMetadata     projectBucketInventoryMetadata
}

type cachedProjectAuditRecordState struct {
	records   []gintegrationsyfon.ProjectRecord
	validator projectAuditRecordValidator
	cachedAt  time.Time
}

type inflightProjectAuditRecordState struct {
	done      chan struct{}
	records   []gintegrationsyfon.ProjectRecord
	validator projectAuditRecordValidator
	err       error
}

type projectAuditRecordValidator struct {
	RecordCount             int
	RecordLatestUpdatedTime string
	RecordRevision          string
}

func (service *StorageAnalyticsService) BuildStorageSummary(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*GitStorageSummaryResponse, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
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

func (service *StorageAnalyticsService) BuildStorageChildren(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string) (*GitStorageChildrenResponse, error) {
	index, err := loadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, err
	}
	directory, err := repoDirectoryAggregate(index, gitSubpath)
	if err != nil {
		return nil, err
	}
	aggregates := cloneDirectoryChildren(directory.Children)
	sortStorageAggregates(aggregates, sortBy, sortOrder)
	page, err := storageChildrenPageForRequest(aggregates, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
	if err != nil {
		return nil, err
	}
	inventory := filterInventoryForStorageChildren(index.sidecar.Files, page.items)
	enriched, err := service.enrichStorageChildrenPage(ctx, authorizationHeader, organization, project, gitSubpath, inventory, page.items)
	if err != nil {
		return nil, err
	}
	return &GitStorageChildrenResponse{
		Items:      storageChildrenItemsFromAggregates(enriched),
		HasMore:    page.hasMore,
		NextCursor: page.nextCursor,
	}, nil
}

func (service *StorageAnalyticsService) BuildStorageFolder(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string, summaryMode string, forceRefresh bool, timings *StorageFolderTimings) (*GitStorageFolderResponse, error) {
	if strings.EqualFold(strings.TrimSpace(summaryMode), StorageFolderSummaryModeExact) {
		exactStart := time.Now()
		index, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, forceRefresh)
		timings.Record("exact_join", time.Since(exactStart))
		if err != nil {
			return nil, err
		}
		directoryStart := time.Now()
		directory, err := repoDirectoryAggregate(index, gitSubpath)
		timings.Record("directory_aggregate", time.Since(directoryStart))
		if err != nil {
			return nil, err
		}
		summaryAgg := summarizeSubtree(gitSubpath, inventory, recordsByChecksum, usageByObjectID, directory.DirectChildCount)
		pageStart := time.Now()
		aggregates := cloneDirectoryChildren(directory.Children)
		sortStorageAggregates(aggregates, sortBy, sortOrder)
		page, err := storageChildrenPageForRequest(aggregates, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
		timings.Record("child_pagination", time.Since(pageStart))
		if err != nil {
			return nil, err
		}
		enrichStart := time.Now()
		pageInventory := filterInventoryForStorageChildren(inventory, page.items)
		enriched := aggregateImmediateChildren(gitSubpath, pageInventory, recordsByChecksum, usageByObjectID, page.items)
		timings.Record("enrich_children_page", time.Since(enrichStart))
		return &GitStorageFolderResponse{
			Summary: GitStorageSummaryResponse{
				Path:               summaryAgg.path,
				Source:             StorageFolderSummarySourceExactJoin,
				FileCount:          summaryAgg.fileCount,
				RecordCount:        summaryAgg.recordCount,
				DirectChildCount:   directory.DirectChildCount,
				TotalBytes:         summaryAgg.totalBytes,
				DownloadCount:      summaryAgg.downloadCount,
				LastDownloadTime:   formatOptionalTime(summaryAgg.lastDownload),
				LatestUpdateTime:   formatOptionalTime(summaryAgg.latestUpdate),
				DuplicatePathCount: summaryAgg.duplicateCount,
			},
			Children: GitStorageChildrenResponse{
				Items:      storageChildrenItemsFromAggregates(enriched),
				HasMore:    page.hasMore,
				NextCursor: page.nextCursor,
			},
		}, nil
	}

	indexStart := time.Now()
	index, err := loadOrBuildRepoAnalyticsIndexWithTimings(ctx, mirrorPath, ref, repo, hash, timings)
	timings.Record("load_repo_index", time.Since(indexStart))
	if err != nil {
		return nil, err
	}
	directoryStart := time.Now()
	directory, err := repoDirectoryServingIndex(index, gitSubpath)
	timings.Record("directory_aggregate", time.Since(directoryStart))
	if err != nil {
		return nil, err
	}
	pageStart := time.Now()
	children, err := storageChildrenResponseForServingIndex(directory, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
	timings.Record("child_pagination", time.Since(pageStart))
	if err != nil {
		return nil, err
	}
	timings.Record("git_index_remote_enrichment", 0)
	normalizedPath := normalizeRepoSubpath(gitSubpath)
	return &GitStorageFolderResponse{
		Summary: GitStorageSummaryResponse{
			Path:             normalizedPath,
			Source:           StorageFolderSummarySourceGitIndex,
			FileCount:        directory.directory.FileCount,
			DirectChildCount: directory.directory.DirectChildCount,
			TotalBytes:       directory.directory.TotalBytes,
		},
		Children: children,
	}, nil
}

func (service *StorageAnalyticsService) BuildProjectDiffAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*GitProjectDiffAuditResponse, error) {
	_, inventory, recordsByChecksum, usageByObjectID, err := service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
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

func (service *StorageAnalyticsService) BuildStorageChainAuditWithOptions(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options StorageChainAuditOptions) (*GitStorageChainAuditResponse, error) {
	normalized, err := normalizeStorageChainAuditOptions(options)
	if err != nil {
		return nil, err
	}
	if service.chainAuditResponseCache != nil && storageChainAuditResponseCacheAllowed(gitSubpath, normalized) {
		return service.buildStorageChainAuditWithResponseCache(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
	}
	if service.chainAuditResponseCache != nil && storageChainAuditRootResponseProjectionAllowed(gitSubpath, normalized) {
		response, ok, err := service.projectStorageChainAuditFromRootResponseCache(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
		if err != nil {
			return nil, err
		}
		if ok {
			return response, nil
		}
	}
	if service.chainAuditResponseCache != nil && normalized.Timings != nil {
		normalized.Timings.Record("audit_response_cache_bypass_non_root", 0)
	}
	return service.buildStorageChainAuditFresh(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, normalized)
}

func storageChainAuditResponseCacheAllowed(gitSubpath string, options StorageChainAuditOptions) bool {
	return normalizeRepoSubpath(gitSubpath) == "" && normalizeRepoSubpath(options.BucketPathPrefix) == ""
}

func storageChainAuditRootResponseProjectionAllowed(gitSubpath string, options StorageChainAuditOptions) bool {
	return normalizeRepoSubpath(gitSubpath) != "" && normalizeRepoSubpath(options.BucketPathPrefix) == "" && !options.ForceAuditRefresh
}

func normalizeStorageChainAuditOptions(options StorageChainAuditOptions) (StorageChainAuditOptions, error) {
	rawProbeMode := strings.TrimSpace(options.ProbeMode)
	probeMode, ok := NormalizeStorageChainProbeMode(options.ProbeMode)
	if !ok {
		return StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain probe mode %q", options.ProbeMode)
	}
	bucketMode, ok := NormalizeStorageChainBucketInventoryMode(options.BucketInventoryMode)
	if !ok {
		return StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain bucket inventory mode %q", options.BucketInventoryMode)
	}
	validationMode := strings.TrimSpace(options.ValidationMode)
	if validationMode == "" {
		if rawProbeMode == "" && bucketMode == StorageChainBucketModeValidate {
			validationMode = StorageChainValidationModeList
		} else {
			validationMode = DefaultStorageChainValidationMode(probeMode, bucketMode)
		}
	}
	validationMode, ok = NormalizeStorageChainValidationMode(validationMode)
	if !ok {
		return StorageChainAuditOptions{}, fmt.Errorf("invalid storage chain validation mode %q", options.ValidationMode)
	}
	options.ProbeMode = probeMode
	options.ValidationMode = validationMode
	options.BucketInventoryMode = bucketMode
	options.BucketPathPrefix = normalizeRepoSubpath(options.BucketPathPrefix)
	options.FindingKind = strings.TrimSpace(options.FindingKind)
	return options, nil
}

func (service *StorageAnalyticsService) buildStorageChainAuditWithResponseCache(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options StorageChainAuditOptions) (*GitStorageChainAuditResponse, error) {
	syfonRevision, err := service.loadStorageAuditSyfonRevision(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	cacheKey := storageChainAuditResponseCacheKey(organization, project, ref, gitSubpath, options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, options.BucketPathPrefix, hash.String(), syfonRevision)
	cache := service.chainAuditResponseCache
	if !options.ForceAuditRefresh {
		start := time.Now()
		cached, ok, err := cache.Get(ctx, cacheKey)
		options.Timings.Record("audit_response_cache_lookup", time.Since(start))
		if err != nil {
			logStorageChainAuditCacheError(options.Timings, cache.Source(), "get", err)
		}
		if ok {
			response := projectStorageChainAuditResponse(cached.Response, options.FindingKind, options.FindingLimit)
			applyStorageChainAuditCacheMetadata(response, true, cached.CachedAt, cached.RefreshDurationMillis, cache.Source(), "")
			options.Timings.Record("audit_response_cache_hit", 0)
			options.Timings.RecordMemory(
				"audit_response_cache_hit",
				"total_findings", response.Summary.TotalFindings,
				"returned_findings", response.Summary.ReturnedFindings,
				"finding_limit", options.FindingLimit,
			)
			return response, nil
		}
		options.Timings.Record("audit_response_cache_miss", 0)
	} else {
		options.Timings.Record("audit_response_cache_force_refresh", 0)
	}

	cached, joinedRefresh, err := service.coalesceStorageChainAuditRefresh(ctx, cacheKey, options.ForceBucketRefresh, func() (cachedStorageChainAuditResponse, error) {
		buildOptions := options
		buildOptions.FindingKind = ""
		buildOptions.FindingLimit = -1
		refreshStart := time.Now()
		response, err := service.buildStorageChainAuditFresh(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, buildOptions)
		if err != nil {
			return cachedStorageChainAuditResponse{}, err
		}
		response.Summary.SyfonRevision = syfonRevision
		value := cachedStorageChainAuditResponse{
			CachedAt:              time.Now(),
			RefreshDurationMillis: time.Since(refreshStart).Milliseconds(),
			Response:              *response,
		}
		cacheStart := time.Now()
		if err := cache.Set(ctx, cacheKey, value, storageChainAuditCacheTTL()); err != nil {
			logStorageChainAuditCacheError(options.Timings, cache.Source(), "set", err)
		}
		options.Timings.Record("audit_response_cache_store", time.Since(cacheStart))
		return value, nil
	})
	if err != nil {
		return nil, err
	}
	if joinedRefresh {
		options.Timings.Record("audit_response_refresh_join", 0)
	}
	projected := projectStorageChainAuditResponse(cached.Response, options.FindingKind, options.FindingLimit)
	source := cache.Source()
	if joinedRefresh {
		source += ":refresh_join"
	}
	applyStorageChainAuditCacheMetadata(projected, false, cached.CachedAt, cached.RefreshDurationMillis, source, "")
	return projected, nil
}

func (service *StorageAnalyticsService) projectStorageChainAuditFromRootResponseCache(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options StorageChainAuditOptions) (*GitStorageChainAuditResponse, bool, error) {
	cache := service.chainAuditResponseCache
	syfonRevision, err := service.loadStorageAuditSyfonRevision(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, false, err
	}
	cacheKey := storageChainAuditResponseCacheKey(organization, project, ref, "", options.ProbeMode, options.ValidationMode, options.BucketInventoryMode, "", hash.String(), syfonRevision)
	start := time.Now()
	cached, ok, err := cache.Get(ctx, cacheKey)
	options.Timings.Record("audit_response_root_cache_lookup", time.Since(start))
	if err != nil {
		logStorageChainAuditCacheError(options.Timings, cache.Source(), "get_root", err)
		return nil, false, nil
	}
	if !ok {
		options.Timings.Record("audit_response_root_cache_miss", 0)
		return nil, false, nil
	}
	inventoryStart := time.Now()
	inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
	options.Timings.Record("audit_response_root_cache_project_inventory", time.Since(inventoryStart))
	if err != nil {
		return nil, false, err
	}
	response := projectStorageChainAuditResponseForSubpath(cached.Response, gitSubpath, inventory, options.FindingKind, options.FindingLimit)
	applyStorageChainAuditCacheMetadata(response, true, cached.CachedAt, cached.RefreshDurationMillis, cache.Source()+":root", "")
	options.Timings.Record("audit_response_root_cache_hit", 0)
	options.Timings.RecordMemory(
		"audit_response_root_cache_hit",
		"git_subpath", normalizeRepoSubpath(gitSubpath),
		"git_files", response.Summary.GitTrackedFileCount,
		"total_findings", response.Summary.TotalFindings,
		"returned_findings", response.Summary.ReturnedFindings,
	)
	return response, true, nil
}

func (service *StorageAnalyticsService) buildStorageChainAuditFresh(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options StorageChainAuditOptions) (*GitStorageChainAuditResponse, error) {
	bucketMode := options.BucketInventoryMode
	validationMode := options.ValidationMode
	bucketPathPrefix := options.BucketPathPrefix
	start := time.Now()
	options.Timings.StageStart("chain_setup_total")
	inputs, err := service.loadStorageChainInputs(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, bucketMode, validationMode, bucketPathPrefix, options.ForceBucketRefresh, options.Timings)
	options.Timings.Record("chain_setup_total", time.Since(start))
	if inputs != nil && inputs.recordSet != nil {
		options.Timings.RecordMemory(
			"chain_setup_total",
			"git_files", len(inputs.inventory),
			"syfon_records", countRecordStates(inputs.recordSet.allProjectRecords),
			"bucket_objects", len(inputs.bucketObjects),
		)
	}
	if err != nil {
		return nil, err
	}
	storageViewStart := time.Now()
	options.Timings.StageStart("storage_view")
	storageView, err := service.buildStorageChainView(ctx, authorizationHeader, organization, project, inputs.recordSet, inputs.inventory, inputs.scopes, inputs.bucketObjects, inputs.bucketObjectsByURL, inputs.bucketInventoryErr, inputs.bucketMetadata, bucketMode, validationMode, options.Timings)
	options.Timings.Record("storage_view", time.Since(storageViewStart))
	if storageView != nil {
		options.Timings.RecordMemory(
			"storage_view",
			"syfon_records", countRecordStates(storageView.allProjectRecords),
			"bucket_objects", len(storageView.bucketObjectsByURL),
		)
	}
	if err != nil {
		return nil, err
	}
	modelStart := time.Now()
	options.Timings.StageStart("model_build")
	includeBucketOrigin := storageView.bucketInventoryAvailable &&
		!storageView.bucketInventoryStale &&
		(bucketMode == StorageChainBucketModeItems || validationMode == StorageChainValidationModeList)
	model := buildStorageChainAuditModel(gitSubpath, inputs.inventory, storageView.recordsByChecksum, storageView.allProjectRecords, storageView.bucketObjectsByURL, inputs.scopes, organization, project, includeBucketOrigin)
	options.Timings.Record("model_build", time.Since(modelStart))
	options.Timings.RecordMemory(
		"model_build",
		"total_findings", len(model.Findings),
		"syfon_records", countRecordStates(storageView.allProjectRecords),
		"bucket_objects", len(storageView.bucketObjectsByURL),
		"git_files", len(inputs.inventory),
	)
	model.Summary.BucketInventoryAvailable = storageView.bucketInventoryAvailable
	model.Summary.BucketInventoryComplete = storageView.bucketInventoryComplete
	model.Summary.BucketInventoryError = storageView.bucketInventoryError
	model.Summary.BucketInventoryWarning = storageView.bucketInventoryWarning
	model.Summary.BucketInventoryObserved = storageView.bucketInventoryObserved
	model.Summary.BucketInventorySource = storageView.bucketInventorySource
	model.Summary.ValidationMode = validationMode
	model.Summary.GitRevision = hash.String()
	model.Summary.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if inputs.bucketSummary != nil {
		exists := inputs.bucketSummary.Exists
		model.Summary.BucketPathExists = &exists
		model.Summary.BucketPathObjectURL = strings.TrimSpace(inputs.bucketSummary.ObjectURL)
		model.Summary.BucketSummaryMode = strings.TrimSpace(inputs.bucketSummary.Mode)
	}
	filteredFindings := filterStorageChainFindingsByKind(model.Findings, options.FindingKind)
	summary := filterStorageChainSummary(model.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, options.FindingLimit, options.FindingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = options.FindingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	options.Timings.RecordMemory(
		"response_shape",
		"total_findings", summary.TotalFindings,
		"filtered_findings", len(filteredFindings),
		"returned_findings", len(findings),
		"finding_limit", options.FindingLimit,
	)
	return &GitStorageChainAuditResponse{
		Findings:         findings,
		Groups:           summarizeChainIssueGroups(filteredFindings),
		Summary:          summary,
		PathPrefix:       model.PathPrefix,
		BucketPathPrefix: bucketPathPrefix,
	}, nil
}

func projectStorageChainAuditResponseForSubpath(base GitStorageChainAuditResponse, gitSubpath string, inventory []RepoInventoryFile, findingKind string, findingLimit int) *GitStorageChainAuditResponse {
	pathPrefix := normalizeRepoSubpath(gitSubpath)
	filteredFindings := make([]GitStorageChainFinding, 0, len(base.Findings))
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
					issueGitPaths[normalizeRepoSubpath(sourcePath)] = struct{}{}
				}
			}
			if len(finding.SourcePaths) == 0 && storagePathMatchesRepoSubpath(finding.NormalizedPath, pathPrefix) && !strings.Contains(finding.NormalizedPath, "://") {
				issueGitPaths[normalizeRepoSubpath(finding.NormalizedPath)] = struct{}{}
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

	projected := base
	projected.Findings = filteredFindings
	projected.Groups = summarizeChainIssueGroups(filteredFindings)
	projected.Summary = summary
	projected.PathPrefix = pathPrefix
	projected.BucketPathPrefix = ""
	return projectStorageChainAuditResponse(projected, findingKind, findingLimit)
}

func storageChainFindingHasGitPath(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "git_only_no_syfon", "syfon_git_no_bucket", "git_syfon_metadata_mismatch", "probe_error":
		return true
	}
	return false
}

func projectStorageChainAuditResponse(base GitStorageChainAuditResponse, findingKind string, findingLimit int) *GitStorageChainAuditResponse {
	response := cloneStorageChainAuditResponse(base)
	filteredFindings := filterStorageChainFindingsByKind(response.Findings, findingKind)
	summary := filterStorageChainSummary(response.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, findingLimit, findingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = findingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	response.Findings = findings
	response.Groups = summarizeChainIssueGroups(filteredFindings)
	response.Summary = summary
	return &response
}

func storageChainFindingMatchesSubpath(finding GitStorageChainFinding, pathPrefix string) bool {
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
	pathPrefix = normalizeRepoSubpath(pathPrefix)
	if pathPrefix == "" {
		return true
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return false
	}
	if _, key, ok := parseStorageURL(value); ok {
		key = normalizeRepoSubpath(key)
		return key == pathPrefix || strings.HasPrefix(key, pathPrefix+"/") || strings.Contains("/"+key, "/"+pathPrefix+"/")
	}
	normalized := normalizeRepoSubpath(value)
	return normalized == pathPrefix || strings.HasPrefix(normalized, pathPrefix+"/") || strings.Contains("/"+normalized, "/"+pathPrefix+"/")
}

func logStorageChainAuditCacheError(timings *StorageChainAuditTimings, source string, operation string, err error) {
	if timings == nil || timings.Logf == nil || err == nil {
		return
	}
	timings.Logf("storage_chain_audit_cache_error %s source=%s operation=%s error=%q", strings.TrimSpace(timings.DebugPrefix), strings.TrimSpace(source), strings.TrimSpace(operation), err.Error())
}

func applyStorageChainAuditCacheMetadata(response *GitStorageChainAuditResponse, hit bool, cachedAt time.Time, refreshDurationMillis int64, source string, cacheError string) {
	if response == nil || cachedAt.IsZero() {
		return
	}
	response.Summary.AuditCacheHit = hit
	response.Summary.AuditCachedAt = cachedAt.UTC().Format(time.RFC3339Nano)
	response.Summary.AuditCacheAgeSeconds = int64(time.Since(cachedAt).Seconds())
	response.Summary.AuditRefreshDurationMs = refreshDurationMillis
	response.Summary.AuditCacheSource = strings.TrimSpace(source)
	response.Summary.AuditCacheError = strings.TrimSpace(cacheError)
}

func filterStorageChainFindingsByKind(findings []GitStorageChainFinding, kind string) []GitStorageChainFinding {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return append([]GitStorageChainFinding(nil), findings...)
	}
	filtered := make([]GitStorageChainFinding, 0, len(findings))
	for _, finding := range findings {
		if finding.Kind == kind {
			filtered = append(filtered, finding)
		}
	}
	return filtered
}

func filterStorageChainSummary(summary GitStorageChainAuditSummary, findings []GitStorageChainFinding) GitStorageChainAuditSummary {
	filteredCounts := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		filteredCounts[kind] = 0
	}
	filteredCounts["bucket_syfon_git_complete"] = summary.CountsByKind["bucket_syfon_git_complete"]
	for _, finding := range findings {
		filteredCounts[finding.Kind]++
	}
	summary.CountsByKind = filteredCounts
	summary.TotalFindings = len(findings)
	return summary
}

func limitStorageChainFindings(findings []GitStorageChainFinding, limit int, kind string) []GitStorageChainFinding {
	if limit <= 0 || limit >= len(findings) {
		return append([]GitStorageChainFinding(nil), findings...)
	}
	if strings.TrimSpace(kind) == "" {
		limited := make([]GitStorageChainFinding, 0, len(findings))
		countsByKind := make(map[string]int)
		for _, finding := range findings {
			if countsByKind[finding.Kind] >= limit {
				continue
			}
			limited = append(limited, finding)
			countsByKind[finding.Kind]++
		}
		return limited
	}
	return append([]GitStorageChainFinding(nil), findings[:limit]...)
}

func (service *StorageAnalyticsService) ApplyStorageCleanup(ctx context.Context, authorizationHeader string, organization string, project string, selectedRepoPaths []string, selectedActions []GitStorageCleanupApplyAction, selectedFindings []GitStorageCleanupApplyFinding, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool, dryRun bool) (*GitStorageCleanupApplyResponse, error) {
	if len(selectedFindings) == 0 {
		return nil, fmt.Errorf("cleanup apply requires findings from a prior audit; refusing to rebuild audit during apply")
	}
	canonicalFindings, scopes, err := service.canonicalizeStorageCleanupFindings(ctx, authorizationHeader, organization, project, selectedFindings)
	if err != nil {
		return nil, err
	}
	actionSelection := indexCleanupActions(selectedActions)
	plan := storageCleanupApplyPlan{
		UpdateAccessMethods: make(map[string][]gintegrationsyfon.ProjectAccessMethod),
	}
	selected := indexCleanupSelection(selectedRepoPaths)
	canonicalFindings, skippedAccessURLPaths, err := service.reconcileBrokenAccessURLFindings(
		ctx,
		authorizationHeader,
		organization,
		project,
		scopes,
		canonicalFindings,
		storageCleanupApplySelection{
			selected:                selected,
			actionSelection:         actionSelection,
			deleteRepoOrphans:       deleteRepoOrphans,
			deleteStaleDuplicates:   deleteStaleDuplicates,
			deleteBucketOnlyObjects: deleteBucketOnlyObjects,
			repairBrokenMappings:    repairBrokenBucketMappings,
		},
	)
	if err != nil {
		return nil, err
	}
	plan.SkippedPaths = append(plan.SkippedPaths, skippedAccessURLPaths...)
	for _, finding := range canonicalFindings {
		if len(selected) > 0 && !storageApplyFindingSelected(selected, finding) {
			continue
		}
		action, err := resolveStorageCleanupApplyAction(finding, actionSelection, deleteRepoOrphans, deleteStaleDuplicates, deleteBucketOnlyObjects, repairBrokenBucketMappings)
		if err != nil {
			return nil, err
		}
		if err := addStorageCleanupApplyFindingToPlan(&plan, finding, action); err != nil {
			return nil, err
		}
	}
	if len(selectedRepoPaths) > 0 && len(plan.DeleteObjectIDs) == 0 && len(plan.DeleteStorageObjectIDs) == 0 && len(plan.DeleteBucketObjects) == 0 && len(plan.UpdateAccessMethods) == 0 && len(plan.ManualPaths) == 0 && len(plan.SkippedPaths) == 0 {
		return nil, fmt.Errorf("selected cleanup paths did not match provided cleanup findings")
	}
	plan.AccessMethodChanges, err = coalesceAccessMethodChanges(plan.AccessMethodChanges)
	if err != nil {
		return nil, err
	}
	return service.executeStorageCleanupApplyPlan(ctx, authorizationHeader, organization, project, plan, dryRun)
}

func (service *StorageAnalyticsService) canonicalizeStorageCleanupFindings(ctx context.Context, authorizationHeader string, organization string, project string, findings []GitStorageCleanupApplyFinding) ([]GitStorageCleanupApplyFinding, []domain.StorageBucketScope, error) {
	if !storageCleanupFindingsContainStorageURL(findings) {
		return append([]GitStorageCleanupApplyFinding(nil), findings...), nil, nil
	}
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, nil, fmt.Errorf("load project storage scopes for cleanup apply: %w", err)
	}
	out := make([]GitStorageCleanupApplyFinding, 0, len(findings))
	for _, finding := range findings {
		clone := finding
		clone.BucketObjectURL = canonicalizeCleanupStorageURL(finding.BucketObjectURL, scopes, organization, project)
		clone.BucketObjectURLs = canonicalizeCleanupStorageURLs(finding.BucketObjectURLs, scopes, organization, project)
		clone.AccessURLs = canonicalizeCleanupStorageURLs(finding.AccessURLs, scopes, organization, project)
		if finding.Evidence != nil {
			evidence := *finding.Evidence
			evidence.BucketObjectURLs = canonicalizeCleanupStorageURLs(evidence.BucketObjectURLs, scopes, organization, project)
			evidence.AccessURLs = canonicalizeCleanupStorageURLs(evidence.AccessURLs, scopes, organization, project)
			clone.Evidence = &evidence
		}
		out = append(out, clone)
	}
	return out, scopes, nil
}

func storageCleanupFindingsContainStorageURL(findings []GitStorageCleanupApplyFinding) bool {
	for _, finding := range findings {
		candidates := []string{finding.NormalizedPath, finding.BucketObjectURL}
		candidates = append(candidates, finding.BucketObjectURLs...)
		candidates = append(candidates, finding.AccessURLs...)
		if finding.Evidence != nil {
			candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
			candidates = append(candidates, finding.Evidence.AccessURLs...)
		}
		for _, candidate := range candidates {
			if _, _, ok := parseStorageURL(candidate); ok {
				return true
			}
		}
	}
	return false
}

func canonicalizeCleanupStorageURL(value string, scopes []domain.StorageBucketScope, organization string, project string) string {
	if objectURL := canonicalizeScopedStorageURL(value, scopes, organization, project); objectURL != "" {
		return objectURL
	}
	if objectURL := canonicalStorageURL("", "", value); objectURL != "" {
		return objectURL
	}
	return strings.TrimSpace(value)
}

func canonicalizeCleanupStorageURLs(values []string, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if canonical := canonicalizeCleanupStorageURL(value, scopes, organization, project); canonical != "" {
			out = append(out, canonical)
		}
	}
	return uniqueStrings(out)
}

func resolveStorageCleanupApplyAction(finding GitStorageCleanupApplyFinding, actionSelection map[string]string, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool) (string, error) {
	kind := strings.TrimSpace(finding.Kind)
	if !knownStorageRepairKind(kind) {
		return "", fmt.Errorf("unsupported cleanup finding kind %q", kind)
	}
	action := cleanupActionForApplyFinding(actionSelection, finding)
	if action == "" && strings.TrimSpace(kind) == "probe_error" {
		return storageActionInspectEvidence, nil
	}
	if action == "" {
		action = strings.TrimSpace(finding.SuggestedAction)
	}
	if action == "" {
		action = legacyDefaultStorageCleanupAction(kind, deleteRepoOrphans, deleteStaleDuplicates, deleteBucketOnlyObjects, repairBrokenBucketMappings)
	}
	if action == "" {
		action = strings.TrimSpace(finding.DefaultAction)
	}
	if action == "" {
		action = storageRepairPolicyForKind(kind).defaultAction
	}
	if action == storageActionInspectEvidence {
		return action, nil
	}
	if !storageRepairActionAllowed(kind, action) {
		return "", fmt.Errorf("cleanup action %q is not supported for finding kind %q", action, kind)
	}
	return action, nil
}

func knownStorageRepairKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "bucket_only_object", "bucket_syfon_no_git",
		"repo_orphan_live_object", "repo_orphan_stale_record",
		"stale_duplicate_record", "live_duplicate_conflict",
		"broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping",
		"storage_validation_mismatch", "git_syfon_metadata_mismatch",
		"storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object",
		"git_only_no_syfon", "storage_probe_error", "probe_error":
		return true
	default:
		return false
	}
}

func legacyDefaultStorageCleanupAction(kind string, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool) string {
	switch strings.TrimSpace(kind) {
	case "repo_orphan_live_object", "repo_orphan_stale_record":
		if deleteRepoOrphans {
			return storageRepairPolicyForKind(kind).defaultAction
		}
	case "bucket_syfon_no_git":
		if deleteRepoOrphans {
			return storageActionDeleteBoth
		}
	case "stale_duplicate_record":
		if deleteStaleDuplicates {
			return storageActionDeleteSyfonRecord
		}
	case "bucket_only_object":
		if deleteBucketOnlyObjects {
			return storageActionDeleteBucketObject
		}
	case "broken_access_url_error", "broken_bucket_mapping", "syfon_broken_bucket_mapping":
		if repairBrokenBucketMappings {
			return storageActionRemoveBrokenAccessURLs
		}
	}
	return ""
}

func storageRepairActionAllowed(kind string, action string) bool {
	for _, allowed := range storageRepairPolicyForKind(kind).actions {
		if action == allowed {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func addStorageCleanupApplyFindingToPlan(plan *storageCleanupApplyPlan, finding GitStorageCleanupApplyFinding, action string) error {
	if err := appendStorageCleanupVerification(plan, finding, action); err != nil {
		return err
	}
	switch action {
	case storageActionInspectEvidence:
		plan.ManualPaths = append(plan.ManualPaths, finding.NormalizedPath)
	case storageActionDeleteSyfonRecord:
		objectIDs := storageApplyFindingObjectIDs(finding)
		if len(objectIDs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing object_ids for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
		plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
	case storageActionDeleteBucketObject:
		bucketURLs := storageApplyFindingBucketObjectURLs(finding)
		if len(bucketURLs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteBucketObjects = append(plan.DeleteBucketObjects, bucketURLs...)
	case storageActionDeleteBoth:
		objectIDs := storageApplyFindingObjectIDs(finding)
		bucketURLs := storageApplyFindingBucketObjectURLs(finding)
		if len(objectIDs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing object_ids for %s", finding.Kind, finding.NormalizedPath, action)
		}
		if strings.TrimSpace(finding.Kind) == "repo_orphan_live_object" {
			plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
			plan.DeleteStorageObjectIDs = append(plan.DeleteStorageObjectIDs, objectIDs...)
			plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
			return nil
		}
		if len(bucketURLs) == 0 {
			return fmt.Errorf("cleanup finding %q at %q is missing bucket object URLs for %s", finding.Kind, finding.NormalizedPath, action)
		}
		plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectIDs...)
		plan.DeleteBucketObjects = append(plan.DeleteBucketObjects, bucketURLs...)
		plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
	case storageActionRemoveBrokenAccessURLs:
		return addAccessMethodRepairToPlan(plan, finding)
	default:
		return fmt.Errorf("unsupported cleanup action %q for finding kind %q", action, finding.Kind)
	}
	return nil
}

func storageApplyFindingObjectIDs(finding GitStorageCleanupApplyFinding) []string {
	objectIDs := append([]string(nil), finding.ObjectIDs...)
	for _, record := range finding.Records {
		objectIDs = append(objectIDs, record.ObjectID)
	}
	return uniqueStrings(objectIDs)
}

func addAccessMethodRepairToPlan(plan *storageCleanupApplyPlan, finding GitStorageCleanupApplyFinding) error {
	records := finding.Records
	if len(records) == 0 {
		for _, objectID := range finding.ObjectIDs {
			records = append(records, GitStorageCleanupRecordAudit{
				ObjectID:   objectID,
				AccessURLs: append([]string(nil), finding.AccessURLs...),
			})
		}
	}
	if len(records) == 0 {
		return fmt.Errorf("cleanup finding %q at %q is missing records for %s", finding.Kind, finding.NormalizedPath, storageActionRemoveBrokenAccessURLs)
	}
	for _, record := range records {
		objectID := strings.TrimSpace(record.ObjectID)
		if objectID == "" {
			return fmt.Errorf("cleanup finding %q at %q has a record without object_id", finding.Kind, finding.NormalizedPath)
		}
		if len(brokenAccessURLsForRecord(record)) == 0 {
			return fmt.Errorf("cleanup finding %q at %q record %q is missing broken access URL evidence", finding.Kind, finding.NormalizedPath, objectID)
		}
		remaining := remainingAccessMethodsAfterBrokenRemoval(record)
		plan.AccessMethodChanges = append(plan.AccessMethodChanges, accessMethodChangeForRecord(record, remaining))
		if len(remaining) == 0 {
			plan.DeleteObjectIDs = append(plan.DeleteObjectIDs, objectID)
			plan.RepoDeletePaths = append(plan.RepoDeletePaths, finding.NormalizedPath)
			continue
		}
		plan.UpdateAccessMethods[objectID] = remaining
		plan.UpdatedRecordIDs = append(plan.UpdatedRecordIDs, objectID)
	}
	return nil
}

func remainingAccessMethodsAfterBrokenRemoval(record GitStorageCleanupRecordAudit) []gintegrationsyfon.ProjectAccessMethod {
	methods := projectAccessMethodsFromCleanupMethods(record.AccessMethods)
	if len(methods) == 0 {
		for _, accessURL := range record.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				methods = append(methods, gintegrationsyfon.ProjectAccessMethod{URL: trimmed})
			}
		}
	}
	brokenURLs := brokenAccessURLsForRecord(record)
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(methods))
	for _, method := range methods {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		if _, broken := brokenURLs[normalizeCleanupSelectionKey(url)]; broken {
			continue
		}
		remaining = append(remaining, method)
	}
	return appendReplacementAccessMethods(remaining, record.AccessProbes, brokenURLs)
}

func brokenAccessURLsForRecord(record GitStorageCleanupRecordAudit) map[string]struct{} {
	broken := make(map[string]struct{})
	for _, probe := range record.AccessProbes {
		url := normalizeCleanupSelectionKey(probe.URL)
		if url == "" {
			continue
		}
		if accessProbeIsBroken(probe) {
			broken[url] = struct{}{}
		}
	}
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) == "" {
			continue
		}
		if len(record.AccessProbes) == 0 && recordStatusMeansBrokenAccess(record.Status) {
			broken[normalizeCleanupSelectionKey(accessURL)] = struct{}{}
		}
	}
	return broken
}

func appendReplacementAccessMethods(existing []gintegrationsyfon.ProjectAccessMethod, probes []GitStorageCleanupAccessProbe, brokenURLs map[string]struct{}) []gintegrationsyfon.ProjectAccessMethod {
	seen := make(map[string]struct{}, len(existing)+len(probes))
	out := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(existing)+len(probes))
	for _, method := range existing {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		key := normalizeCleanupSelectionKey(url)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, method)
	}
	for _, probe := range probes {
		if strings.TrimSpace(probe.Status) != "present" || !probeValidationMatched(probe.ValidationStatus) {
			continue
		}
		replacementURL := canonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if replacementURL == "" {
			continue
		}
		key := normalizeCleanupSelectionKey(replacementURL)
		if _, broken := brokenURLs[key]; broken {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, gintegrationsyfon.ProjectAccessMethod{
			AccessID: "s3",
			Type:     "s3",
			URL:      replacementURL,
		})
	}
	return out
}

func probeValidationMatched(status string) bool {
	switch strings.TrimSpace(status) {
	case "", "not_requested", "matched":
		return true
	default:
		return false
	}
}

func recordStatusMeansBrokenAccess(status string) bool {
	switch strings.TrimSpace(status) {
	case "missing", "error":
		return true
	default:
		return false
	}
}

func accessProbeIsBroken(probe GitStorageCleanupAccessProbe) bool {
	switch strings.TrimSpace(probe.ErrorKind) {
	case "missing_access_url", "credential_missing":
		return true
	}
	switch strings.TrimSpace(probe.Status) {
	case "missing", "forbidden", "unsupported", "invalid", "error":
		return true
	case "not_found":
		return strings.TrimSpace(probe.Operation) == StorageChainValidationModeMetadata
	}
	return false
}

func (service *StorageAnalyticsService) executeStorageCleanupApplyPlan(ctx context.Context, authorizationHeader string, organization string, project string, plan storageCleanupApplyPlan, dryRun bool) (*GitStorageCleanupApplyResponse, error) {
	if err := service.verifyAccessMethodChanges(ctx, authorizationHeader, plan.AccessMethodChanges); err != nil {
		return nil, err
	}
	toDelete := uniqueStrings(plan.DeleteObjectIDs)
	toDeleteWithStorage := uniqueStrings(plan.DeleteStorageObjectIDs)
	toDeleteMetadataOnly := differenceStrings(toDelete, toDeleteWithStorage)
	toDeleteBucketObjects := uniqueStrings(plan.DeleteBucketObjects)
	repoDeletePaths := uniqueStrings(plan.RepoDeletePaths)
	deletedBucketObjectURLs := make([]string, 0)
	updatedRecordIDs := uniqueStrings(plan.UpdatedRecordIDs)
	manualPaths := uniqueStrings(plan.ManualPaths)
	skippedPaths := uniqueStrings(plan.SkippedPaths)
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
			AccessMethodChanges:     plan.AccessMethodChanges,
		}, nil
	}
	if err := service.verifyStorageCleanupApplyPlan(ctx, authorizationHeader, &plan); err != nil {
		return nil, err
	}
	toDeleteBucketObjects = uniqueStrings(plan.DeleteBucketObjects)
	skippedPaths = uniqueStrings(plan.SkippedPaths)
	if len(plan.UpdateAccessMethods) > 0 {
		if err := service.storage.BulkUpdateAccessMethods(ctx, authorizationHeader, plan.UpdateAccessMethods); err != nil {
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
	if len(plan.UpdateAccessMethods) > 0 || len(toDelete) > 0 || len(toDeleteBucketObjects) > 0 {
		service.evictProjectJoinCache(organization, project)
		service.evictProjectAuditRecordCache(organization, project)
		service.evictProjectChainInputCache(organization, project)
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
		AccessMethodChanges:     plan.AccessMethodChanges,
	}, nil
}

func indexCleanupSelection(paths []string) map[string]struct{} {
	index := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		normalized := normalizeCleanupSelectionKey(path)
		if normalized == "" {
			continue
		}
		index[normalized] = struct{}{}
	}
	return index
}

func storageApplyFindingSelected(selected map[string]struct{}, finding GitStorageCleanupApplyFinding) bool {
	for _, candidate := range storageApplyFindingSelectionCandidates(finding) {
		if _, ok := selected[candidate]; ok {
			return true
		}
	}
	return false
}

func storageApplyFindingSelectionCandidates(finding GitStorageCleanupApplyFinding) []string {
	candidates := []string{
		finding.NormalizedPath,
		finding.BucketObjectURL,
	}
	candidates = append(candidates, finding.ObjectIDs...)
	candidates = append(candidates, finding.BucketObjectURLs...)
	candidates = append(candidates, finding.AccessURLs...)
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.AccessURLs...)
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.ObjectIDs...)
	}
	return uniqueCleanupSelectionCandidates(candidates)
}

func storageApplyFindingBucketObjectURLs(finding GitStorageCleanupApplyFinding) []string {
	candidates := []string{
		finding.BucketObjectURL,
	}
	candidates = append(candidates, finding.BucketObjectURLs...)
	candidates = append(candidates, finding.AccessURLs...)
	if finding.Evidence != nil {
		candidates = append(candidates, finding.Evidence.BucketObjectURLs...)
		candidates = append(candidates, finding.Evidence.AccessURLs...)
	}
	if len(uniqueCleanupSelectionCandidates(candidates)) == 0 && canonicalStorageURL("", "", finding.NormalizedPath) != "" {
		candidates = append(candidates, finding.NormalizedPath)
	}
	return uniqueCleanupSelectionCandidates(candidates)
}

func cleanupActionForApplyFinding(index map[string]string, finding GitStorageCleanupApplyFinding) string {
	for _, path := range storageApplyFindingSelectionCandidates(finding) {
		if action := strings.TrimSpace(index[cleanupActionKey(finding.Kind, path)]); action != "" {
			return action
		}
		if action := strings.TrimSpace(index[cleanupActionKey("", path)]); action != "" {
			return action
		}
	}
	return ""
}

func indexCleanupActions(actions []GitStorageCleanupApplyAction) map[string]string {
	if len(actions) == 0 {
		return nil
	}
	index := make(map[string]string, len(actions))
	for _, action := range actions {
		if strings.TrimSpace(action.Action) == "" {
			continue
		}
		path := normalizeRepoSubpath(action.NormalizedPath)
		if path == "" {
			continue
		}
		key := cleanupActionKey(strings.TrimSpace(action.Kind), path)
		index[key] = strings.TrimSpace(action.Action)
	}
	return index
}

func cleanupActionKey(kind string, path string) string {
	return strings.TrimSpace(kind) + "::" + normalizeRepoSubpath(path)
}

func (service *StorageAnalyticsService) loadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*repoAnalyticsIndex, []RepoInventoryFile, map[string][]projectRecordState, map[string]gintegrationsyfon.FileUsage, error) {
	index, err := loadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	inventory, err := filterRepoInventoryFiles(index, gitSubpath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	recordsByChecksum, usageByObjectID, err := service.loadProjectJoinCache(ctx, authorizationHeader, organization, project, hash, index.sidecar.Files, forceRefresh)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return index, inventory, recordsByChecksum, usageByObjectID, nil
}

func (service *StorageAnalyticsService) loadProjectJoinCache(ctx context.Context, authorizationHeader string, organization string, project string, hash plumbing.Hash, inventory []RepoInventoryFile, forceRefresh bool) (map[string][]projectRecordState, map[string]gintegrationsyfon.FileUsage, error) {
	cacheKey := service.projectJoinCacheKey(organization, project, hash.String())
	redisKey := storageExactProjectJoinCacheKey(organization, project, hash.String())
	service.projectJoinMu.Lock()
	if !forceRefresh {
		if cached, ok := service.projectJoinCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
			service.projectJoinMu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=memory records_by_checksum=%d usage_rows=%d", organization, project, len(cached.recordsByChecksum), len(cached.usageByObjectID))
			return cloneRecordStateMap(cached.recordsByChecksum), cloneFileUsageMap(cached.usageByObjectID), nil
		}
	}
	if inflight, ok := service.projectJoinWork[cacheKey]; ok {
		service.projectJoinMu.Unlock()
		<-inflight.done
		return cloneRecordStateMap(inflight.recordsByChecksum), cloneFileUsageMap(inflight.usageByObjectID), inflight.err
	}
	inflight := &inflightProjectJoinState{done: make(chan struct{})}
	service.projectJoinWork[cacheKey] = inflight
	service.projectJoinMu.Unlock()
	defer func() {
		service.projectJoinMu.Lock()
		delete(service.projectJoinWork, cacheKey)
		close(inflight.done)
		service.projectJoinMu.Unlock()
	}()

	if !forceRefresh && service.exactProjectJoinCache != nil {
		redisStart := time.Now()
		cached, ok, err := service.exactProjectJoinCache.Get(ctx, redisKey)
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=get error=%q", organization, project, service.exactProjectJoinCache.Source(), err.Error())
		}
		if ok {
			state := cachedProjectJoinState{
				expiresAt:         time.Now().Add(projectJoinCacheTTL),
				recordsByChecksum: cloneRecordStateMap(cached.RecordsByChecksum),
				usageByObjectID:   cloneFileUsageMap(cached.UsageByObjectID),
			}
			service.projectJoinMu.Lock()
			service.projectJoinCache[cacheKey] = state
			service.projectJoinMu.Unlock()
			log.Printf("storage_folder_exact_join_cache_hit org=%s project=%s source=%s age_seconds=%d records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), int64(time.Since(cached.CachedAt).Seconds()), len(cached.RecordsByChecksum), len(cached.UsageByObjectID), time.Since(redisStart).Milliseconds())
			inflight.recordsByChecksum = cloneRecordStateMap(cached.RecordsByChecksum)
			inflight.usageByObjectID = cloneFileUsageMap(cached.UsageByObjectID)
			return cloneRecordStateMap(cached.RecordsByChecksum), cloneFileUsageMap(cached.UsageByObjectID), nil
		}
		log.Printf("storage_folder_exact_join_cache_miss org=%s project=%s source=%s duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), time.Since(redisStart).Milliseconds())
	}

	recordStart := time.Now()
	projectRecords, err := service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		inflight.err = err
		return nil, nil, inflight.err
	}
	checksums := inventoryChecksumSet(inventory)
	matchedRecords := filterProjectRecordsByChecksum(projectRecords, checksums)
	log.Printf("storage_folder_exact_syfon_local_join org=%s project=%s git_checksums=%d project_records=%d matched_records=%d duration_ms=%d", organization, project, len(checksums), len(projectRecords), len(matchedRecords), time.Since(recordStart).Milliseconds())

	usageStart := time.Now()
	objectIDs := projectRecordObjectIDs(matchedRecords)
	usageByObjectID, err := service.listProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs, cleanupInactiveDays)
	if err != nil {
		inflight.err = fmt.Errorf("list syfon project file usage: %w", err)
		return nil, nil, inflight.err
	}
	log.Printf("storage_folder_exact_bulk_usage_lookup org=%s project=%s object_ids=%d usage_rows=%d duration_ms=%d", organization, project, len(objectIDs), len(usageByObjectID), time.Since(usageStart).Milliseconds())

	recordsByChecksum := buildProjectJoinRecordsByChecksum(matchedRecords, usageByObjectID)
	service.projectJoinMu.Lock()
	service.projectJoinCache[cacheKey] = cachedProjectJoinState{
		expiresAt:         time.Now().Add(projectJoinCacheTTL),
		recordsByChecksum: cloneRecordStateMap(recordsByChecksum),
		usageByObjectID:   cloneFileUsageMap(usageByObjectID),
	}
	service.projectJoinMu.Unlock()
	if service.exactProjectJoinCache != nil {
		setStart := time.Now()
		err := service.exactProjectJoinCache.Set(ctx, redisKey, cachedExactProjectJoinState{
			CachedAt:          time.Now(),
			RecordsByChecksum: recordsByChecksum,
			UsageByObjectID:   usageByObjectID,
		}, storageChainAuditCacheTTL())
		if err != nil {
			log.Printf("storage_folder_exact_join_cache_error org=%s project=%s source=%s operation=set error=%q", organization, project, service.exactProjectJoinCache.Source(), err.Error())
		} else {
			log.Printf("storage_folder_exact_join_cache_store org=%s project=%s source=%s records_by_checksum=%d usage_rows=%d duration_ms=%d", organization, project, service.exactProjectJoinCache.Source(), len(recordsByChecksum), len(usageByObjectID), time.Since(setStart).Milliseconds())
		}
	}
	inflight.recordsByChecksum = cloneRecordStateMap(recordsByChecksum)
	inflight.usageByObjectID = cloneFileUsageMap(usageByObjectID)
	return recordsByChecksum, usageByObjectID, nil
}

func inventoryChecksumSet(inventory []RepoInventoryFile) map[string]struct{} {
	checksums := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		checksum := normalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		checksums[checksum] = struct{}{}
	}
	return checksums
}

func filterProjectRecordsByChecksum(records []gintegrationsyfon.ProjectRecord, checksums map[string]struct{}) []gintegrationsyfon.ProjectRecord {
	out := make([]gintegrationsyfon.ProjectRecord, 0)
	for _, record := range records {
		normalizedChecksum := normalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		if _, ok := checksums[normalizedChecksum]; !ok {
			continue
		}
		record.Checksum = normalizedChecksum
		out = append(out, record)
	}
	return out
}

func projectRecordObjectIDs(records []gintegrationsyfon.ProjectRecord) []string {
	objectIDs := make([]string, 0, len(records))
	for _, record := range records {
		objectIDs = append(objectIDs, record.ObjectID)
	}
	return uniqueStrings(objectIDs)
}

func buildProjectJoinRecordsByChecksum(records []gintegrationsyfon.ProjectRecord, usageByObjectID map[string]gintegrationsyfon.FileUsage) map[string][]projectRecordState {
	recordsByChecksum := make(map[string][]projectRecordState)
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
	return recordsByChecksum
}

func (service *StorageAnalyticsService) listProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	usageByObjectID := make(map[string]gintegrationsyfon.FileUsage)
	if len(objectIDs) == 0 {
		return usageByObjectID, nil
	}
	for start := 0; start < len(objectIDs); start += projectFileUsageBulkChunkSize {
		end := start + projectFileUsageBulkChunkSize
		if end > len(objectIDs) {
			end = len(objectIDs)
		}
		chunkUsage, err := service.storage.ListProjectFileUsageByObjectIDs(ctx, authorizationHeader, organization, project, objectIDs[start:end], inactiveDays)
		if err != nil {
			if gintegrationsyfon.IsHTTPStatus(err, http.StatusNotFound, http.StatusMethodNotAllowed) {
				return service.storage.ListProjectFileUsage(ctx, authorizationHeader, organization, project, inactiveDays)
			}
			return nil, err
		}
		for objectID, usage := range chunkUsage {
			usageByObjectID[objectID] = usage
		}
	}
	return usageByObjectID, nil
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

func (service *StorageAnalyticsService) evictProjectAuditRecordCache(organization string, project string) {
	cacheKey := service.projectChainInputCacheKey(organization, project)
	service.chainInputMu.Lock()
	delete(service.projectAuditCache, cacheKey)
	service.chainInputMu.Unlock()
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

func applyScopedStorageMappings(recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, scopes []domain.StorageBucketScope, organization string, project string) (map[string][]projectRecordState, map[string][]projectRecordState) {
	attach := func(input map[string][]projectRecordState) map[string][]projectRecordState {
		out := make(map[string][]projectRecordState, len(input))
		for checksum, group := range input {
			states := make([]projectRecordState, 0, len(group))
			for _, record := range group {
				clone := record
				clone.CanonicalAccessURLs = canonicalizeRecordAccessURLs(record.AccessURLs, scopes, organization, project)
				clone.CanonicalAccessURLByRaw = canonicalizeRecordAccessURLMappings(record.AccessURLs, scopes, organization, project)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return attach(recordsByChecksum), attach(allProjectRecords)
}

func (service *StorageAnalyticsService) attachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState) (map[string][]projectRecordState, map[string][]projectRecordState, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, false)
}

func (service *StorageAnalyticsService) attachStorageListValidations(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState) (map[string][]projectRecordState, map[string][]projectRecordState, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, true)
}

func (service *StorageAnalyticsService) attachStorageValidationResults(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]projectRecordState, allProjectRecords map[string][]projectRecordState, useListValidation bool) (map[string][]projectRecordState, map[string][]projectRecordState, error) {
	started := time.Now()
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemKeys := map[string]string{}
	recordProbeKeysByObjectID := map[string][]string{}
	accessURLCount := 0

	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, accessURL := range probeAccessURLsForRecord(record) {
				accessURLCount++
				normalizedURL := strings.TrimSpace(accessURL)
				if normalizedURL == "" {
					continue
				}
				key := storageProbeRequestKey(normalizedURL, record.Size, record.Checksum)
				expectedName := ""
				if useListValidation {
					expectedName = expectedStorageObjectNameForListValidation(normalizedURL, record.Name)
					key = storageListValidationRequestKey(normalizedURL, record.Size, expectedName)
				}
				recordProbeKeysByObjectID[record.ObjectID] = append(recordProbeKeysByObjectID[record.ObjectID], key)
				if _, ok := itemKeys[key]; ok {
					continue
				}
				itemKeys[key] = key
				expectedSize := record.Size
				item := gintegrationsyfon.BulkStorageProbeItem{
					ID:                key,
					ObjectURL:         normalizedURL,
					ExpectedSizeBytes: &expectedSize,
					ExpectedSHA256:    strings.TrimSpace(record.Checksum),
				}
				if useListValidation {
					item.ExpectedSHA256 = ""
					item.ExpectedName = expectedName
				}
				items = append(items, item)
				if useListValidation && len(items) <= storageChainValidationDebugSampleLimit {
					log.Printf(
						"INFO: storage_chain_bulk_list_item object_id=%s name=%q checksum=%s submitted_url=%s raw_access_urls=%q canonical_access_urls=%q expected_size=%d expected_name=%q",
						strings.TrimSpace(record.ObjectID),
						strings.TrimSpace(record.Name),
						strings.TrimSpace(record.Checksum),
						normalizedURL,
						strings.Join(rawAccessURLsForRecord(record), ","),
						strings.Join(accessURLsForStorage(record), ","),
						record.Size,
						item.ExpectedName,
					)
				}
			}
		}
	}
	mode := "head"
	operation := StorageChainValidationModeMetadata
	if useListValidation {
		mode = "list"
		operation = StorageChainValidationModeList
	}
	log.Printf(
		"INFO: storage_chain_validation_request_built mode=%s record_count=%d access_url_count=%d unique_request_count=%d duplicate_request_count=%d build_ms=%d",
		mode,
		countRecordStates(allProjectRecords),
		accessURLCount,
		len(items),
		accessURLCount-len(items),
		time.Since(started).Milliseconds(),
	)

	resultsByKey := map[string]gintegrationsyfon.BulkStorageProbeResult{}
	if len(items) > 0 {
		var (
			results []gintegrationsyfon.BulkStorageProbeResult
			err     error
		)
		if useListValidation {
			results, err = service.storage.BulkListStorageObjects(ctx, authorizationHeader, items)
		} else {
			results, err = service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, items)
		}
		if err != nil {
			if useListValidation {
				log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
				return nil, nil, fmt.Errorf("list-validate syfon storage objects: %w", err)
			}
			log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
			return nil, nil, fmt.Errorf("probe syfon storage objects: %w", err)
		}
		for _, result := range results {
			result.Operation = operation
			resultsByKey[strings.TrimSpace(result.ID)] = result
		}
	}
	log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d result_count=%d duration_ms=%d", mode, len(items), len(resultsByKey), time.Since(started).Milliseconds())

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
						result.Operation = operation
						probes = append(probes, result)
					}
				}
				clone.AccessProbes = append(append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...), probes...)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}

	return attach(recordsByChecksum), attach(allProjectRecords), nil
}

func countRecordStates(input map[string][]projectRecordState) int {
	count := 0
	for _, group := range input {
		count += len(group)
	}
	return count
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
		if normalized := normalizeCleanupSelectionKey(path); normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	includePath := func(paths ...string) bool {
		if len(allowed) == 0 {
			return true
		}
		for _, path := range paths {
			if _, ok := allowed[normalizeCleanupSelectionKey(path)]; ok {
				return true
			}
		}
		return false
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
				repairMatches := make([]projectRecordState, 0, len(matches))
				for _, match := range matches {
					repairMatches = append(repairMatches, repairableBrokenAccessRecord(match))
				}
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, repairMatches, false, "access_url", "Syfon access URL did not resolve through a configured bucket mapping.")
				repairDeleteIDs, repairUpdates := brokenBucketMappingRepairPlan(repairMatches)
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
				public := buildCleanupFinding(string(storageFindingKind), item.RepoPath, matches, false, "access_url", "Mapped bucket object exists, but object evidence disagrees with the Syfon/Git record.")
				findings = append(findings, cleanupFindingModel{
					Public:              public,
					DeleteObjectIDs:     recordObjectIDs(matches),
					DeleteBucketObjects: cleanupMatchedBucketObjectURLs(matches, bucketObjectsByURL),
					Manual:              true,
				})
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
		if !includePath(cleanupSelectionCandidatesForRecords(checksum, displayPath, matches)...) {
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
			if !includePath(cleanupSelectionCandidatesForBucketObject(objectURL, bucketObjectsByURL[objectURL])...) {
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
