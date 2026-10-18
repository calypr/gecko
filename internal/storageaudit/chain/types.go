package chain

import (
	"context"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

const (
	chainInputCacheTTL               = 45 * time.Second
	chainProjectRecordCacheMaxAge    = 30 * time.Minute
	storageChainValidationDebugLimit = 20
)

type BaseInputs struct {
	Index             *repoindex.Index
	Inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]record.State
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type JoinState struct {
	Index             *repoindex.Index
	Inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]record.State
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type JoinReader interface {
	LoadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*JoinState, error)
	LoadRepoIndex(ctx context.Context, ref string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*repoindex.Index, error)
}

type RecordReader interface {
	ListProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]record.State, error)
	GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error)
	ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	LoadStorageAuditSyfonRevision(ctx context.Context, authorizationHeader string, organization string, project string) (string, error)
}

type ScopeReader interface {
	ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
	LoadProjectStorageScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
}

type BucketReader interface {
	ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
	ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
}

type EvidenceAttacher interface {
	AttachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State) (map[string][]record.State, map[string][]record.State, error)
	AttachStorageListValidations(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State) (map[string][]record.State, map[string][]record.State, error)
}

type Dependencies struct {
	Join     JoinReader
	Records  RecordReader
	Scopes   ScopeReader
	Buckets  BucketReader
	Evidence EvidenceAttacher
}

type chainAuditModel struct {
	Findings   []model.ChainFinding
	Summary    model.ChainSummary
	PathPrefix string
}

type ChainInputCacheEntry struct {
	ExpiresAt          time.Time
	ProjectRecords     []gintegrationsyfon.ProjectRecord
	ProjectScopes      []domain.StorageBucketScope
	BucketSummary      *gintegrationsyfon.ProjectBucketSummary
	BucketObjects      []gintegrationsyfon.ProjectBucketObject
	BucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
	BucketMetadata     record.BucketInventoryMetadata
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

type Service struct {
	deps Dependencies

	chainInputMu      sync.RWMutex
	ChainInputCache   map[string]ChainInputCacheEntry
	projectAuditCache map[string]cachedProjectAuditRecordState
	projectAuditWork  map[string]*inflightProjectAuditRecordState

	ChainAuditRefreshMu     sync.Mutex
	ChainAuditRefreshWork   map[string]*InflightResponseRefresh
	ChainAuditResponseCache storecache.ResponseCache
	ProjectBucketCache      storecache.BucketInventoryCache
}

func New(deps Dependencies) *Service {
	return &Service{
		deps:                  deps,
		ChainInputCache:       map[string]ChainInputCacheEntry{},
		projectAuditCache:     map[string]cachedProjectAuditRecordState{},
		projectAuditWork:      map[string]*inflightProjectAuditRecordState{},
		ChainAuditRefreshWork: map[string]*InflightResponseRefresh{},
	}
}

func (service *Service) EvictProjectAuditRecords(cacheKey string) {
	service.chainInputMu.Lock()
	delete(service.projectAuditCache, cacheKey)
	service.chainInputMu.Unlock()
}

func (service *Service) EnableResponseCachesFromEnv() {
	service.ChainAuditResponseCache = storecache.NewStorageChainAuditResponseCacheFromEnv()
	service.ProjectBucketCache = storecache.NewProjectBucketInventoryCacheFromEnv()
}

func (service *Service) LoadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, record.BucketInventoryMetadata, error) {
	return service.loadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceRefresh)
}

func (service *Service) EvictProjectChainInputCache(organization string, project string) {
	service.evictProjectChainInputCache(organization, project)
}

func (service *Service) LoadStorageAuditStorageView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *record.RecordSet, includeBucketInventory bool, includeProbes bool) (*record.StorageView, error) {
	return service.loadStorageAuditStorageView(ctx, authorizationHeader, organization, project, recordSet, includeBucketInventory, includeProbes)
}

func (service *Service) LoadBaseInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*BaseInputs, error) {
	base, err := service.loadStorageAuditBaseInputs(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	return &BaseInputs{
		Index:             base.index,
		Inventory:         base.inventory,
		RecordsByChecksum: base.RecordsByChecksum,
		UsageByObjectID:   base.usageByObjectID,
	}, nil
}

func (service *Service) LoadScopedProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string, base *BaseInputs) (*record.RecordSet, error) {
	return service.loadScopedProjectRecords(ctx, authorizationHeader, organization, project, &storageAuditBaseInputs{
		index:             base.Index,
		inventory:         base.Inventory,
		RecordsByChecksum: base.RecordsByChecksum,
		usageByObjectID:   base.UsageByObjectID,
	})
}

func (service *Service) LoadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
}

func (service *Service) LoadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]model.RepoInventoryFile, error) {
	return service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
}

func (service *Service) LoadCachedProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
}

func (service *Service) ProjectChainInputCacheKey(organization string, project string) string {
	return service.projectChainInputCacheKey(organization, project)
}

func (service *Service) ExpireChainInputCacheEntry(key string) {
	service.chainInputMu.Lock()
	entry := service.ChainInputCache[key]
	entry.ExpiresAt = time.Now().Add(-time.Second)
	service.ChainInputCache[key] = entry
	service.chainInputMu.Unlock()
}
