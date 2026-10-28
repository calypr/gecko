package chain

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/sharedwork"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

const (
	projectScopeCacheTTL             = 45 * time.Second
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

type AuditEvidenceOptions struct {
	Organization             string
	Project                  string
	Scopes                   []domain.StorageBucketScope
	BucketObjects            []gintegrationsyfon.ProjectBucketObject
	BucketObjectsByURL       map[string]gintegrationsyfon.ProjectBucketObject
	BucketInventoryComplete  bool
	BucketInventoryStale     bool
	BucketPathPrefix         string
	ForceStorageRevalidation bool
}

type AuditEvidenceAttacher interface {
	AttachStorageProbesForAudit(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State, options AuditEvidenceOptions) (map[string][]record.State, map[string][]record.State, error)
	AttachStorageListValidationsForAudit(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State, options AuditEvidenceOptions) (map[string][]record.State, map[string][]record.State, error)
}

type Dependencies struct {
	Join      JoinReader
	Records   RecordReader
	Scopes    ScopeReader
	Buckets   BucketReader
	Evidence  EvidenceAttacher
	AuditWork *sharedwork.Group[StorageChainAuditRefreshResult]
}

type StorageChainAuditRefreshResult struct {
	Response    storecache.CachedResponse
	CacheHit    bool
	CacheSource string
}

type chainAuditModel struct {
	Findings   []model.ChainFinding
	Summary    model.ChainSummary
	PathPrefix string
}

type cachedProjectScopeState struct {
	expiresAt time.Time
	scopes    []domain.StorageBucketScope
}

type projectAuditRecordValidator struct {
	RecordCount             int
	RecordLatestUpdatedTime string
	RecordRevision          string
}

type Service struct {
	deps Dependencies

	chainInputMu      sync.RWMutex
	projectScopeCache map[string]cachedProjectScopeState
	projectAuditWork  *sharedwork.Group[[]gintegrationsyfon.ProjectRecord]

	auditWork               *sharedwork.Group[StorageChainAuditRefreshResult]
	ProjectAuditCoordinator storecache.ProjectAuditCoordinator
	ChainAuditResponseCache storecache.ResponseCache
	ProjectBucketCache      storecache.BucketInventoryCache
}

func New(deps Dependencies) *Service {
	auditWork := deps.AuditWork
	if auditWork == nil {
		auditWork = sharedwork.New[StorageChainAuditRefreshResult](sharedwork.DefaultBudget)
	}
	return &Service{
		deps:              deps,
		projectScopeCache: map[string]cachedProjectScopeState{},
		projectAuditWork:  sharedwork.New[[]gintegrationsyfon.ProjectRecord](sharedwork.DefaultBudget),
		auditWork:         auditWork,
	}
}

func (service *Service) Shutdown(ctx context.Context) error {
	if service == nil {
		return nil
	}
	return errors.Join(service.auditWork.Shutdown(ctx), service.projectAuditWork.Shutdown(ctx))
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

func (service *Service) LoadProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return service.loadProjectAuditRecords(ctx, authorizationHeader, organization, project)
}
