package storageaudit

import (
	"context"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) BuildStorageChainAuditWithOptions(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, options StorageChainAuditOptions) (*ChainAuditResponse, error) {
	return service.chainSvc.BuildStorageChainAuditWithOptions(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, options)
}

func (service *StorageAnalyticsService) LoadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, record.BucketInventoryMetadata, error) {
	return service.chainSvc.LoadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceRefresh)
}

func (service *StorageAnalyticsService) loadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return service.chainSvc.LoadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
}

func newChainService(service *StorageAnalyticsService) *chain.Service {
	return chain.New(chainStore{service: service})
}

type chainStore struct {
	service *StorageAnalyticsService
}

func (store chainStore) LoadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*chain.JoinState, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := store.service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, forceRefresh)
	if err != nil {
		return nil, err
	}
	return &chain.JoinState{
		Index:             index,
		Inventory:         inventory,
		RecordsByChecksum: recordsByChecksum,
		UsageByObjectID:   usageByObjectID,
	}, nil
}

func (store chainStore) LoadRepoIndex(ctx context.Context, ref string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*repoindex.Index, error) {
	return repoindex.LoadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
}

func (store chainStore) ListProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]record.State, error) {
	return store.service.listProjectRecordStates(ctx, authorizationHeader, organization, project, usageByObjectID)
}

func (store chainStore) GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error) {
	return store.service.storage.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
}

func (store chainStore) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return store.service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, "")
}

func (store chainStore) ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error) {
	return store.service.storage.ListProjectBucketObjects(ctx, authorizationHeader, organization, project, bucketPathPrefix)
}

func (store chainStore) ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error) {
	return store.service.storage.ListProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix)
}

func (store chainStore) ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return store.service.storage.ListProjectScopes(ctx, authorizationHeader, organization, project)
}

func (store chainStore) LoadProjectStorageScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return store.service.loadProjectStorageScopes(ctx, authorizationHeader, organization, project)
}

func (store chainStore) AttachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State) (map[string][]record.State, map[string][]record.State, error) {
	return store.service.attachStorageProbes(ctx, authorizationHeader, recordsByChecksum, allProjectRecords)
}

func (store chainStore) AttachStorageListValidations(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State) (map[string][]record.State, map[string][]record.State, error) {
	return store.service.attachStorageListValidations(ctx, authorizationHeader, recordsByChecksum, allProjectRecords)
}

func (store chainStore) LoadStorageAuditSyfonRevision(ctx context.Context, authorizationHeader string, organization string, project string) (string, error) {
	return store.service.loadStorageAuditSyfonRevision(ctx, authorizationHeader, organization, project)
}
