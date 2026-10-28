package storageaudit

import (
	"context"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/registration"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) RegisterGitOnlySyfonRecords(ctx context.Context, authorizationHeader string, organization string, project string, ref string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, request GitOnlySyfonRegistrationRequest) (*GitOnlySyfonRegistrationResponse, error) {
	return service.registrationSvc.RegisterGitOnlySyfonRecords(ctx, authorizationHeader, organization, project, ref, mirrorPath, repo, hash, request)
}

type registrationStore struct {
	service *StorageAnalyticsService
}

func (store registrationStore) LoadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]model.RepoInventoryFile, error) {
	return store.service.chainSvc.LoadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
}

func (store registrationStore) LoadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return store.service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
}

func (store registrationStore) LoadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, record.BucketInventoryMetadata, error) {
	return store.service.LoadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceRefresh)
}

func (store registrationStore) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	return store.service.storage.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, checksums)
}

func (store registrationStore) BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return store.service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, items)
}

func (store registrationStore) RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error) {
	return store.service.storage.RegisterProjectObjects(ctx, authorizationHeader, candidates)
}

func newRegistrationService(service *StorageAnalyticsService) *registration.Service {
	return registration.New(registrationStore{service: service})
}
