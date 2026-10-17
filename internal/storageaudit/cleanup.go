package storageaudit

import (
	"context"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/cleanup"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) BuildStorageCleanupAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, selectedRepoPaths []string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, checkStorage bool) (*CleanupAuditResponse, *cleanup.AuditModel, error) {
	return service.cleanupSvc.BuildStorageCleanupAudit(ctx, authorizationHeader, organization, project, ref, gitSubpath, selectedRepoPaths, mirrorPath, repo, hash, checkStorage)
}

func (service *StorageAnalyticsService) ApplyStorageCleanup(ctx context.Context, authorizationHeader string, organization string, project string, selectedRepoPaths []string, selectedActions []CleanupApplyAction, selectedFindings []CleanupApplyFinding, deleteRepoOrphans bool, deleteStaleDuplicates bool, deleteBucketOnlyObjects bool, repairBrokenBucketMappings bool, dryRun bool) (*CleanupApplyResponse, error) {
	return service.cleanupSvc.ApplyStorageCleanup(ctx, authorizationHeader, organization, project, selectedRepoPaths, selectedActions, selectedFindings, deleteRepoOrphans, deleteStaleDuplicates, deleteBucketOnlyObjects, repairBrokenBucketMappings, dryRun)
}

func newCleanupService(service *StorageAnalyticsService) *cleanup.Service {
	return cleanup.New(cleanupStore{service: service}, service.chainSvc)
}

type cleanupStore struct {
	service *StorageAnalyticsService
}

func (store cleanupStore) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return store.service.storage.ListProjectAuditRecords(ctx, authorizationHeader, organization, project, "")
}

func (store cleanupStore) BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return store.service.storage.BulkConfirmStorageObjects(ctx, authorizationHeader, items)
}

func (store cleanupStore) BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error {
	return store.service.storage.BulkUpdateAccessMethods(ctx, authorizationHeader, updates)
}

func (store cleanupStore) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	return store.service.storage.BulkDeleteObjects(ctx, authorizationHeader, objectIDs, deleteStorageData)
}

func (store cleanupStore) DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error) {
	return store.service.storage.DeleteProjectBucketObjects(ctx, authorizationHeader, organization, project, objectURLs)
}

func (store cleanupStore) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	return store.service.storage.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, checksums)
}

func (store cleanupStore) EvictProjectJoinCache(organization string, project string) {
	store.service.evictProjectJoinCache(organization, project)
}

func (store cleanupStore) EvictProjectAuditRecordCache(organization string, project string) {
	store.service.evictProjectAuditRecordCache(organization, project)
}

func (store cleanupStore) EvictProjectChainInputCache(organization string, project string) {
	store.service.chainSvc.EvictProjectChainInputCache(organization, project)
}
