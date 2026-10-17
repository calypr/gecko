package storageaudit

import (
	"context"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/browse"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) BuildStorageSummary(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*StorageSummaryResponse, error) {
	return service.browseSvc.BuildStorageSummary(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
}

func (service *StorageAnalyticsService) BuildStorageChildren(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string) (*StorageChildrenResponse, error) {
	return service.browseSvc.BuildStorageChildren(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, limit, sortBy, sortOrder, cursor)
}

func (service *StorageAnalyticsService) BuildStorageFolder(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string, summaryMode string, forceRefresh bool, timings *StorageFolderTimings) (*StorageFolderResponse, error) {
	return service.browseSvc.BuildStorageFolder(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, limit, sortBy, sortOrder, cursor, summaryMode, forceRefresh, timings)
}

func (service *StorageAnalyticsService) BuildProjectDiffAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*ProjectDiffAuditResponse, error) {
	return service.browseSvc.BuildProjectDiffAudit(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash)
}

type browseStore struct {
	service *StorageAnalyticsService
}

func newBrowseService(service *StorageAnalyticsService) *browse.Service {
	return browse.New(browseStore{service: service})
}

func (store browseStore) LoadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*browse.JoinState, error) {
	index, inventory, recordsByChecksum, usageByObjectID, err := store.service.loadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, forceRefresh)
	if err != nil {
		return nil, err
	}
	return &browse.JoinState{
		Index:             index,
		Inventory:         inventory,
		RecordsByChecksum: recordsByChecksum,
		UsageByObjectID:   usageByObjectID,
	}, nil
}

func (store browseStore) LoadRepoIndex(ctx context.Context, mirrorPath string, ref string, repo *gogit.Repository, hash plumbing.Hash, timings *model.StorageFolderTimings) (*repoindex.Index, error) {
	return repoindex.LoadOrBuildRepoAnalyticsIndexWithTimings(ctx, mirrorPath, ref, repo, hash, timings)
}

func (store browseStore) ListProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]record.State, error) {
	return store.service.listProjectRecordStates(ctx, authorizationHeader, organization, project, usageByObjectID)
}

func (store browseStore) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	return store.service.storage.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, checksums)
}
