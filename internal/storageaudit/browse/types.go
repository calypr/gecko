package browse

import (
	"context"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

type JoinState struct {
	Index             *repoindex.Index
	Inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]record.State
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type Store interface {
	LoadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*JoinState, error)
	LoadRepoIndex(ctx context.Context, mirrorPath string, ref string, repo *gogit.Repository, hash plumbing.Hash, timings *model.StorageFolderTimings) (*repoindex.Index, error)
	ListProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]record.State, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
}

type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}
