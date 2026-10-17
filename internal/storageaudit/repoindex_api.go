package storageaudit

import (
	"context"

	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func RepoAnalyticsIndexPath(mirrorPath string) string {
	return repoindex.RepoAnalyticsIndexPath(mirrorPath)
}

func PersistRepoAnalyticsIndex(ctx context.Context, mirrorPath string, repo *gogit.Repository, ref string, hash plumbing.Hash) error {
	return repoindex.PersistRepoAnalyticsIndex(ctx, mirrorPath, repo, ref, hash)
}
