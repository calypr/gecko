package github

import (
	"context"

	"github.com/calypr/gecko/internal/git"
	gitapp "github.com/calypr/gecko/internal/git/app"
)

type Inspector struct {
	service *git.GitService
}

func NewInspector(service *git.GitService) gitapp.GitHubInspector {
	return &Inspector{service: service}
}

func (inspector *Inspector) RepositoryMetadata(ctx context.Context, accessToken string, identity git.GitRepositoryIdentity) (*git.GitHubRepositoryMetadata, error) {
	return inspector.service.FetchRepositoryMetadata(ctx, accessToken, identity)
}
