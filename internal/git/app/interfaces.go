package app

import (
	"context"

	"github.com/calypr/gecko/internal/git"
)

type FenceBroker interface {
	OrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (git.GitRepositoryInstallationStatus, error)
	InstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity git.GitRepositoryIdentity, access string) (string, error)
}

type GitHubInspector interface {
	RepositoryMetadata(ctx context.Context, accessToken string, identity git.GitRepositoryIdentity) (*git.GitHubRepositoryMetadata, error)
}

type StorageBucket struct {
	Bucket    string
	Provider  string
	Endpoint  string
	Region    string
	Resources []string
}

type StorageConfig struct {
	Bucket              string
	Provider            string
	Endpoint            string
	Region              string
	AccessKey           string
	SecretKey           string
	Organization        string
	ProjectID           string
	Path                string
	PathPrefix          string
	OrganizationSubPath string
	ProjectSubPath      string
}

type StorageManager interface {
	ListBuckets(ctx context.Context, authorizationHeader string) (map[string]StorageBucket, error)
	PutBucket(ctx context.Context, authorizationHeader string, config StorageConfig) error
	AddScope(ctx context.Context, authorizationHeader string, config StorageConfig) error
	CleanupProject(ctx context.Context, authorizationHeader string, organization string, project string) error
}
