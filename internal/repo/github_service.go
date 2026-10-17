package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v87/github"
	"github.com/jmoiron/sqlx"
)

func (service *Service) githubClient(accessToken string) (*github.Client, error) {
	options := []github.ClientOptionsFunc{
		github.WithAuthToken(accessToken),
		github.WithHTTPClient(service.client),
	}
	if strings.TrimRight(service.config.GitHubAPIBase, "/") != "https://api.github.com" {
		apiBase := strings.TrimRight(service.config.GitHubAPIBase, "/") + "/"
		options = append(options, github.WithEnterpriseURLs(apiBase, apiBase))
	}
	client, err := github.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create github client: %w", err)
	}
	return client, nil
}

func (service *Service) EnsureDataDir() error {
	if err := os.MkdirAll(service.config.DataDir, 0o755); err != nil {
		return fmt.Errorf("create git data dir: %w", err)
	}
	return nil
}

func (service *Service) MirrorPathForIdentity(identity Identity) string {
	return filepath.Join(service.config.DataDir, sanitizePathPart(identity.Host), sanitizePathPart(identity.Owner), sanitizePathPart(identity.Repo)+".git")
}

func (service *Service) LoadProjectState(ctx context.Context, db *sqlx.DB, projectID string, identity Identity) (*geckodb.GitProjectState, error) {
	state, err := geckodb.GitProjectStateByProjectIDContext(ctx, db, projectID)
	if err != nil || state == nil {
		return state, err
	}
	expectedMirrorPath := service.MirrorPathForIdentity(identity)
	if state.RepoHost == identity.Host &&
		state.RepoOwner == identity.Owner &&
		state.RepoName == identity.Repo &&
		state.MirrorPath == expectedMirrorPath {
		return state, nil
	}
	state.RepoHost = identity.Host
	state.RepoOwner = identity.Owner
	state.RepoName = identity.Repo
	state.MirrorPath = expectedMirrorPath
	if err := geckodb.UpsertGitProjectStateContext(ctx, db, *state); err != nil {
		return nil, fmt.Errorf("persist git project state mirror path: %w", err)
	}
	return state, nil
}

func (service *Service) FetchRepositoryMetadata(ctx context.Context, accessToken string, identity Identity) (*domain.GitHubRepositoryMetadata, error) {
	if service.githubAPI == nil {
		return nil, fmt.Errorf("github client is not initialized")
	}
	return service.githubAPI.FetchRepositoryMetadata(ctx, accessToken, identity)
}

func (service *Service) RefreshProject(ctx context.Context, projectID string, identity Identity, state *geckodb.GitProjectState, accessToken string) (*ProjectRefreshResponse, *geckodb.GitProjectState, error) {
	repoMetadata, err := service.FetchRepositoryMetadata(ctx, accessToken, identity)
	if err != nil {
		return nil, state, err
	}
	if state == nil {
		state = &geckodb.GitProjectState{ProjectID: projectID}
	}
	if state.MirrorPath == "" {
		state.MirrorPath = service.MirrorPathForIdentity(identity)
	}
	cloneURL := fmt.Sprintf("https://%s/%s/%s.git", identity.Host, identity.Owner, identity.Repo)
	if err := SyncRepositoryMirror(ctx, cloneURL, state.MirrorPath, &githttp.BasicAuth{Username: "x-access-token", Password: accessToken}); err != nil {
		return nil, state, err
	}
	mirror, err := OpenRepository(state.MirrorPath)
	if err != nil {
		return nil, state, fmt.Errorf("open refreshed git mirror: %w", err)
	}
	if !RepositoryIsEmpty(mirror) {
		refName, hash, err := ResolveGitReference(mirror, repoMetadata.DefaultBranch, repoMetadata.DefaultBranch)
		if err != nil {
			return nil, state, fmt.Errorf("resolve refreshed git ref: %w", err)
		}
		if service.config.RepoAnalyticsIndexer != nil {
			if err := service.config.RepoAnalyticsIndexer(ctx, state.MirrorPath, mirror, refName, hash); err != nil {
				return nil, state, fmt.Errorf("persist repo analytics index: %w", err)
			}
		}
	}
	updated := *state
	updated.InstallationTarget = sql.NullString{String: identity.Owner, Valid: identity.Owner != ""}
	updated.InstallationTargetType = sql.NullString{String: "Organization", Valid: identity.Owner != ""}
	updated.DefaultBranch = sql.NullString{String: repoMetadata.DefaultBranch, Valid: repoMetadata.DefaultBranch != ""}
	updated.SyncState = SyncStateReady
	updated.LastRefreshedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	updated.LastError = sql.NullString{}
	return &ProjectRefreshResponse{Success: true, ProjectID: projectID, SyncState: SyncStateReady, DefaultBranch: repoMetadata.DefaultBranch, LastFetchedRef: repoMetadata.DefaultBranch}, &updated, nil
}

func (service *Service) RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error) {
	if service.fenceAPI == nil {
		return "", fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestInstallationURL(ctx, authorizationHeader, owner, redirectPath)
}

func (service *Service) ResolveTargetAndRepositoryIDs(ctx context.Context, identity Identity) (int64, int64, error) {
	client, err := service.publicGitHubClient()
	if err != nil {
		return 0, 0, err
	}

	repo, _, err := client.Repositories.Get(ctx, identity.Owner, identity.Repo)
	if err != nil {
		return 0, 0, fmt.Errorf("github repository lookup failed for %s/%s: %w", identity.Owner, identity.Repo, err)
	}

	if repo.GetOwner() == nil {
		return 0, 0, fmt.Errorf("github repository %s/%s has no owner details", identity.Owner, identity.Repo)
	}

	return repo.GetOwner().GetID(), repo.GetID(), nil
}

func (service *Service) publicGitHubClient() (*github.Client, error) {
	options := []github.ClientOptionsFunc{
		github.WithHTTPClient(service.client),
	}
	if strings.TrimRight(service.config.GitHubAPIBase, "/") != "https://api.github.com" {
		apiBase := strings.TrimRight(service.config.GitHubAPIBase, "/") + "/"
		options = append(options, github.WithEnterpriseURLs(apiBase, apiBase))
	}
	client, err := github.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create public github client: %w", err)
	}
	return client, nil
}

func (service *Service) RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (domain.GitRepositoryInstallationStatus, error) {
	if service.fenceAPI == nil {
		return domain.GitRepositoryInstallationStatus{}, fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
}

func (service *Service) ListInstallationRepositories(ctx context.Context, authorizationHeader string, organization string, owner string, installationID int64) ([]domain.GitHubInstallationRepository, error) {
	if service.fenceAPI == nil {
		return nil, fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.ListInstallationRepositories(ctx, authorizationHeader, organization, owner, installationID)
}

func (service *Service) RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity Identity, access string) (string, error) {
	if service.fenceAPI == nil {
		return "", fmt.Errorf("fence client is not initialized")
	}
	started := time.Now()
	log.Printf("INFO: github_installation_token_request_start organization=%s project=%s owner=%s repo=%s access=%s", organization, project, identity.Owner, identity.Repo, access)
	token, err := service.fenceAPI.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, access)
	if err != nil {
		log.Printf("INFO: github_installation_token_request_done organization=%s project=%s owner=%s repo=%s access=%s token_received=false duration_ms=%d error=%q", organization, project, identity.Owner, identity.Repo, access, time.Since(started).Milliseconds(), err.Error())
		return "", err
	}
	log.Printf("INFO: github_installation_token_request_done organization=%s project=%s owner=%s repo=%s access=%s token_received=%t token_fingerprint=%s token_length=%d duration_ms=%d", organization, project, identity.Owner, identity.Repo, access, strings.TrimSpace(token) != "", githubAccessTokenFingerprint(token), githubAccessTokenLength(token), time.Since(started).Milliseconds())
	return token, nil
}
