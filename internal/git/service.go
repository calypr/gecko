package git

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v87/github"
)

func (service *GitService) githubClient(accessToken string) (*github.Client, error) {
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

func (service *GitService) EnsureDataDir() error {
	if err := os.MkdirAll(service.config.DataDir, 0o755); err != nil {
		return fmt.Errorf("create git data dir: %w", err)
	}
	return nil
}

func (service *GitService) MirrorPathForIdentity(identity GitRepositoryIdentity) string {
	return filepath.Join(service.config.DataDir, sanitizePathPart(identity.Host), sanitizePathPart(identity.Owner), sanitizePathPart(identity.Repo)+".git")
}

func (service *GitService) FetchRepositoryMetadata(ctx context.Context, accessToken string, identity GitRepositoryIdentity) (*GitHubRepositoryMetadata, error) {
	if service.githubAPI == nil {
		return nil, fmt.Errorf("github client is not initialized")
	}
	return service.githubAPI.FetchRepositoryMetadata(ctx, accessToken, identity)
}

func (service *GitService) RefreshProject(ctx context.Context, projectID string, identity GitRepositoryIdentity, state *geckodb.GitProjectState, accessToken string) (*GitProjectRefreshResponse, *geckodb.GitProjectState, error) {
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
	repo, err := OpenRepository(state.MirrorPath)
	if err != nil {
		return nil, state, fmt.Errorf("open refreshed git mirror: %w", err)
	}
	if !RepositoryIsEmpty(repo) {
		refName, hash, err := ResolveGitReference(repo, repoMetadata.DefaultBranch, repoMetadata.DefaultBranch)
		if err != nil {
			return nil, state, fmt.Errorf("resolve refreshed git ref: %w", err)
		}
		if err := PersistRepoAnalyticsIndex(ctx, state.MirrorPath, repo, refName, hash); err != nil {
			return nil, state, fmt.Errorf("persist repo analytics index: %w", err)
		}
	}
	updated := *state
	updated.InstallationTarget = sql.NullString{String: identity.Owner, Valid: identity.Owner != ""}
	updated.InstallationTargetType = sql.NullString{String: "Organization", Valid: identity.Owner != ""}
	updated.DefaultBranch = sql.NullString{String: repoMetadata.DefaultBranch, Valid: repoMetadata.DefaultBranch != ""}
	updated.SyncState = GitSyncReady
	updated.LastRefreshedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	updated.LastError = sql.NullString{}
	return &GitProjectRefreshResponse{Success: true, ProjectID: projectID, SyncState: GitSyncReady, DefaultBranch: repoMetadata.DefaultBranch, LastFetchedRef: repoMetadata.DefaultBranch}, &updated, nil
}

func (service *GitService) StatusFromState(projectID string, organization string, project string, cfg appconfig.ProjectConfig, identity GitRepositoryIdentity, state *geckodb.GitProjectState, orgState *geckodb.GitOrganizationState) GitProjectStatusResponse {
	workflowStage := ""
	if strings.TrimSpace(cfg.SrcRepo) == "" {
		workflowStage = GitWorkflowStageAwaitingGitHubConnect
	}
	response := GitProjectStatusResponse{
		ProjectID:                 projectID,
		Organization:              organization,
		Project:                   project,
		ResourcePath:              ProgramProjectResourcePath(organization, project),
		RequestAccessResourcePath: ProgramProjectResourcePath(organization, project),
		Config:                    cfg,
		Repository:                identity,
		WorkflowStage:             workflowStage,
		InstallationState:         GitInstallationNotConnected,
		SyncState:                 GitSyncNeverSynced,
	}
	if orgState != nil {
		response.OrganizationAppInstalled = orgState.Installed
		if orgState.HTMLURL.Valid {
			response.OrganizationHTMLURL = orgState.HTMLURL.String
		}
		if orgState.RepositorySelection.Valid {
			response.OrganizationRepositorySelection = orgState.RepositorySelection.String
		}
	}
	if state == nil {
		if response.WorkflowStage == "" && response.OrganizationAppInstalled && response.OrganizationRepositorySelection == "all" {
			response.WorkflowStage = GitWorkflowStageGitHubConnected
			response.InstallationState = GitInstallationConnected
		}
		return response
	}
	if state.InstallationID.Valid || state.InstallationTarget.Valid {
		response.InstallationState = GitInstallationConnected
		response.WorkflowStage = GitWorkflowStageGitHubConnected
	}
	if state.InstallationID.Valid {
		installationID := state.InstallationID.Int64
		response.InstallationID = &installationID
	}
	if state.InstallationTarget.Valid {
		response.InstallationTarget = state.InstallationTarget.String
	}
	if state.InstallationTargetType.Valid {
		response.InstallationTargetType = state.InstallationTargetType.String
	}
	if state.SyncState != "" {
		response.SyncState = state.SyncState
	}
	if state.DefaultBranch.Valid {
		response.DefaultBranch = state.DefaultBranch.String
	}
	if state.LastRefreshedAt.Valid {
		refreshedAt := state.LastRefreshedAt.Time
		response.LastRefreshedAt = &refreshedAt
	}
	if state.LastError.Valid {
		response.LastError = state.LastError.String
	}
	if state.MirrorPath != "" {
		if info, err := os.Stat(state.MirrorPath); err == nil && info.IsDir() {
			response.MirrorReady = true
		}
	}
	return response
}

func OrganizationConfigurationState(appInstalled bool, configuredProjects int, totalProjects int) string {
	switch {
	case !appInstalled:
		return "not_connected"
	case totalProjects == 0:
		return "connected"
	case configuredProjects <= 0:
		return "installed_unconfigured"
	case configuredProjects < totalProjects:
		return "partially_configured"
	default:
		return "connected"
	}
}
func (service *GitService) RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error) {
	if service.fenceAPI == nil {
		return "", fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestInstallationURL(ctx, authorizationHeader, owner, redirectPath)
}

func (service *GitService) ResolveTargetAndRepositoryIDs(ctx context.Context, identity GitRepositoryIdentity) (int64, int64, error) {
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

func (service *GitService) publicGitHubClient() (*github.Client, error) {
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

func (service *GitService) RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (GitRepositoryInstallationStatus, error) {
	if service.fenceAPI == nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
}

func (service *GitService) ListInstallationRepositories(ctx context.Context, authorizationHeader string, organization string, owner string, installationID int64) ([]GitHubInstallationRepository, error) {
	if service.fenceAPI == nil {
		return nil, fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.ListInstallationRepositories(ctx, authorizationHeader, organization, owner, installationID)
}

func (service *GitService) RequestInstallationStatus(ctx context.Context, authorizationHeader string, organization string, identity GitRepositoryIdentity) (GitRepositoryInstallationStatus, error) {
	if service.fenceAPI == nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestInstallationStatus(ctx, authorizationHeader, organization, identity)
}

func (service *GitService) RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity GitRepositoryIdentity, access string) (string, error) {
	if service.fenceAPI == nil {
		return "", fmt.Errorf("fence client is not initialized")
	}
	return service.fenceAPI.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, access)
}
