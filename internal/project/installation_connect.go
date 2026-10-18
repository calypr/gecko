package project

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/repo"
)

func (service *InstallationService) InitConnect(ctx context.Context, organization string, authorizationHeader string, redirectPath string, repositoryFullName string) (string, error) {
	redirectURL, err := service.repository.RequestInstallationURL(ctx, authorizationHeader, organization, redirectPath)
	if err != nil {
		return "", integrationError(err, map[string]any{"organization": organization}, "failed to request GitHub App install URL from Fence")
	}
	if strings.TrimSpace(repositoryFullName) == "" {
		return redirectURL, nil
	}
	identity, err := ParseRequestedRepositoryIdentity(repositoryFullName)
	if err != nil {
		return "", domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("invalid_request"),
			http.StatusBadRequest,
			fmt.Sprintf("invalid repository_full_name %q: %s", repositoryFullName, err),
			map[string]any{"organization": organization},
		)
	}
	targetID, repoID, resolveErr := service.repository.ResolveTargetAndRepositoryIDs(ctx, identity)
	if resolveErr != nil {
		log.Printf("WARNING: skipping GitHub install redirect optimization for %s/%s: %v", identity.Owner, identity.Repo, resolveErr)
		if settingsURL, ok := service.organizationInstallationSettingsURL(ctx, authorizationHeader, organization, identity.Owner, redirectURL); ok {
			redirectURL = settingsURL
		}
		return redirectURL, nil
	}
	return DecorateInstallationRedirectURL(redirectURL, targetID, repoID), nil
}

func (service *InstallationService) organizationInstallationSettingsURL(ctx context.Context, authorizationHeader string, organization string, owner string, redirectURL string) (string, bool) {
	installation, err := service.repository.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
	if err != nil {
		log.Printf("WARNING: failed to load GitHub organization installation for %s: %v", owner, err)
		return "", false
	}
	if !installation.Installed || installation.InstallationID == nil || *installation.InstallationID <= 0 {
		return "", false
	}
	if appInstallationURL := AppInstallationRedirectURL(strings.TrimSpace(redirectURL), *installation.InstallationID); appInstallationURL != "" {
		return appInstallationURL, true
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", false
	}
	return fmt.Sprintf("https://github.com/organizations/%s/settings/installations/%d", owner, *installation.InstallationID), true
}

func (service *InstallationService) ConnectOrganization(ctx context.Context, organization string, authorizationHeader string, githubOwner string, installationID int64) ([]domain.GitHubInstallationRepository, error) {
	installation, err := service.repository.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, githubOwner)
	if err != nil {
		return nil, integrationError(err, map[string]any{"organization": organization, "github_owner": githubOwner}, "failed to load GitHub organization installation status")
	}
	if !installation.Installed || installation.InstallationID == nil || *installation.InstallationID <= 0 {
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("conflict"),
			http.StatusConflict,
			"organization is not connected to the GitHub App",
			map[string]any{"organization": organization, "github_owner": githubOwner},
		)
	}
	if err := service.PersistConnectedOrganizationState(ctx, organization, githubOwner, installation); err != nil {
		return nil, err
	}
	return service.ListConnectedInstallationRepositories(ctx, authorizationHeader, organization, "", githubOwner, installationID)
}

func (service *InstallationService) ConnectProject(ctx context.Context, organization string, project string, authorizationHeader string, repositoryFullName string) (string, error) {
	projectID := organization + "/" + project
	projectCfg, err := service.LoadProjectConfig(ctx, projectID)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(repositoryFullName) == "" {
		if err := service.UnbindProjectRepository(ctx, projectID, projectCfg); err != nil {
			return "", err
		}
		return "disconnected", nil
	}
	orgState, err := service.LoadOrBootstrapConnectedOrganizationState(ctx, authorizationHeader, organization, project, repositoryFullName)
	if err != nil {
		return "", err
	}
	owner := organization
	if orgState.InstallationTarget.Valid {
		owner = strings.TrimSpace(orgState.InstallationTarget.String)
	}
	repositories, err := service.ListConnectedInstallationRepositories(ctx, authorizationHeader, organization, project, owner, orgState.InstallationID.Int64)
	if err != nil {
		return "", err
	}
	selectedIdentity, found, err := NormalizeInstallationRepository(repositoryFullName, repositories, organization, project)
	if err != nil {
		return "", err
	}
	if !found {
		return "", domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("conflict"),
			http.StatusConflict,
			fmt.Sprintf("GitHub App is not connected to repository %q", repositoryFullName),
			map[string]any{"organization": organization, "project": project, "repository": repositoryFullName},
		)
	}
	if err := service.BindProjectRepository(ctx, projectID, projectCfg, orgState, selectedIdentity); err != nil {
		return "", err
	}
	return "connected", nil
}

func (service *InstallationService) UpdateProject(ctx context.Context, resolved ResolvedProject, authorizationHeader string) (*repo.ProjectRefreshResponse, error) {
	state, err := service.repository.LoadProjectState(ctx, service.db, resolved.ID, resolved.Identity)
	if err != nil {
		return nil, projectRepositoryError(resolved.ID, "failed to read git state", err)
	}
	if state == nil {
		state = &geckodb.GitProjectState{
			ProjectID:  resolved.ID,
			RepoHost:   resolved.Identity.Host,
			RepoOwner:  resolved.Identity.Owner,
			RepoName:   resolved.Identity.Repo,
			MirrorPath: service.repository.MirrorPathForIdentity(resolved.Identity),
			SyncState:  GitSyncNeverSynced,
		}
	}
	accessToken, err := service.repository.RequestInstallationToken(ctx, authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, "read")
	if err != nil {
		return nil, integrationError(err, map[string]any{"project_id": resolved.ID, "repository": resolved.Config.SrcRepo}, "failed to exchange GitHub token with Fence")
	}
	state.SyncState = GitSyncUpdating
	state.LastError = sql.NullString{}
	if err := geckodb.UpsertGitProjectStateContext(ctx, service.db, *state); err != nil {
		return nil, projectRepositoryError(resolved.ID, "failed to persist updating git state", err)
	}
	refreshResponse, updatedState, err := service.repository.RefreshProject(ctx, resolved.ID, resolved.Identity, state, accessToken)
	if err != nil {
		state.SyncState = GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectStateContext(ctx, service.db, *state)
		return nil, domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type("integration_error"),
			http.StatusBadGateway,
			fmt.Sprintf("failed to update git checkout for %s/%s: %s", resolved.Organization, resolved.Name, err),
			map[string]any{"project_id": resolved.ID, "repository": resolved.Config.SrcRepo},
		)
	}
	if err := geckodb.UpsertGitProjectStateContext(ctx, service.db, *updatedState); err != nil {
		return nil, projectRepositoryError(resolved.ID, "failed to persist updated git state", err)
	}
	return refreshResponse, nil
}
