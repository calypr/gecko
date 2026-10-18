package project

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/repo"
	"github.com/jmoiron/sqlx"
)

// InstallationRepositoryService is the GitHub-facing slice needed to connect a
// project to an installation. Keeping it narrow makes the service easy to fake.
type InstallationRepositoryService interface {
	RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error)
	RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (domain.GitRepositoryInstallationStatus, error)
	ListInstallationRepositories(ctx context.Context, authorizationHeader string, organization string, owner string, installationID int64) ([]domain.GitHubInstallationRepository, error)
	ResolveTargetAndRepositoryIDs(ctx context.Context, identity repo.Identity) (int64, int64, error)
	LoadProjectState(ctx context.Context, db *sqlx.DB, projectID string, identity repo.Identity) (*geckodb.GitProjectState, error)
	RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity repo.Identity, access string) (string, error)
	RefreshProject(ctx context.Context, projectID string, identity repo.Identity, state *geckodb.GitProjectState, accessToken string) (*repo.ProjectRefreshResponse, *geckodb.GitProjectState, error)
	MirrorPathForIdentity(identity repo.Identity) string
}

type InstallationService struct {
	db         *sqlx.DB
	repository InstallationRepositoryService
}

func NewInstallationService(db *sqlx.DB, repository InstallationRepositoryService) *InstallationService {
	return &InstallationService{db: db, repository: repository}
}

func (service *InstallationService) PersistConnectedOrganizationState(ctx context.Context, organization string, githubOwner string, installation domain.GitRepositoryInstallationStatus) error {
	now := time.Now().UTC()
	state := geckodb.GitOrganizationState{
		Organization: organization,
		Installed:    installation.Installed,
		UpdatedAt:    now,
		LastSeenAt:   sql.NullTime{Time: now, Valid: true},
		ConfiguredAt: sql.NullTime{Time: now, Valid: installation.Installed},
		LastError:    sql.NullString{},
	}
	if installation.InstallationID != nil && *installation.InstallationID > 0 {
		state.InstallationID = sql.NullInt64{Int64: *installation.InstallationID, Valid: true}
	}
	target := strings.TrimSpace(installation.Target)
	if target == "" {
		target = strings.TrimSpace(githubOwner)
	}
	if target != "" {
		state.InstallationTarget = sql.NullString{String: target, Valid: true}
	}
	targetType := strings.TrimSpace(installation.TargetType)
	if targetType == "" && target != "" {
		targetType = "Organization"
	}
	if targetType != "" {
		state.InstallationTargetType = sql.NullString{String: targetType, Valid: true}
	}
	if strings.TrimSpace(installation.HTMLURL) != "" {
		state.HTMLURL = sql.NullString{String: strings.TrimSpace(installation.HTMLURL), Valid: true}
	}
	if strings.TrimSpace(installation.RepositorySelection) != "" {
		state.RepositorySelection = sql.NullString{String: strings.TrimSpace(installation.RepositorySelection), Valid: true}
	}
	if err := geckodb.UpsertGitOrganizationStateContext(ctx, service.db, state); err != nil {
		return domain.NewTypedError(
			domain.ErrorKindDatabase,
			apierror.TypeDatabaseError,
			http.StatusInternalServerError,
			fmt.Sprintf("failed to persist organization git state: %s", err),
			map[string]any{"organization": organization, "github_owner": githubOwner},
		)
	}
	return nil
}

func (service *InstallationService) LoadProjectConfig(ctx context.Context, projectID string) (appconfig.ProjectConfig, error) {
	var projectCfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGenericContext(ctx, service.db, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		if err == sql.ErrNoRows {
			return appconfig.ProjectConfig{}, domain.NewTypedError(
				domain.ErrorKindNotFound,
				apierror.Type("not_found"),
				http.StatusNotFound,
				fmt.Sprintf("no project config found for %s", projectID),
				map[string]any{"project_id": projectID},
			)
		}
		return appconfig.ProjectConfig{}, domain.NewTypedError(
			domain.ErrorKindDatabase,
			apierror.TypeDatabaseError,
			http.StatusInternalServerError,
			fmt.Sprintf("failed to load project config: %s", err),
			map[string]any{"project_id": projectID},
		)
	}
	return projectCfg, nil
}

func (service *InstallationService) LoadConnectedOrganizationState(ctx context.Context, organization string, project string) (*geckodb.GitOrganizationState, error) {
	orgState, err := geckodb.GitOrganizationStateByOrganizationContext(ctx, service.db, organization)
	if err != nil {
		return nil, domain.NewTypedError(
			domain.ErrorKindDatabase,
			apierror.TypeDatabaseError,
			http.StatusInternalServerError,
			fmt.Sprintf("failed to load organization git state: %s", err),
			map[string]any{"organization": organization},
		)
	}
	if orgState == nil || !orgState.Installed || !orgState.InstallationID.Valid {
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("conflict"),
			http.StatusConflict,
			"organization is not connected to the GitHub App",
			map[string]any{"organization": organization, "project": project},
		)
	}
	return orgState, nil
}

func (service *InstallationService) LoadOrBootstrapConnectedOrganizationState(ctx context.Context, authorizationHeader string, organization string, project string, repositoryFullName string) (*geckodb.GitOrganizationState, error) {
	orgState, err := service.LoadConnectedOrganizationState(ctx, organization, project)
	if err == nil {
		return orgState, nil
	}
	appErr, ok := err.(*domain.Error)
	if !ok || appErr.StatusCode != http.StatusConflict {
		return nil, err
	}

	identity, parseErr := ParseRequestedRepositoryIdentity(repositoryFullName)
	if parseErr != nil {
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("invalid_request"),
			http.StatusBadRequest,
			fmt.Sprintf("invalid repository_full_name %q: %s", repositoryFullName, parseErr),
			map[string]any{"organization": organization, "project": project},
		)
	}
	installation, installErr := service.repository.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, identity.Owner)
	if installErr != nil {
		if statusErr, ok := installErr.(*domain.HTTPStatusError); ok {
			return nil, domain.NewTypedError(
				domain.ErrorKindIntegration,
				apierror.Type(statusErr.Code),
				statusErr.StatusCode,
				statusErr.Message,
				map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner},
			)
		}
		return nil, domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type("integration_error"),
			http.StatusBadGateway,
			fmt.Sprintf("failed to load GitHub organization installation status: %s", installErr),
			map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner},
		)
	}
	if !installation.Installed || installation.InstallationID == nil || *installation.InstallationID <= 0 {
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("conflict"),
			http.StatusConflict,
			"organization is not connected to the GitHub App",
			map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner},
		)
	}
	if err := service.PersistConnectedOrganizationState(ctx, organization, identity.Owner, installation); err != nil {
		if appErr, ok := err.(*domain.Error); ok {
			appErr.Details = map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner}
		}
		return nil, err
	}
	return service.LoadConnectedOrganizationState(ctx, organization, project)
}

func (service *InstallationService) ListConnectedInstallationRepositories(ctx context.Context, authorizationHeader string, organization string, project string, owner string, installationID int64) ([]domain.GitHubInstallationRepository, error) {
	repositories, err := service.repository.ListInstallationRepositories(ctx, authorizationHeader, organization, owner, installationID)
	if err != nil {
		details := map[string]any{"organization": organization, "installation_id": installationID}
		if project != "" {
			details["project"] = project
		}
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			return nil, domain.NewTypedError(
				domain.ErrorKindIntegration,
				apierror.Type(statusErr.Code),
				statusErr.StatusCode,
				statusErr.Message,
				details,
			)
		}
		return nil, domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type("integration_error"),
			http.StatusBadGateway,
			fmt.Sprintf("failed to list GitHub installation repositories: %s", err),
			details,
		)
	}
	return repositories, nil
}

func (service *InstallationService) BindProjectRepository(ctx context.Context, projectID string, projectCfg appconfig.ProjectConfig, orgState *geckodb.GitOrganizationState, identity repo.Identity) error {
	projectState, err := geckodb.GitProjectStateByProjectIDContext(ctx, service.db, projectID)
	if err != nil {
		return projectRepositoryError(projectID, "failed to bind project repository", fmt.Errorf("load project git state: %w", err))
	}
	if projectState == nil {
		projectState = &geckodb.GitProjectState{ProjectID: projectID}
	}
	projectCfg.SrcRepo = identity.URL
	projectState.RepoHost = identity.Host
	projectState.RepoOwner = identity.Owner
	projectState.RepoName = identity.Repo
	projectState.InstallationID = orgState.InstallationID
	projectState.InstallationTarget = orgState.InstallationTarget
	projectState.InstallationTargetType = orgState.InstallationTargetType
	projectState.MirrorPath = service.repository.MirrorPathForIdentity(identity)
	projectState.SyncState = GitSyncNeverSynced
	projectState.DefaultBranch = sql.NullString{}
	projectState.LastRefreshedAt = sql.NullTime{}
	projectState.LastError = sql.NullString{}

	tx, err := service.db.BeginTxx(ctx, nil)
	if err != nil {
		return projectRepositoryError(projectID, "failed to bind project repository", fmt.Errorf("begin project repository bind transaction: %w", err))
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := geckodb.ConfigPUTGenericTxContext(ctx, tx, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		return projectRepositoryError(projectID, "failed to bind project repository", fmt.Errorf("update project config: %w", err))
	}
	if err := geckodb.UpsertGitProjectStateTxContext(ctx, tx, *projectState); err != nil {
		return projectRepositoryError(projectID, "failed to bind project repository", fmt.Errorf("update project git state: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return projectRepositoryError(projectID, "failed to bind project repository", fmt.Errorf("commit project repository bind transaction: %w", err))
	}
	return nil
}

func (service *InstallationService) UnbindProjectRepository(ctx context.Context, projectID string, projectCfg appconfig.ProjectConfig) error {
	projectCfg.SrcRepo = ""

	tx, err := service.db.BeginTxx(ctx, nil)
	if err != nil {
		return projectRepositoryError(projectID, "failed to clear project repository binding", fmt.Errorf("begin project repository unbind transaction: %w", err))
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := geckodb.ConfigPUTGenericTxContext(ctx, tx, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		return projectRepositoryError(projectID, "failed to clear project repository binding", fmt.Errorf("update project config: %w", err))
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM config_schema.git_project_state WHERE project_id = $1`, projectID); err != nil {
		return projectRepositoryError(projectID, "failed to clear project repository binding", fmt.Errorf("delete project git state: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return projectRepositoryError(projectID, "failed to clear project repository binding", fmt.Errorf("commit project repository unbind transaction: %w", err))
	}
	return nil
}

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

func integrationError(err error, details map[string]any, fallback string) error {
	if statusErr, ok := err.(*domain.HTTPStatusError); ok {
		return domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type(statusErr.Code),
			statusErr.StatusCode,
			statusErr.Message,
			details,
		)
	}
	return domain.NewTypedError(
		domain.ErrorKindIntegration,
		apierror.Type("integration_error"),
		http.StatusBadGateway,
		fmt.Sprintf("%s: %s", fallback, err),
		details,
	)
}

func projectRepositoryError(projectID string, operation string, err error) error {
	return domain.NewTypedError(
		domain.ErrorKindDatabase,
		apierror.TypeDatabaseError,
		http.StatusInternalServerError,
		fmt.Sprintf("%s: %s", operation, err),
		map[string]any{"project_id": projectID},
	)
}

func NormalizeInstallationRepository(repositoryFullName string, repositories []domain.GitHubInstallationRepository, organization string, project string) (repo.Identity, bool, error) {
	requested := strings.TrimSpace(repositoryFullName)
	for _, repository := range repositories {
		if !strings.EqualFold(repository.FullName, requested) {
			continue
		}
		repoURL := strings.TrimSpace(repository.CloneURL)
		if repoURL == "" {
			repoURL = strings.TrimSpace(repository.HTMLURL)
		}
		if repoURL == "" {
			repoURL = "https://github.com/" + strings.TrimSpace(repository.FullName)
		}
		identity, err := repo.ParseIdentity(repoURL)
		if err != nil {
			return repo.Identity{}, false, domain.NewTypedError(
				domain.ErrorKindIntegration,
				apierror.Type("integration_error"),
				http.StatusBadGateway,
				fmt.Sprintf("failed to normalize installation repository %q: %s", repository.FullName, err),
				map[string]any{"organization": organization, "project": project, "repository": repository.FullName},
			)
		}
		return identity, true, nil
	}
	return repo.Identity{}, false, nil
}

func ParseRequestedRepositoryIdentity(repositoryFullName string) (repo.Identity, error) {
	repoURL := strings.TrimSpace(repositoryFullName)
	if !strings.HasPrefix(repoURL, "http://") && !strings.HasPrefix(repoURL, "https://") && !strings.HasPrefix(repoURL, "git@") {
		repoURL = "https://github.com/" + repoURL
	}
	return repo.ParseIdentity(repoURL)
}

func DecorateInstallationRedirectURL(redirectURL string, targetID int64, repoID int64) string {
	hasNew := strings.Contains(redirectURL, "/installations/new")
	hasSelectTarget := strings.Contains(redirectURL, "/installations/select_target")
	if !hasNew && !hasSelectTarget {
		return redirectURL
	}
	if hasNew {
		redirectURL = strings.Replace(redirectURL, "/installations/new", "/installations/new/permissions", 1)
	} else {
		redirectURL = strings.Replace(redirectURL, "/installations/select_target", "/installations/new/permissions", 1)
	}
	separator := "?"
	if strings.Contains(redirectURL, "?") {
		separator = "&"
	}
	redirectURL = fmt.Sprintf("%s%ssuggested_target_id=%d", redirectURL, separator, targetID)
	return fmt.Sprintf("%s&repository_ids[]=%d", redirectURL, repoID)
}

func AppInstallationRedirectURL(redirectURL string, installationID int64) string {
	if installationID <= 0 || strings.TrimSpace(redirectURL) == "" {
		return ""
	}
	request, err := http.NewRequest(http.MethodGet, redirectURL, nil)
	if err != nil || request.URL == nil {
		return ""
	}
	pathParts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(pathParts) < 2 || pathParts[0] != "apps" || strings.TrimSpace(pathParts[1]) == "" {
		return ""
	}
	return fmt.Sprintf("%s://%s/apps/%s/installations/%d", request.URL.Scheme, request.URL.Host, pathParts[1], installationID)
}
