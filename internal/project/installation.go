package project

import (
	"context"
	"database/sql"
	"fmt"
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
