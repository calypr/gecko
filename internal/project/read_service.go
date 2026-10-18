package project

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/repo"
	gogit "github.com/go-git/go-git/v5"
	"github.com/jmoiron/sqlx"
)

type GitReadRepository interface {
	LoadProjectState(ctx context.Context, db *sqlx.DB, projectID string, identity repo.Identity) (*geckodb.GitProjectState, error)
	RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity repo.Identity, access string) (string, error)
	RefreshProject(ctx context.Context, projectID string, identity repo.Identity, state *geckodb.GitProjectState, accessToken string) (*repo.ProjectRefreshResponse, *geckodb.GitProjectState, error)
}

type ReadService struct {
	db         *sqlx.DB
	repository GitReadRepository
}

func NewReadService(db *sqlx.DB, repository GitReadRepository) *ReadService {
	return &ReadService{db: db, repository: repository}
}

type ResolvedProject struct {
	Organization string
	Name         string
	ID           string
	Config       appconfig.ProjectConfig
	Identity     repo.Identity
}

func (service *ReadService) Resolve(ctx context.Context, organization string, project string) (ResolvedProject, error) {
	projectID, cfg, err := service.loadProjectConfig(ctx, organization, project)
	if err != nil {
		return ResolvedProject{}, err
	}
	if strings.TrimSpace(cfg.SrcRepo) == "" {
		return ResolvedProject{}, domain.NewTypedError(domain.ErrorKindValidation, apierror.Type("conflict"), http.StatusConflict, fmt.Sprintf("GitHub connection has not been completed for %s", projectID), map[string]any{
			"project_id":     projectID,
			"workflow_stage": GitWorkflowStageAwaitingGitHubConnect,
		})
	}
	identity, err := repo.ParseIdentity(cfg.SrcRepo)
	if err != nil {
		return ResolvedProject{}, domain.NewTypedError(domain.ErrorKindValidation, apierror.TypeValidationFailed, http.StatusBadRequest, fmt.Sprintf("invalid src_repo for %s: %s", projectID, err), map[string]any{"project_id": projectID, "src_repo": cfg.SrcRepo})
	}
	return ResolvedProject{
		Organization: strings.TrimSpace(organization),
		Name:         strings.TrimSpace(project),
		ID:           projectID,
		Config:       cfg,
		Identity:     identity,
	}, nil
}

func (service *ReadService) Status(ctx context.Context, organization string, project string, authorizationHeader string) (GitProjectStatusResponse, error) {
	projectID, cfg, err := service.loadProjectConfig(ctx, organization, project)
	if err != nil {
		return GitProjectStatusResponse{}, err
	}
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	identity, identityErr := repo.ParseIdentity(cfg.SrcRepo)
	if identityErr != nil {
		orgState, _ := geckodb.GitOrganizationStateByOrganizationContext(ctx, service.db, organization)
		status := StatusFromState(projectID, organization, project, cfg, repo.Identity{}, nil, orgState)
		status.Accessible = true
		return status, nil
	}
	resolved := ResolvedProject{
		Organization: organization,
		Name:         project,
		ID:           projectID,
		Config:       cfg,
		Identity:     identity,
	}
	state, err := service.State(ctx, resolved)
	if err != nil {
		return GitProjectStatusResponse{}, err
	}
	if strings.TrimSpace(authorizationHeader) != "" {
		refreshCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		state, err = service.EnsureMirrorReady(refreshCtx, authorizationHeader, resolved, state, false)
		if err != nil {
			log.Printf("WARNING: failed to warm git mirror for %s: %v", projectID, err)
		}
	}
	orgState, _ := geckodb.GitOrganizationStateByOrganizationContext(ctx, service.db, organization)
	status := StatusFromState(projectID, organization, project, cfg, identity, state, orgState)
	status.Accessible = true
	return status, nil
}

func (service *ReadService) ListStatuses(ctx context.Context, allowedResources []string) ([]GitProjectStatusResponse, error) {
	states, err := geckodb.ListGitProjectStatesContext(ctx, service.db)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKindDatabase, http.StatusInternalServerError, "failed to list git state", err, nil)
	}
	projects, err := List(ctx, service.db)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKindDatabase, http.StatusInternalServerError, "failed to list projects", err, nil)
	}
	projectIDs := make([]string, 0, len(projects))
	for _, item := range projects {
		projectIDs = append(projectIDs, item.ID)
	}
	projectIDs = access.FilterProjectIDsByAllowedResources(projectIDs, allowedResources)
	allowed := make(map[string]bool, len(projectIDs))
	for _, id := range projectIDs {
		allowed[id] = true
	}
	responses := make([]GitProjectStatusResponse, 0, len(projectIDs))
	for _, item := range projects {
		if !allowed[item.ID] || item.Config == nil {
			continue
		}
		projectID := item.ID
		cfg := *item.Config
		identity, err := repo.ParseIdentity(cfg.SrcRepo)
		if err != nil {
			continue
		}
		var statePtr *geckodb.GitProjectState
		if state, ok := states[projectID]; ok {
			copyState := state
			statePtr = &copyState
		}
		orgState, _ := geckodb.GitOrganizationStateByOrganizationContext(ctx, service.db, item.Organization)
		status := StatusFromState(projectID, item.Organization, item.Name, cfg, identity, statePtr, orgState)
		if len(allowedResources) > 0 {
			status.Accessible = access.ResourceListAllowsProject(allowedResources, item.Organization, item.Name)
			status.RequestAccess = !status.Accessible
			status.RequestAccessResourcePath = access.ProgramProjectResourcePath(item.Organization, item.Name)
		}
		responses = append(responses, status)
	}
	return responses, nil
}

func (service *ReadService) ProjectIDs(ctx context.Context) ([]string, error) {
	projectIDs, err := geckodb.ConfigListByTypeContext(ctx, service.db, string(appconfig.TypeProjects))
	if err != nil {
		return nil, domain.NewTypedError(
			domain.ErrorKindDatabase,
			apierror.TypeDatabaseError,
			http.StatusInternalServerError,
			fmt.Sprintf("failed to list project configs: %s", err),
			nil,
		)
	}
	return projectIDs, nil
}

func (service *ReadService) ProjectConfig(ctx context.Context, projectID string) (appconfig.ProjectConfig, error) {
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGenericContext(ctx, service.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return appconfig.ProjectConfig{}, domain.NewTypedError(
				domain.ErrorKindNotFound,
				apierror.TypeNotFound,
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
	return cfg, nil
}

func (service *ReadService) loadProjectConfig(ctx context.Context, organization string, project string) (string, appconfig.ProjectConfig, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" {
		return "", appconfig.ProjectConfig{}, domain.NewTypedError(domain.ErrorKindValidation, apierror.Type("invalid_request"), http.StatusBadRequest, "organization and project are required", nil)
	}
	projectID := organization + "/" + project
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGenericContext(ctx, service.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", appconfig.ProjectConfig{}, domain.NewTypedError(domain.ErrorKindNotFound, apierror.TypeNotFound, http.StatusNotFound, fmt.Sprintf("no project config found for %s", projectID), map[string]any{"project_id": projectID})
		}
		return "", appconfig.ProjectConfig{}, domain.NewTypedError(domain.ErrorKindDatabase, apierror.TypeDatabaseError, http.StatusInternalServerError, fmt.Sprintf("failed to load project config: %s", err), map[string]any{"project_id": projectID})
	}
	return projectID, cfg, nil
}

func (service *ReadService) State(ctx context.Context, resolved ResolvedProject) (*geckodb.GitProjectState, error) {
	state, err := service.repository.LoadProjectState(ctx, service.db, resolved.ID, resolved.Identity)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKindDatabase, http.StatusInternalServerError, "failed to read git state", err, map[string]any{"project_id": resolved.ID})
	}
	return state, nil
}

type GitMirror struct {
	Resolved      ResolvedProject
	State         *geckodb.GitProjectState
	Repository    *gogit.Repository
	DefaultBranch string
	Empty         bool
}

func (service *ReadService) OpenMirror(ctx context.Context, authorizationHeader string, resolved ResolvedProject, requireCurrentMirror bool) (*GitMirror, error) {
	state, err := service.State(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if state == nil || strings.TrimSpace(state.MirrorPath) == "" {
		return nil, domain.NewTypedError(domain.ErrorKindValidation, apierror.Type("conflict"), http.StatusConflict, fmt.Sprintf("project %s has not been refreshed yet", resolved.ID), map[string]any{"project_id": resolved.ID})
	}
	if strings.TrimSpace(authorizationHeader) != "" {
		refreshCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		refreshed, refreshErr := service.EnsureMirrorReady(refreshCtx, authorizationHeader, resolved, state, requireCurrentMirror)
		if refreshErr != nil {
			if requireCurrentMirror {
				return nil, refreshErr
			}
			log.Printf("WARNING: failed to warm git mirror for %s: %v", resolved.ID, refreshErr)
		} else {
			if refreshed != nil {
				state = refreshed
			}
		}
	}
	mirror, err := repo.OpenRepository(state.MirrorPath)
	if err != nil {
		return nil, domain.NewTypedError(domain.ErrorKindIntegration, apierror.Type("integration_error"), http.StatusBadGateway, fmt.Sprintf("failed to open git mirror: %s", err), map[string]any{"project_id": resolved.ID})
	}
	return &GitMirror{
		Resolved:      resolved,
		State:         state,
		Repository:    mirror,
		DefaultBranch: state.DefaultBranch.String,
		Empty:         repo.RepositoryIsEmpty(mirror),
	}, nil
}

func (service *ReadService) EnsureMirrorReady(ctx context.Context, authorizationHeader string, resolved ResolvedProject, state *geckodb.GitProjectState, requireCurrentMirror bool) (*geckodb.GitProjectState, error) {
	if state == nil || !state.InstallationID.Valid {
		return state, nil
	}
	if strings.TrimSpace(state.MirrorPath) == "" {
		return state, nil
	}
	if !shouldRefreshMirrorForRead(state.MirrorPath, requireCurrentMirror) {
		return state, nil
	}
	accessToken, err := service.repository.RequestInstallationToken(ctx, authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, "read")
	if err != nil {
		state.SyncState = GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectStateContext(ctx, service.db, *state)
		return state, err
	}
	_, updatedState, err := service.repository.RefreshProject(ctx, resolved.ID, resolved.Identity, state, accessToken)
	if err != nil {
		state.SyncState = GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectStateContext(ctx, service.db, *state)
		return state, err
	}
	if err := geckodb.UpsertGitProjectStateContext(ctx, service.db, *updatedState); err != nil {
		return nil, fmt.Errorf("persist refreshed git project state: %w", err)
	}
	return updatedState, nil
}

func mirrorExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func shouldRefreshMirrorForRead(mirrorPath string, requireCurrentMirror bool) bool {
	return requireCurrentMirror || !mirrorExists(mirrorPath)
}
