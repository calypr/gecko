package git

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func filterProjectIDsByAllowedResources(projectIDs []string, allowedResources []string) []string {
	if len(allowedResources) == 0 {
		return []string{}
	}
	filtered := make([]string, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 {
			continue
		}
		projectParts := strings.SplitN(parts[1], "/", 2)
		if len(projectParts) != 1 || projectParts[0] == "" {
			continue
		}
		if servermw.GitProjectReadable(allowedResources, parts[0], projectParts[0]) {
			filtered = append(filtered, projectID)
		}
	}
	sort.Strings(filtered)
	return filtered
}

func organizationAllowedByResources(organization string, allowedResources []string) bool {
	return git.ResourceListAllowsOrganization(allowedResources, organization)
}

func (handler *Handler) resolveGitProject(ctx fiber.Ctx) (string, string, string, appconfig.ProjectConfig, git.GitRepositoryIdentity, *httputil.ErrorResponse) {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	projectID := organization + "/" + project
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			response := httputil.NewError("not_found", fmt.Sprintf("no project config found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
		}
		response := httputil.NewError("database_error", fmt.Sprintf("failed to load project config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	identity, err := git.ParseRepositoryIdentity(cfg.SrcRepo)
	if err != nil {
		response := httputil.NewError("validation_failed", fmt.Sprintf("invalid src_repo for %s: %s", projectID, err), http.StatusBadRequest, map[string]any{"project_id": projectID, "src_repo": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	return organization, project, projectID, cfg, identity, nil
}

func (handler *Handler) loadGitProjectState(projectID string, identity git.GitRepositoryIdentity) (*geckodb.GitProjectState, error) {
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil || state == nil {
		return state, err
	}
	expectedMirrorPath := handler.gitService.MirrorPathForIdentity(identity)
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
	if err := geckodb.UpsertGitProjectState(handler.db, *state); err != nil {
		return nil, fmt.Errorf("persist git project state mirror path: %w", err)
	}
	return state, nil
}

func (handler *Handler) ensureMirrorReadyForRead(ctx context.Context, authorizationHeader string, projectID string, identity git.GitRepositoryIdentity, state *geckodb.GitProjectState) (*geckodb.GitProjectState, error) {
	if state == nil || !state.InstallationID.Valid {
		return state, nil
	}
	if strings.TrimSpace(state.MirrorPath) == "" {
		return state, nil
	}
	if _, err := os.Stat(state.MirrorPath); err == nil {
		return state, nil
	}
	org, project, _ := strings.Cut(projectID, "/")
	accessToken, err := handler.gitService.RequestInstallationToken(ctx, authorizationHeader, org, project, identity, "read")
	if err != nil {
		state.SyncState = git.GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectState(handler.db, *state)
		return state, err
	}
	_, updatedState, err := handler.gitService.RefreshProject(ctx, projectID, identity, state, accessToken)
	if err != nil {
		state.SyncState = git.GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectState(handler.db, *state)
		return state, err
	}
	if err := geckodb.UpsertGitProjectState(handler.db, *updatedState); err != nil {
		return nil, fmt.Errorf("persist refreshed git project state: %w", err)
	}
	return updatedState, nil
}

func (handler *Handler) handleGitProjectsGET(ctx fiber.Ctx) error {
	states, err := geckodb.ListGitProjectStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git state: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	allowedResources, _ := gitAllowedReadResources(strings.TrimSpace(ctx.Get("Authorization")))
	projectIDs = filterProjectIDsByAllowedResources(projectIDs, allowedResources)
	responses := make([]git.GitProjectStatusResponse, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 {
			continue
		}
		var cfg appconfig.ProjectConfig
		if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
			continue
		}
		identity, err := git.ParseRepositoryIdentity(cfg.SrcRepo)
		if err != nil {
			continue
		}
		var statePtr *geckodb.GitProjectState
		if state, ok := states[projectID]; ok {
			copyState := state
			statePtr = &copyState
		}
		orgState, _ := geckodb.GitOrganizationStateByOrganization(handler.db, parts[0])
		status := handler.gitService.StatusFromState(projectID, parts[0], parts[1], cfg, identity, statePtr, orgState)
		if len(allowedResources) > 0 {
			status.Accessible = servermw.GitProjectReadable(allowedResources, parts[0], parts[1])
			status.RequestAccess = !status.Accessible
			status.RequestAccessResourcePath = git.ProgramProjectResourcePath(parts[0], parts[1])
		}
		responses = append(responses, status)
	}
	return httputil.JSON(responses, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectGET(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectID := organization + "/" + project
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			response := httputil.NewError("not_found", fmt.Sprintf("no project config found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError("database_error", fmt.Sprintf("failed to load project config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	identity, identityErr := git.ParseRepositoryIdentity(cfg.SrcRepo)
	if identityErr != nil {
		orgState, _ := geckodb.GitOrganizationStateByOrganization(handler.db, organization)
		status := handler.gitService.StatusFromState(projectID, organization, project, cfg, git.GitRepositoryIdentity{}, nil, orgState)
		status.Accessible = true
		return httputil.JSON(status, http.StatusOK).Write(ctx)
	}
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	if authorizationHeader != "" {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s: %v", projectID, err)
		}
	}
	orgState, _ := geckodb.GitOrganizationStateByOrganization(handler.db, organization)
	status := handler.gitService.StatusFromState(projectID, organization, project, cfg, identity, state, orgState)
	status.Accessible = true
	return httputil.JSON(status, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationStatusGET(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		response := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	statusAccess, errResponse := gitStatusAccessSnapshot(authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.readableResources
	if !organizationAllowedByResources(organization, allowedResources) {
		response := httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	responsePayload, err := handler.projectSync.BuildSingleOrganizationStatus(context.Background(), authorizationHeader, organization, projectIDs, allowedResources)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	applyOrganizationStatusCapabilities(&responsePayload, statusAccess.snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationsStatusGET(ctx fiber.Ctx) error {
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	statusAccess, errResponse := gitStatusAccessSnapshot(authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.readableResources
	responsePayload, err := handler.projectSync.BuildOrganizationsStatus(context.Background(), authorizationHeader, projectIDs, allowedResources)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	for i := range responsePayload.Organizations {
		applyOrganizationStatusCapabilities(&responsePayload.Organizations[i], statusAccess.snapshot)
	}
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func gitAllowedReadResources(token string) ([]string, *httputil.ErrorResponse) {
	if token == "" {
		return nil, nil
	}
	resources, err := servermw.GitAllowedResources(servermw.NewFenceUserAccessHandler(nil), token, "read")
	if err != nil {
		return nil, err
	}
	return resources, nil
}

type gitStatusAccess struct {
	readableResources []string
	snapshot          servermw.ResourceAccessSnapshot
}

func gitStatusAccessSnapshot(token string) (*gitStatusAccess, *httputil.ErrorResponse) {
	if token == "" {
		return &gitStatusAccess{}, nil
	}
	authzHandler := servermw.NewFenceUserAccessHandler(nil)
	snapshot, err := authzHandler.GetResourceAccess(token)
	if err != nil {
		if serverErr, ok := err.(*servermw.AccessError); ok {
			return nil, httputil.NewError(gitStatusServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
		}
		return nil, httputil.NewError("authorization_service_error", fmt.Sprintf("authorization lookup failed: %s", err), http.StatusForbidden, nil, nil)
	}
	readableResources := make([]string, 0, len(snapshot))
	for resourcePath := range snapshot {
		if servermw.ResourceAccessAllows(snapshot, resourcePath, "read", "*") {
			readableResources = append(readableResources, resourcePath)
		}
	}
	sort.Strings(readableResources)
	return &gitStatusAccess{
		readableResources: readableResources,
		snapshot:          snapshot,
	}, nil
}

func gitStatusServiceErrorType(code int) apierror.Type {
	switch code {
	case http.StatusUnauthorized:
		return apierror.TypeUnauthorized
	case http.StatusForbidden:
		return apierror.TypeForbidden
	case http.StatusNotFound:
		return apierror.TypeNotFound
	case http.StatusMethodNotAllowed:
		return apierror.TypeMethodNotAllowed
	default:
		return apierror.TypeAuthorizationServiceError
	}
}

func applyOrganizationStatusCapabilities(response *git.GitOrganizationStatusResponse, snapshot servermw.ResourceAccessSnapshot) {
	if response == nil {
		return
	}
	response.CanAccessSettings = true
	orgResource := fmt.Sprintf("/programs/%s", response.Organization)
	orgProjectsResource := fmt.Sprintf("/programs/%s/projects", response.Organization)
	response.CanCreateProjects = servermw.ResourceAccessAllows(snapshot, orgProjectsResource, "create-descendant", "arborist")
	response.CanManagePeople = servermw.ResourceAccessAllows(snapshot, orgResource, "manage-owners", "arborist")
	response.CanDeleteOrg = response.CanManagePeople || servermw.ResourceAccessAllows(snapshot, orgResource, "delete", "*")
	for i := range response.Projects {
		response.Projects[i].CanManageSettings = servermw.ResourceAccessAllows(snapshot, response.Projects[i].ResourcePath, "update", "*")
	}
}

func (handler *Handler) handleGitOrganizationReconcilePOST(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		response := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	statusAccess, errResponse := gitStatusAccessSnapshot(authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.readableResources
	if !organizationAllowedByResources(organization, allowedResources) {
		response := httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := handler.projectSync.ReconcileOrganization(reconcileCtx, organization, authorizationHeader, projectIDs); err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	statusAccess, errResponse = gitStatusAccessSnapshot(authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	allowedResources = statusAccess.readableResources
	responsePayload, err := handler.projectSync.BuildSingleOrganizationStatus(context.Background(), authorizationHeader, organization, projectIDs, allowedResources)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	applyOrganizationStatusCapabilities(&responsePayload, statusAccess.snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationsReconcilePOST(ctx fiber.Ctx) error {
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	statusAccess, errResponse := gitStatusAccessSnapshot(authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.readableResources
	projectIDs = filterProjectIDsByAllowedResources(projectIDs, allowedResources)
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := handler.projectSync.ReconcileOrganizations(reconcileCtx, authorizationHeader, projectIDs); err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	responsePayload, err := handler.projectSync.BuildOrganizationsStatus(context.Background(), authorizationHeader, projectIDs, allowedResources)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	for i := range responsePayload.Organizations {
		applyOrganizationStatusCapabilities(&responsePayload.Organizations[i], statusAccess.snapshot)
	}
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}
