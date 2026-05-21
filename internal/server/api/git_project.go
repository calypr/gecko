package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

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
		responses = append(responses, handler.gitService.StatusFromState(projectID, parts[0], parts[1], cfg, identity, statePtr, orgState))
	}
	return httputil.JSON(responses, http.StatusOK).Write(ctx)
}

func (handler *Handler) buildGitOrganizationStatus(organization string, projectIDs []string, projectStates map[string]geckodb.GitProjectState, organizationStates map[string]geckodb.GitOrganizationState) (git.GitOrganizationStatusResponse, *httputil.ErrorResponse) {
	responsePayload := git.GitOrganizationStatusResponse{
		Organization: organization,
		Projects:     make([]git.GitOrganizationProjectStatus, 0),
	}
	orgState, hasOrgState := organizationStates[organization]
	if hasOrgState {
		responsePayload.AppInstalled = orgState.Installed
		responsePayload.Connected = orgState.Installed
		if orgState.InstallationID.Valid {
			installationID := orgState.InstallationID.Int64
			responsePayload.InstallationID = &installationID
		}
		if orgState.HTMLURL.Valid {
			responsePayload.HTMLURL = orgState.HTMLURL.String
		}
		if orgState.RepositorySelection.Valid {
			responsePayload.RepositorySelection = orgState.RepositorySelection.String
		}
	}

	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] != organization {
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

		state, hasProjectState := projectStates[projectID]
		if !hasProjectState {
			state = geckodb.GitProjectState{
				ProjectID:  projectID,
				RepoHost:   identity.Host,
				RepoOwner:  identity.Owner,
				RepoName:   identity.Repo,
				MirrorPath: handler.gitService.MirrorPathForIdentity(identity),
				SyncState:  git.GitSyncNeverSynced,
			}
		}

		installation := git.GitRepositoryInstallationStatus{}
		configured := false
		if responsePayload.AppInstalled && responsePayload.RepositorySelection == "all" {
			configured = true
			installation.Installed = true
			installation.InstallationID = responsePayload.InstallationID
			installation.Target = organization
			installation.TargetType = "Organization"
			installation.HTMLURL = responsePayload.HTMLURL
			installation.RepositorySelection = responsePayload.RepositorySelection
		} else if state.InstallationID.Valid || state.InstallationTarget.Valid {
			configured = true
			installation.Installed = true
			if state.InstallationID.Valid {
				installationID := state.InstallationID.Int64
				installation.InstallationID = &installationID
			}
			if state.InstallationTarget.Valid {
				installation.Target = state.InstallationTarget.String
			}
			if state.InstallationTargetType.Valid {
				installation.TargetType = state.InstallationTargetType.String
			}
			if responsePayload.HTMLURL != "" {
				installation.HTMLURL = responsePayload.HTMLURL
			}
			if responsePayload.RepositorySelection != "" {
				installation.RepositorySelection = responsePayload.RepositorySelection
			}
		}

		responsePayload.Projects = append(responsePayload.Projects, git.GitOrganizationProjectStatus{
			ProjectID:    projectID,
			Project:      parts[1],
			Repository:   identity,
			Configured:   configured,
			Installation: installation,
		})
	}

	responsePayload.TotalProjects = len(responsePayload.Projects)
	for _, projectStatus := range responsePayload.Projects {
		if projectStatus.Installation.Installed {
			responsePayload.ConnectedProjects++
		}
		if projectStatus.Configured {
			responsePayload.ConfiguredProjects++
		}
	}
	responsePayload.ConfigurationState = git.OrganizationConfigurationState(
		responsePayload.AppInstalled,
		responsePayload.ConfiguredProjects,
		responsePayload.TotalProjects,
	)
	return responsePayload, nil
}

func projectConfigOrganizations(projectIDs []string) []string {
	organizations := make([]string, 0)
	seen := make(map[string]struct{})
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		if _, ok := seen[parts[0]]; ok {
			continue
		}
		seen[parts[0]] = struct{}{}
		organizations = append(organizations, parts[0])
	}
	sort.Strings(organizations)
	return organizations
}

func (handler *Handler) reconcileGitOrganizationState(ctx context.Context, organization string, authorizationHeader string, projectIDs []string) *httputil.ErrorResponse {
	now := time.Now().UTC()
	orgInstallation, err := handler.gitService.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			if statusErr.StatusCode != http.StatusNotFound {
				response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization}, nil)
				response.WriteLog(handler.logger)
				return response
			}
			orgInstallation = git.GitRepositoryInstallationStatus{}
		} else {
			response := httputil.NewError("integration_error", fmt.Sprintf("failed to load GitHub organization installation status: %s", err), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response
		}
	}

	orgState := geckodb.GitOrganizationState{
		Organization: organization,
		Installed:    orgInstallation.Installed,
		UpdatedAt:    now,
		LastSeenAt:   sql.NullTime{Time: now, Valid: true},
		ConfiguredAt: sql.NullTime{Time: now, Valid: orgInstallation.Installed},
		LastError:    sql.NullString{},
	}
	if orgInstallation.InstallationID != nil {
		orgState.InstallationID = sql.NullInt64{Int64: *orgInstallation.InstallationID, Valid: true}
	}
	if orgInstallation.Target != "" {
		orgState.InstallationTarget = sql.NullString{String: orgInstallation.Target, Valid: true}
	}
	if orgInstallation.TargetType != "" {
		orgState.InstallationTargetType = sql.NullString{String: orgInstallation.TargetType, Valid: true}
	}
	if orgInstallation.HTMLURL != "" {
		orgState.HTMLURL = sql.NullString{String: orgInstallation.HTMLURL, Valid: true}
	}
	if orgInstallation.RepositorySelection != "" {
		orgState.RepositorySelection = sql.NullString{String: orgInstallation.RepositorySelection, Valid: true}
	}
	if err := geckodb.UpsertGitOrganizationState(handler.db, orgState); err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to persist git organization state: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response
	}

	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] != organization {
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
		projectState, _ := geckodb.GitProjectStateByProjectID(handler.db, projectID)
		if projectState == nil {
			projectState = &geckodb.GitProjectState{
				ProjectID:  projectID,
				RepoHost:   identity.Host,
				RepoOwner:  identity.Owner,
				RepoName:   identity.Repo,
				MirrorPath: handler.gitService.MirrorPathForIdentity(identity),
				SyncState:  git.GitSyncNeverSynced,
			}
		}

		if orgInstallation.Installed && orgInstallation.RepositorySelection == "all" {
			if orgInstallation.InstallationID != nil {
				projectState.InstallationID = sql.NullInt64{Int64: *orgInstallation.InstallationID, Valid: true}
			} else {
				projectState.InstallationID = sql.NullInt64{}
			}
			projectState.InstallationTarget = sql.NullString{String: organization, Valid: organization != ""}
			projectState.InstallationTargetType = sql.NullString{String: "Organization", Valid: true}
			_ = geckodb.UpsertGitProjectState(handler.db, *projectState)
			continue
		}

		repoInstallation, err := handler.gitService.RequestInstallationStatus(ctx, authorizationHeader, identity)
		if err != nil {
			if statusErr, ok := err.(*git.HTTPStatusError); ok {
				if statusErr.StatusCode == http.StatusForbidden || statusErr.StatusCode == http.StatusNotFound {
					projectState.InstallationID = sql.NullInt64{}
					projectState.InstallationTarget = sql.NullString{}
					projectState.InstallationTargetType = sql.NullString{}
					_ = geckodb.UpsertGitProjectState(handler.db, *projectState)
					continue
				}
				response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization, "project_id": projectID, "repository": cfg.SrcRepo}, nil)
				response.WriteLog(handler.logger)
				return response
			}
			response := httputil.NewError("integration_error", fmt.Sprintf("failed to load GitHub installation status: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "project_id": projectID, "repository": cfg.SrcRepo}, nil)
			response.WriteLog(handler.logger)
			return response
		}

		if repoInstallation.Installed {
			if repoInstallation.InstallationID != nil {
				projectState.InstallationID = sql.NullInt64{Int64: *repoInstallation.InstallationID, Valid: true}
			} else {
				projectState.InstallationID = sql.NullInt64{}
			}
			projectState.InstallationTarget = sql.NullString{String: repoInstallation.Target, Valid: repoInstallation.Target != ""}
			projectState.InstallationTargetType = sql.NullString{String: repoInstallation.TargetType, Valid: repoInstallation.TargetType != ""}
		} else {
			projectState.InstallationID = sql.NullInt64{}
			projectState.InstallationTarget = sql.NullString{}
			projectState.InstallationTargetType = sql.NullString{}
		}
		_ = geckodb.UpsertGitProjectState(handler.db, *projectState)
	}
	return nil
}

func (handler *Handler) buildGitOrganizationsSummary(projectIDs []string) (git.GitOrganizationsStatusResponse, *httputil.ErrorResponse) {
	organizations := projectConfigOrganizations(projectIDs)
	projectStates, err := geckodb.ListGitProjectStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git project states: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return git.GitOrganizationsStatusResponse{}, response
	}
	organizationStates, err := geckodb.ListGitOrganizationStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git organization states: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return git.GitOrganizationsStatusResponse{}, response
	}
	responsePayload := git.GitOrganizationsStatusResponse{Organizations: make([]git.GitOrganizationStatusResponse, 0, len(organizations))}
	for _, organization := range organizations {
		organizationStatus, errResponse := handler.buildGitOrganizationStatus(organization, projectIDs, projectStates, organizationStates)
		if errResponse != nil {
			return git.GitOrganizationsStatusResponse{}, errResponse
		}
		responsePayload.Organizations = append(responsePayload.Organizations, organizationStatus)
		responsePayload.TotalProjects += organizationStatus.TotalProjects
		responsePayload.ConnectedProjects += organizationStatus.ConnectedProjects
		responsePayload.ConfiguredProjects += organizationStatus.ConfiguredProjects
		if organizationStatus.Connected {
			responsePayload.ConnectedOrganizations++
		}
		if organizationStatus.AppInstalled {
			responsePayload.InstalledOrganizations++
		}
	}
	responsePayload.TotalOrganizations = len(responsePayload.Organizations)
	responsePayload.AppInstalled = responsePayload.InstalledOrganizations > 0
	responsePayload.Connected = responsePayload.AppInstalled
	responsePayload.ConfigurationState = git.OrganizationConfigurationState(responsePayload.AppInstalled, responsePayload.ConfiguredProjects, responsePayload.TotalProjects)
	return responsePayload, nil
}

func (handler *Handler) handleGitProjectGET(ctx fiber.Ctx) error {
	organization, project, projectID, cfg, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	orgState, _ := geckodb.GitOrganizationStateByOrganization(handler.db, organization)
	return httputil.JSON(handler.gitService.StatusFromState(projectID, organization, project, cfg, identity, state, orgState), http.StatusOK).Write(ctx)
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
	projectStates, err := geckodb.ListGitProjectStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git project states: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	organizationStates, err := geckodb.ListGitOrganizationStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git organization states: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	responsePayload, errResponse := handler.buildGitOrganizationStatus(organization, projectIDs, projectStates, organizationStates)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationsStatusGET(ctx fiber.Ctx) error {
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	responsePayload, errResponse := handler.buildGitOrganizationsSummary(projectIDs)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
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
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if errResponse := handler.reconcileGitOrganizationState(reconcileCtx, organization, authorizationHeader, projectIDs); errResponse != nil {
		return errResponse.Write(ctx)
	}
	projectStates, err := geckodb.ListGitProjectStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git project states: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	organizationStates, err := geckodb.ListGitOrganizationStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git organization states: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	responsePayload, errResponse := handler.buildGitOrganizationStatus(organization, projectIDs, projectStates, organizationStates)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
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
	organizations := projectConfigOrganizations(projectIDs)
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, organization := range organizations {
		if errResponse := handler.reconcileGitOrganizationState(reconcileCtx, organization, authorizationHeader, projectIDs); errResponse != nil {
			return errResponse.Write(ctx)
		}
	}
	responsePayload, errResponse := handler.buildGitOrganizationsSummary(projectIDs)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}
