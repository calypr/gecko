package git

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/google/go-github/v87/github"
)

func (handler *Handler) handleGitOrganizationInitConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	type initConnectRequest struct {
		RedirectPath       string `json:"redirect_path"`
		Project            string `json:"project"`
		RepositoryFullName string `json:"repository_full_name"`
	}
	requestBody := initConnectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	redirectPath := strings.TrimSpace(requestBody.RedirectPath)
	if redirectPath == "" {
		redirectPath = "/git"
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	redirectURL, err := handler.gitService.RequestInstallationURL(
		connectCtx,
		authorizationHeader,
		organization,
		redirectPath,
	)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to request GitHub App install URL from Fence: %s", err), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	if strings.TrimSpace(requestBody.RepositoryFullName) == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "repository_full_name is required", http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	repoURL := requestBody.RepositoryFullName
	if !strings.HasPrefix(repoURL, "http://") && !strings.HasPrefix(repoURL, "https://") && !strings.HasPrefix(repoURL, "git@") {
		repoURL = "https://github.com/" + repoURL
	}

	identity, parseErr := git.ParseRepositoryIdentity(repoURL)
	if parseErr != nil {
		response := httputil.NewError(apierror.Type("invalid_request"), fmt.Sprintf("invalid repository_full_name %q: %s", requestBody.RepositoryFullName, parseErr), http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	// If the organization already has the GitHub App installed, check if this repo is already allowed
	orgState, _ := geckodb.GitOrganizationStateByOrganization(handler.db, organization)
	if orgState != nil && orgState.Installed && orgState.InstallationID.Valid {
		allowedRepos, err := handler.gitService.ListInstallationRepositories(
			connectCtx,
			authorizationHeader,
			orgState.InstallationID.Int64,
		)
		if err == nil {
			repoIsAllowed := false
			for _, r := range allowedRepos {
				if strings.EqualFold(r.FullName, identity.Owner+"/"+identity.Repo) {
					repoIsAllowed = true
					break
				}
			}
			if repoIsAllowed {
				// Update project config src_repo
				projectID := organization + "/" + requestBody.Project
				var projectCfg appconfig.ProjectConfig
				if getErr := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &projectCfg); getErr == nil {
					projectCfg.SrcRepo = identity.URL
					if putErr := geckodb.ConfigPUTGeneric(handler.db, projectID, string(appconfig.TypeProjects), &projectCfg); putErr != nil {
						handler.logger.Warning(fmt.Sprintf("failed to update project config src_repo for %s: %v", projectID, putErr))
					}
				}

				// Reset the git project state
				projectState, getErr := geckodb.GitProjectStateByProjectID(handler.db, projectID)
				if getErr == nil && projectState != nil {
					projectState.RepoHost = identity.Host
					projectState.RepoOwner = identity.Owner
					projectState.RepoName = identity.Repo
					projectState.InstallationID = orgState.InstallationID
					projectState.InstallationTarget = orgState.InstallationTarget
					projectState.InstallationTargetType = orgState.InstallationTargetType
					projectState.SyncState = git.GitSyncNeverSynced
					projectState.LastError = sql.NullString{}
					if upsertErr := geckodb.UpsertGitProjectState(handler.db, *projectState); upsertErr != nil {
						handler.logger.Warning(fmt.Sprintf("failed to reset git project state for %s: %v", projectID, upsertErr))
					}
				}

				return httputil.JSON(git.GitOrganizationConnectResponse{
					Mode: "connected",
				}, http.StatusOK).Write(ctx)
			}
		}
	}

	targetID, repoID, resolveErr := handler.gitService.ResolveTargetAndRepositoryIDs(
		connectCtx,
		authorizationHeader,
		organization,
		requestBody.Project,
		identity,
	)
	if resolveErr != nil {
		var githubErr *github.ErrorResponse
		if errors.As(resolveErr, &githubErr) && githubErr.Response != nil {
			if githubErr.Response.StatusCode == http.StatusNotFound {
				response := httputil.NewError(apierror.Type("invalid_request"), fmt.Sprintf("GitHub repository %q does not exist or you do not have permission to access it", requestBody.RepositoryFullName), http.StatusBadRequest, map[string]any{"organization": organization, "repository": requestBody.RepositoryFullName}, nil)
				response.WriteLog(handler.logger)
				return response.Write(ctx)
			}
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to resolve target and repository IDs: %s", resolveErr), http.StatusBadGateway, map[string]any{"organization": organization, "repository": requestBody.RepositoryFullName}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	// Update the project configuration src_repo in the database
	projectID := organization + "/" + requestBody.Project
	var projectCfg appconfig.ProjectConfig
	if getErr := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &projectCfg); getErr == nil {
		projectCfg.SrcRepo = identity.URL
		if putErr := geckodb.ConfigPUTGeneric(handler.db, projectID, string(appconfig.TypeProjects), &projectCfg); putErr != nil {
			handler.logger.Warning(fmt.Sprintf("failed to update project config src_repo for %s: %v", projectID, putErr))
		}
	}

	// Reset the git project state to use the new repository coordinates and clear stale installation states
	projectState, getErr := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if getErr == nil && projectState != nil {
		projectState.RepoHost = identity.Host
		projectState.RepoOwner = identity.Owner
		projectState.RepoName = identity.Repo
		projectState.InstallationID = sql.NullInt64{}
		projectState.InstallationTarget = sql.NullString{}
		projectState.InstallationTargetType = sql.NullString{}
		projectState.SyncState = git.GitSyncNeverSynced
		projectState.LastError = sql.NullString{}
		if upsertErr := geckodb.UpsertGitProjectState(handler.db, *projectState); upsertErr != nil {
			handler.logger.Warning(fmt.Sprintf("failed to reset git project state for %s: %v", projectID, upsertErr))
		}
	}

	hasNew := strings.Contains(redirectURL, "/installations/new")
	hasSelectTarget := strings.Contains(redirectURL, "/installations/select_target")
	if !hasNew && !hasSelectTarget {
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("Fence returned an unrecognized redirect URL: %s", redirectURL), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
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
	redirectURL = fmt.Sprintf("%s%ssuggested_target_id=%d&repository_ids[]=%d", redirectURL, separator, targetID, repoID)

	return httputil.JSON(git.GitOrganizationConnectResponse{
		Mode:        "redirect",
		RedirectURL: redirectURL,
	}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	type connectRequest struct {
		InstallationID *int64 `json:"installation_id"`
	}
	requestBody := connectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	if requestBody.InstallationID == nil || *requestBody.InstallationID <= 0 {
		response := httputil.NewError(apierror.Type("invalid_request"), "installation_id is required", http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	repositories, err := handler.gitService.ListInstallationRepositories(
		connectCtx,
		authorizationHeader,
		*requestBody.InstallationID,
	)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization, "installation_id": *requestBody.InstallationID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to list GitHub installation repositories: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "installation_id": *requestBody.InstallationID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(git.GitOrganizationConnectResponse{
		Mode:           "select_repository",
		InstallationID: requestBody.InstallationID,
		Repositories:   repositories,
	}, http.StatusOK).Write(ctx)
}


func (handler *Handler) handleGitProjectUpdatePOST(ctx fiber.Ctx) error {
	organization, project, projectID, cfg, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil {
		state = &geckodb.GitProjectState{ProjectID: projectID, RepoHost: identity.Host, RepoOwner: identity.Owner, RepoName: identity.Repo, MirrorPath: handler.gitService.MirrorPathForIdentity(identity), SyncState: git.GitSyncNeverSynced}
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	accessToken, err := handler.gitService.RequestInstallationToken(refreshCtx, authorizationHeader, organization, project, identity, "read")
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to exchange GitHub token with Fence: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	state.SyncState = git.GitSyncUpdating
	state.LastError = sql.NullString{}
	if err := geckodb.UpsertGitProjectState(handler.db, *state); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist updating git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refreshResponse, updatedState, err := handler.gitService.RefreshProject(refreshCtx, projectID, identity, state, accessToken)
	if err != nil {
		state.SyncState = git.GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectState(handler.db, *state)
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to update git checkout for %s/%s: %s", organization, project, err), http.StatusBadGateway, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := geckodb.UpsertGitProjectState(handler.db, *updatedState); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist updated git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refreshResponse, http.StatusOK).Write(ctx)
}
