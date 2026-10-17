package httpapi

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/gofiber/fiber/v3"
)

type gitOrganizationConnectResponse struct {
	Mode           string                                `json:"mode"`
	RedirectURL    string                                `json:"redirect_url,omitempty"`
	InstallationID *int64                                `json:"installation_id,omitempty"`
	Repositories   []domain.GitHubInstallationRepository `json:"repositories,omitempty"`
}

func (handler *GitHandler) handleGitOrganizationInitConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
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
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	redirectURL, err := handler.repoService.RequestInstallationURL(
		connectCtx,
		authorizationHeader,
		organization,
		redirectPath,
	)
	if err != nil {
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to request GitHub App install URL from Fence: %s", err), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	if strings.TrimSpace(requestBody.RepositoryFullName) != "" {
		identity, parseErr := projectpkg.ParseRequestedRepositoryIdentity(requestBody.RepositoryFullName)
		if parseErr != nil {
			response := httputil.NewError(apierror.Type("invalid_request"), fmt.Sprintf("invalid repository_full_name %q: %s", requestBody.RepositoryFullName, parseErr), http.StatusBadRequest, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}

		targetID, repoID, resolveErr := handler.repoService.ResolveTargetAndRepositoryIDs(connectCtx, identity)
		if resolveErr != nil {
			handler.logger.Warning(fmt.Sprintf("skipping GitHub install redirect optimization for %s/%s: %v", identity.Owner, identity.Repo, resolveErr))
			if settingsURL, ok := handler.organizationInstallationSettingsURL(connectCtx, authorizationHeader, organization, identity.Owner, redirectURL); ok {
				redirectURL = settingsURL
			}
		} else {
			redirectURL = projectpkg.DecorateInstallationRedirectURL(redirectURL, targetID, repoID)
		}
	}

	return httputil.JSON(gitOrganizationConnectResponse{
		Mode:        "redirect",
		RedirectURL: redirectURL,
	}, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) organizationInstallationSettingsURL(ctx context.Context, authorizationHeader string, organization string, owner string, redirectURL string) (string, bool) {
	installation, err := handler.repoService.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
	if err != nil {
		handler.logger.Warning(fmt.Sprintf("failed to load GitHub organization installation for %s: %v", owner, err))
		return "", false
	}
	if !installation.Installed {
		return "", false
	}
	if installation.InstallationID == nil || *installation.InstallationID <= 0 {
		return "", false
	}

	if appInstallationURL := projectpkg.AppInstallationRedirectURL(strings.TrimSpace(redirectURL), *installation.InstallationID); appInstallationURL != "" {
		return appInstallationURL, true
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", false
	}
	return fmt.Sprintf("https://github.com/organizations/%s/settings/installations/%d", owner, *installation.InstallationID), true
}

func (handler *GitHandler) handleGitOrganizationConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	type connectRequest struct {
		InstallationID *int64 `json:"installation_id"`
		GitHubOwner    string `json:"github_owner"`
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
	githubOwner := strings.TrimSpace(requestBody.GitHubOwner)
	if githubOwner == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "github_owner is required", http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	installation, err := handler.repoService.RequestOrganizationInstallationStatus(
		connectCtx,
		authorizationHeader,
		organization,
		githubOwner,
	)
	if err != nil {
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization, "github_owner": githubOwner}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to load GitHub organization installation status: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "github_owner": githubOwner}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if !installation.Installed || installation.InstallationID == nil || *installation.InstallationID <= 0 {
		response := httputil.NewError(apierror.Type("conflict"), "organization is not connected to the GitHub App", http.StatusConflict, map[string]any{"organization": organization, "github_owner": githubOwner}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := handler.installation().PersistConnectedOrganizationState(ctx.Context(), organization, githubOwner, installation); err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	repositories, err := handler.installation().ListConnectedInstallationRepositories(
		connectCtx,
		authorizationHeader,
		organization,
		"",
		githubOwner,
		*requestBody.InstallationID,
	)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(gitOrganizationConnectResponse{
		Mode:           "select_repository",
		InstallationID: requestBody.InstallationID,
		Repositories:   repositories,
	}, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectEditConnectPOST(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	type editConnectRequest struct {
		RepositoryFullName string `json:"repository_full_name"`
	}
	requestBody := editConnectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization, "project": project}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	projectID := organization + "/" + project
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	projectCfg, err := handler.installation().LoadProjectConfig(connectCtx, projectID)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	if strings.TrimSpace(requestBody.RepositoryFullName) == "" {
		if err := handler.installation().UnbindProjectRepository(connectCtx, projectID, projectCfg); err != nil {
			return WriteAppError(ctx, err, handler.logger)
		}
		return httputil.JSON(gitOrganizationConnectResponse{Mode: "disconnected"}, http.StatusOK).Write(ctx)
	}
	orgState, err := handler.installation().LoadOrBootstrapConnectedOrganizationState(connectCtx, authorizationHeader, organization, project, requestBody.RepositoryFullName)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	owner := organization
	if orgState.InstallationTarget.Valid {
		owner = strings.TrimSpace(orgState.InstallationTarget.String)
	}
	repositories, err := handler.installation().ListConnectedInstallationRepositories(connectCtx, authorizationHeader, organization, project, owner, orgState.InstallationID.Int64)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	selectedIdentity, found, err := projectpkg.NormalizeInstallationRepository(requestBody.RepositoryFullName, repositories, organization, project)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	if !found {
		response := httputil.NewError(apierror.Type("conflict"), fmt.Sprintf("GitHub App is not connected to repository %q", requestBody.RepositoryFullName), http.StatusConflict, map[string]any{"organization": organization, "project": project, "repository": requestBody.RepositoryFullName}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := handler.installation().BindProjectRepository(connectCtx, projectID, projectCfg, orgState, selectedIdentity); err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(gitOrganizationConnectResponse{Mode: "connected"}, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectUpdatePOST(ctx fiber.Ctx) error {
	organization, project, projectID, cfg, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(ctx.Context(), projectID, identity)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil {
		state = &geckodb.GitProjectState{ProjectID: projectID, RepoHost: identity.Host, RepoOwner: identity.Owner, RepoName: identity.Repo, MirrorPath: handler.repoService.MirrorPathForIdentity(identity), SyncState: projectpkg.GitSyncNeverSynced}
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refreshCtx, cancel := context.WithTimeout(ctx.Context(), 60*time.Second)
	defer cancel()
	accessToken, err := handler.repoService.RequestInstallationToken(refreshCtx, authorizationHeader, organization, project, identity, "read")
	if err != nil {
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to exchange GitHub token with Fence: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	state.SyncState = projectpkg.GitSyncUpdating
	state.LastError = sql.NullString{}
	if err := geckodb.UpsertGitProjectStateContext(ctx.Context(), handler.db, *state); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist updating git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refreshResponse, updatedState, err := handler.repoService.RefreshProject(refreshCtx, projectID, identity, state, accessToken)
	if err != nil {
		state.SyncState = projectpkg.GitSyncError
		state.LastError = sql.NullString{String: err.Error(), Valid: true}
		_ = geckodb.UpsertGitProjectStateContext(ctx.Context(), handler.db, *state)
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to update git checkout for %s/%s: %s", organization, project, err), http.StatusBadGateway, map[string]any{"project_id": projectID, "repository": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := geckodb.UpsertGitProjectStateContext(ctx.Context(), handler.db, *updatedState); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist updated git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refreshResponse, http.StatusOK).Write(ctx)
}
