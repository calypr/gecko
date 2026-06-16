package git

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
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
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

	if strings.TrimSpace(requestBody.RepositoryFullName) != "" {
		identity, parseErr := parseRequestedRepositoryIdentity(requestBody.RepositoryFullName)
		if parseErr != nil {
			response := httputil.NewError(apierror.Type("invalid_request"), fmt.Sprintf("invalid repository_full_name %q: %s", requestBody.RepositoryFullName, parseErr), http.StatusBadRequest, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}

		targetID, repoID, resolveErr := handler.gitService.ResolveTargetAndRepositoryIDs(connectCtx, identity)
		if resolveErr != nil {
			handler.logger.Warning(fmt.Sprintf("skipping GitHub install redirect optimization for %s/%s: %v", identity.Owner, identity.Repo, resolveErr))
			if settingsURL, ok := handler.organizationInstallationSettingsURL(connectCtx, authorizationHeader, organization, identity.Owner, redirectURL); ok {
				redirectURL = settingsURL
			}
		} else {
			redirectURL = decorateInstallationRedirectURL(redirectURL, targetID, repoID)
		}
	}

	return httputil.JSON(git.GitOrganizationConnectResponse{
		Mode:        "redirect",
		RedirectURL: redirectURL,
	}, http.StatusOK).Write(ctx)
}

func (handler *Handler) organizationInstallationSettingsURL(ctx context.Context, authorizationHeader string, organization string, owner string, redirectURL string) (string, bool) {
	installation, err := handler.gitService.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
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

	if appInstallationURL := appInstallationRedirectURL(strings.TrimSpace(redirectURL), *installation.InstallationID); appInstallationURL != "" {
		return appInstallationURL, true
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", false
	}
	return fmt.Sprintf("https://github.com/organizations/%s/settings/installations/%d", owner, *installation.InstallationID), true
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
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	installation, err := handler.gitService.RequestOrganizationInstallationStatus(
		connectCtx,
		authorizationHeader,
		organization,
		githubOwner,
	)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
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
	if err := handler.persistConnectedOrganizationState(organization, githubOwner, installation); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist organization git state: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization, "github_owner": githubOwner}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	repositories, err := handler.gitService.ListInstallationRepositories(
		connectCtx,
		authorizationHeader,
		organization,
		githubOwner,
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

func (handler *Handler) persistConnectedOrganizationState(organization string, githubOwner string, installation git.GitRepositoryInstallationStatus) error {
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
	return geckodb.UpsertGitOrganizationState(handler.db, state)
}

func (handler *Handler) handleGitProjectEditConnectPOST(ctx fiber.Ctx) error {
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
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	projectCfg, errResponse := handler.loadProjectConfig(connectCtx, projectID)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if strings.TrimSpace(requestBody.RepositoryFullName) == "" {
		if err := handler.unbindProjectRepository(connectCtx, projectID, projectCfg); err != nil {
			response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to clear project repository binding: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		return httputil.JSON(git.GitOrganizationConnectResponse{Mode: "disconnected"}, http.StatusOK).Write(ctx)
	}
	orgState, errResponse := handler.loadOrBootstrapConnectedOrganizationState(connectCtx, authorizationHeader, organization, project, requestBody.RepositoryFullName)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	owner := organization
	if orgState.InstallationTarget.Valid {
		owner = strings.TrimSpace(orgState.InstallationTarget.String)
	}
	repositories, errResponse := handler.listConnectedInstallationRepositories(connectCtx, authorizationHeader, organization, project, owner, orgState.InstallationID.Int64)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	selectedIdentity, found, errResponse := normalizeInstallationRepository(requestBody.RepositoryFullName, repositories, organization, project)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if !found {
		response := httputil.NewError(apierror.Type("conflict"), fmt.Sprintf("GitHub App is not connected to repository %q", requestBody.RepositoryFullName), http.StatusConflict, map[string]any{"organization": organization, "project": project, "repository": requestBody.RepositoryFullName}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	if err := handler.bindProjectRepository(connectCtx, projectID, projectCfg, orgState, selectedIdentity); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to bind project repository: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	return httputil.JSON(git.GitOrganizationConnectResponse{Mode: "connected"}, http.StatusOK).Write(ctx)
}

func (handler *Handler) loadOrBootstrapConnectedOrganizationState(ctx context.Context, authorizationHeader string, organization string, project string, repositoryFullName string) (*geckodb.GitOrganizationState, *httputil.ErrorResponse) {
	orgState, errResponse := handler.loadConnectedOrganizationState(ctx, organization, project)
	if errResponse == nil {
		return orgState, nil
	}
	if errResponse.Error.Code != http.StatusConflict {
		return nil, errResponse
	}

	identity, err := parseRequestedRepositoryIdentity(repositoryFullName)
	if err != nil {
		response := httputil.NewError(apierror.Type("invalid_request"), fmt.Sprintf("invalid repository_full_name %q: %s", repositoryFullName, err), http.StatusBadRequest, map[string]any{"organization": organization, "project": project}, nil)
		return nil, response
	}
	installation, installErr := handler.gitService.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, identity.Owner)
	if installErr != nil {
		if statusErr, ok := installErr.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner}, nil)
			return nil, response
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to load GitHub organization installation status: %s", installErr), http.StatusBadGateway, map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner}, nil)
		return nil, response
	}
	if !installation.Installed || installation.InstallationID == nil || *installation.InstallationID <= 0 {
		response := httputil.NewError(apierror.Type("conflict"), "organization is not connected to the GitHub App", http.StatusConflict, map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner}, nil)
		return nil, response
	}
	if err := handler.persistConnectedOrganizationState(organization, identity.Owner, installation); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist organization git state: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization, "project": project, "github_owner": identity.Owner}, nil)
		return nil, response
	}
	return handler.loadConnectedOrganizationState(ctx, organization, project)
}

func normalizeInstallationRepository(repositoryFullName string, repositories []git.GitHubInstallationRepository, organization string, project string) (git.GitRepositoryIdentity, bool, *httputil.ErrorResponse) {
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
		identity, err := git.ParseRepositoryIdentity(repoURL)
		if err != nil {
			response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to normalize installation repository %q: %s", repository.FullName, err), http.StatusBadGateway, map[string]any{"organization": organization, "project": project, "repository": repository.FullName}, nil)
			return git.GitRepositoryIdentity{}, false, response
		}
		return identity, true, nil
	}
	return git.GitRepositoryIdentity{}, false, nil
}

func parseRequestedRepositoryIdentity(repositoryFullName string) (git.GitRepositoryIdentity, error) {
	repoURL := strings.TrimSpace(repositoryFullName)
	if !strings.HasPrefix(repoURL, "http://") && !strings.HasPrefix(repoURL, "https://") && !strings.HasPrefix(repoURL, "git@") {
		repoURL = "https://github.com/" + repoURL
	}
	return git.ParseRepositoryIdentity(repoURL)
}

func decorateInstallationRedirectURL(redirectURL string, targetID int64, repoID int64) string {
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

func appInstallationRedirectURL(redirectURL string, installationID int64) string {
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

func (handler *Handler) loadProjectConfig(ctx context.Context, projectID string) (appconfig.ProjectConfig, *httputil.ErrorResponse) {
	var projectCfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGenericContext(ctx, handler.db, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		if err == sql.ErrNoRows {
			response := httputil.NewError(apierror.Type("not_found"), fmt.Sprintf("no project config found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			return appconfig.ProjectConfig{}, response
		}
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to load project config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		return appconfig.ProjectConfig{}, response
	}
	return projectCfg, nil
}

func (handler *Handler) loadConnectedOrganizationState(ctx context.Context, organization string, project string) (*geckodb.GitOrganizationState, *httputil.ErrorResponse) {
	orgState, err := geckodb.GitOrganizationStateByOrganizationContext(ctx, handler.db, organization)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to load organization git state: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		return nil, response
	}
	if orgState == nil || !orgState.Installed || !orgState.InstallationID.Valid {
		response := httputil.NewError(apierror.Type("conflict"), "organization is not connected to the GitHub App", http.StatusConflict, map[string]any{"organization": organization, "project": project}, nil)
		return nil, response
	}
	return orgState, nil
}

func (handler *Handler) listConnectedInstallationRepositories(ctx context.Context, authorizationHeader string, organization string, project string, owner string, installationID int64) ([]git.GitHubInstallationRepository, *httputil.ErrorResponse) {
	repositories, err := handler.gitService.ListInstallationRepositories(ctx, authorizationHeader, organization, owner, installationID)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"organization": organization, "project": project, "installation_id": installationID}, nil)
			return nil, response
		}
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to list GitHub installation repositories: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "project": project, "installation_id": installationID}, nil)
		return nil, response
	}
	return repositories, nil
}

func (handler *Handler) bindProjectRepository(ctx context.Context, projectID string, projectCfg appconfig.ProjectConfig, orgState *geckodb.GitOrganizationState, identity git.GitRepositoryIdentity) error {
	projectState, err := geckodb.GitProjectStateByProjectIDContext(ctx, handler.db, projectID)
	if err != nil {
		return fmt.Errorf("load project git state: %w", err)
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
	projectState.MirrorPath = handler.gitService.MirrorPathForIdentity(identity)
	projectState.SyncState = git.GitSyncNeverSynced
	projectState.DefaultBranch = sql.NullString{}
	projectState.LastRefreshedAt = sql.NullTime{}
	projectState.LastError = sql.NullString{}

	tx, err := handler.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin project repository bind transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := geckodb.ConfigPUTGenericTxContext(ctx, tx, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		return fmt.Errorf("update project config: %w", err)
	}
	if err := geckodb.UpsertGitProjectStateTxContext(ctx, tx, *projectState); err != nil {
		return fmt.Errorf("update project git state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project repository bind transaction: %w", err)
	}
	return nil
}

func (handler *Handler) unbindProjectRepository(ctx context.Context, projectID string, projectCfg appconfig.ProjectConfig) error {
	projectCfg.SrcRepo = ""

	tx, err := handler.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin project repository unbind transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := geckodb.ConfigPUTGenericTxContext(ctx, tx, projectID, string(appconfig.TypeProjects), &projectCfg); err != nil {
		return fmt.Errorf("update project config: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM config_schema.git_project_state WHERE project_id = $1`, projectID); err != nil {
		return fmt.Errorf("delete project git state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project repository unbind transaction: %w", err)
	}
	return nil
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
