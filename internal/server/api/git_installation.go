package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

func (handler *Handler) handleGitOrganizationConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	userID, userErr := handler.authenticatedUserID(ctx)
	if userErr != nil {
		userErr.WriteLog(handler.logger)
		return userErr.Write(ctx)
	}
	requestBody := map[string]string{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	redirectPath := strings.TrimSpace(requestBody["redirect_path"])
	if redirectPath == "" {
		redirectPath = "/git"
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	beforeRepoIDs := []int64{}
	var installationID sql.NullInt64
	if status, statusErr := handler.gitService.RequestOrganizationInstallationStatus(connectCtx, authorizationHeader, organization); statusErr != nil {
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to inspect existing GitHub App installation: %s", statusErr), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	} else if status.Installed && status.InstallationID != nil {
		installationID = sql.NullInt64{Int64: *status.InstallationID, Valid: true}
		repositories, listErr := handler.gitService.ListInstallationRepositories(connectCtx, authorizationHeader, *status.InstallationID)
		if listErr != nil {
			response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to snapshot existing GitHub App repositories: %s", listErr), http.StatusBadGateway, map[string]any{"organization": organization}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		for _, repository := range repositories {
			beforeRepoIDs = append(beforeRepoIDs, repository.ID)
		}
	}
	setupSessionID := uuid.NewString()
	now := time.Now().UTC()
	setupSession := geckodb.GitSetupSession{
		ID:              setupSessionID,
		CreatedByUserID: userID,
		Organization:    organization,
		InstallationID:  installationID,
		BeforeRepoIDs:   geckodb.EncodeRepoIDs(beforeRepoIDs),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := geckodb.UpsertGitSetupSession(handler.db, setupSession); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to create GitHub setup session: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	redirectURL, err := handler.gitService.RequestInstallationURL(
		connectCtx,
		authorizationHeader,
		organization,
		git.GitSetupState(setupSessionID),
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
	_ = redirectPath
	return httputil.JSON(git.GitOrganizationConnectResponse{RedirectURL: redirectURL, SetupSessionID: setupSessionID}, http.StatusOK).Write(ctx)
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
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	accessToken, err := handler.gitService.RequestInstallationToken(refreshCtx, authorizationHeader, identity, "read")
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
