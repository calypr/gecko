package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func parseInstallationID(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("installation_id is required")
	}
	installationID, err := strconv.ParseInt(value, 10, 64)
	if err != nil || installationID <= 0 {
		return 0, fmt.Errorf("installation_id must be a positive integer")
	}
	return installationID, nil
}

func (handler *Handler) handleGitHubWebhookPOST(ctx fiber.Ctx) error {
	event := strings.TrimSpace(ctx.Get("X-GitHub-Event"))
	signature := strings.TrimSpace(ctx.Get("X-Hub-Signature-256"))
	body := append([]byte(nil), ctx.Body()...)

	if err := handler.gitService.VerifyWebhookSignature(body, signature); err != nil {
		response := httputil.NewError(apierror.TypeForbidden, err.Error(), http.StatusUnauthorized, map[string]any{"event": event}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	webhookCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := handler.gitService.HandleGitHubWebhook(webhookCtx, handler.db, event, body); err != nil {
		response := httputil.NewError(apierror.Type("integration_error"), fmt.Sprintf("failed to process github webhook: %s", err), http.StatusBadGateway, map[string]any{"event": event}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(map[string]any{"accepted": true}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitPendingRepositoriesGET(ctx fiber.Ctx) error {
	userID, userErr := handler.authenticatedUserID(ctx)
	if userErr != nil {
		userErr.WriteLog(handler.logger)
		return userErr.Write(ctx)
	}
	var installationID int64
	var err error
	if strings.TrimSpace(ctx.Query("installation_id")) != "" {
		installationID, err = parseInstallationID(ctx.Query("installation_id"))
		if err != nil {
			response := httputil.NewError(apierror.Type("invalid_request"), err.Error(), http.StatusBadRequest, nil, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	}
	setupSessionID := strings.TrimSpace(ctx.Query("setup_session_id"))
	records, listErr := geckodb.ListGitPendingRepositoriesByUser(handler.db, userID, installationID, setupSessionID)
	if listErr != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to list pending repositories: %s", listErr), http.StatusInternalServerError, map[string]any{"installation_id": installationID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	payload := git.GitPendingRepositoriesResponse{
		InstallationID: installationID,
		SetupSessionID: setupSessionID,
		Pending:        make([]git.GitPendingRepository, 0, len(records)),
	}
	for _, record := range records {
		payload.Pending = append(payload.Pending, git.PendingRepositoryResponse(record))
	}
	return httputil.JSON(payload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitPendingRepositoriesReconcilePOST(ctx fiber.Ctx) error {
	request := git.GitPendingRepositoriesReconcileRequest{}
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, nil); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if request.InstallationID <= 0 {
		response := httputil.NewError(apierror.Type("invalid_request"), "installation_id must be a positive integer", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	userID, userErr := handler.authenticatedUserID(ctx)
	if userErr != nil {
		userErr.WriteLog(handler.logger)
		return userErr.Write(ctx)
	}
	var setupSession *geckodb.GitSetupSession
	setupSessionID := strings.TrimSpace(request.SetupSessionID)
	if setupSessionID != "" {
		var sessionErr error
		setupSession, sessionErr = geckodb.GitSetupSessionByID(handler.db, setupSessionID)
		if sessionErr != nil {
			response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read setup session: %s", sessionErr), http.StatusInternalServerError, map[string]any{"setup_session_id": setupSessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		if setupSession == nil || setupSession.CreatedByUserID != userID {
			response := httputil.NewError(apierror.TypeForbidden, "setup session is not available for this user", http.StatusForbidden, map[string]any{"setup_session_id": setupSessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		if request.InstallationID > 0 && (!setupSession.InstallationID.Valid || setupSession.InstallationID.Int64 != request.InstallationID) {
			setupSession.InstallationID = sqlNullInt64(request.InstallationID)
			setupSession.UpdatedAt = time.Now().UTC()
			if err := geckodb.UpsertGitSetupSession(handler.db, *setupSession); err != nil {
				response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to update setup session: %s", err), http.StatusInternalServerError, map[string]any{"setup_session_id": setupSessionID}, nil)
				response.WriteLog(handler.logger)
				return response.Write(ctx)
			}
		}
	}
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	records, err := handler.gitService.ReconcilePendingRepositories(reconcileCtx, handler.db, ctx.Get("Authorization"), request.InstallationID, setupSession)
	if err != nil {
		statusCode := http.StatusBadGateway
		errorType := apierror.Type("integration_error")
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			errorType = apierror.Type(statusErr.Code)
		}
		response := httputil.NewError(errorType, fmt.Sprintf("failed to reconcile pending repositories: %s", err), statusCode, map[string]any{"installation_id": request.InstallationID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	payload := git.GitPendingRepositoriesResponse{
		InstallationID: request.InstallationID,
		SetupSessionID: setupSessionID,
		Pending:        make([]git.GitPendingRepository, 0, len(records)),
	}
	for _, record := range records {
		payload.Pending = append(payload.Pending, git.PendingRepositoryResponse(record))
	}
	return httputil.JSON(payload, http.StatusOK).Write(ctx)
}

func sqlNullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: value > 0}
}
