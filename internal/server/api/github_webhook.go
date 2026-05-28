package api

import (
	"context"
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
	installationID, err := parseInstallationID(ctx.Query("installation_id"))
	if err != nil {
		response := httputil.NewError(apierror.Type("invalid_request"), err.Error(), http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	records, listErr := geckodb.ListGitPendingRepositoriesByInstallation(handler.db, installationID)
	if listErr != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to list pending repositories: %s", listErr), http.StatusInternalServerError, map[string]any{"installation_id": installationID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	payload := git.GitPendingRepositoriesResponse{
		InstallationID: installationID,
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
	reconcileCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	records, err := handler.gitService.ReconcilePendingRepositories(reconcileCtx, handler.db, ctx.Get("Authorization"), request.InstallationID)
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
		Pending:        make([]git.GitPendingRepository, 0, len(records)),
	}
	for _, record := range records {
		payload.Pending = append(payload.Pending, git.PendingRepositoryResponse(record))
	}
	return httputil.JSON(payload, http.StatusOK).Write(ctx)
}
