package git

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleCalyprProjectSetupPUT(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	var request git.CalyprProjectSetupRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := handler.projectSetup.InitializeProject(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleCalyprProjectStoragePUT(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	var request git.CalyprProjectStorageRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := handler.projectSetup.PopulateStorage(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return writeAppError(ctx, handler.logger, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}
