package httpapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/gofiber/fiber/v3"
)

func (handler *GitHandler) handleCalyprProjectSetupPUT(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	var request projectpkg.CalyprProjectSetupRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(ctx.Context(), 20*time.Second)
	defer cancel()
	response, err := handler.projectSetup.InitializeProject(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return WriteAppError(ctx, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleCalyprProjectStoragePUT(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	var request projectpkg.CalyprProjectStorageRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(ctx.Context(), 20*time.Second)
	defer cancel()
	response, err := handler.projectSetup.PopulateStorage(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return WriteAppError(ctx, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}
