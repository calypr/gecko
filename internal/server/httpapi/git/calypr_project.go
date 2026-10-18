package git

import (
	"context"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) calyprProjectRequestContext(ctx fiber.Ctx) (string, string, string, *httputil.ErrorResponse) {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		return "", "", "", httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		return "", "", "", httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
	}
	return organization, project, authorizationHeader, nil
}

func (handler *Handler) handleCalyprProjectSetupPUT(ctx fiber.Ctx) error {
	organization, project, authorizationHeader, errResponse := handler.calyprProjectRequestContext(ctx)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}

	var request projectpkg.CalyprProjectSetupRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(ctx.Context(), 20*time.Second)
	defer cancel()
	response, err := handler.ProjectSetup.InitializeProject(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleCalyprProjectStoragePUT(ctx fiber.Ctx) error {
	organization, project, authorizationHeader, errResponse := handler.calyprProjectRequestContext(ctx)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	var request projectpkg.CalyprProjectStorageRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}

	setupCtx, cancel := context.WithTimeout(ctx.Context(), 20*time.Second)
	defer cancel()
	response, err := handler.ProjectSetup.PopulateStorage(setupCtx, authorizationHeader, organization, project, request)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}
