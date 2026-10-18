package git

import (
	"context"
	"fmt"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) resolveGitProject(ctx fiber.Ctx) (projectpkg.ResolvedProject, error) {
	return handler.ProjectRead.Resolve(ctx.Context(), ctx.Params("orgTitle"), ctx.Params("projectTitle"))
}

func (handler *Handler) handleGitProjectsGET(ctx fiber.Ctx) error {
	allowedResources, errResponse := access.AllowedReadResources(ctx.Context(), strings.TrimSpace(ctx.Get("Authorization")))
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	statuses, err := handler.ProjectRead.ListStatuses(ctx.Context(), allowedResources)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(statuses, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectGET(ctx fiber.Ctx) error {
	status, err := handler.ProjectRead.Status(
		ctx.Context(),
		ctx.Params("orgTitle"),
		ctx.Params("projectTitle"),
		strings.TrimSpace(ctx.Get("Authorization")),
	)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(status, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationStatusGET(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		response := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	projectIDs, err := handler.ProjectRead.ProjectIDs(ctx.Context())
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	statusAccess, errResponse := access.ReadStatusAccess(ctx.Context(), authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.ReadableResources
	if !access.ResourceListAllowsOrganization(allowedResources, organization) {
		response := httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	responsePayload, err := handler.ProjectSync.BuildSingleOrganizationStatus(ctx.Context(), authorizationHeader, organization, projectIDs, allowedResources)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	projectpkg.ApplyOrganizationStatusCapabilities(&responsePayload, statusAccess.Snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationsStatusGET(ctx fiber.Ctx) error {
	projectIDs, err := handler.ProjectRead.ProjectIDs(ctx.Context())
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	statusAccess, errResponse := access.ReadStatusAccess(ctx.Context(), authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.ReadableResources
	responsePayload, err := handler.ProjectSync.BuildOrganizationsStatus(ctx.Context(), authorizationHeader, projectIDs, allowedResources)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	projectpkg.ApplyOrganizationsStatusCapabilities(responsePayload.Organizations, statusAccess.Snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationReconcilePOST(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		response := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	statusAccess, errResponse := access.ReadStatusAccess(ctx.Context(), authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.ReadableResources
	if !access.ResourceListAllowsOrganization(allowedResources, organization) {
		response := httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	projectIDs, err := handler.ProjectRead.ProjectIDs(ctx.Context())
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	reconcileCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	if err := handler.ProjectSync.ReconcileOrganization(reconcileCtx, organization, authorizationHeader, projectIDs); err != nil {
		return core.WriteAppError(ctx, err)
	}
	statusAccess, errResponse = access.ReadStatusAccess(ctx.Context(), authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	allowedResources = statusAccess.ReadableResources
	responsePayload, err := handler.ProjectSync.BuildSingleOrganizationStatus(ctx.Context(), authorizationHeader, organization, projectIDs, allowedResources)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	projectpkg.ApplyOrganizationStatusCapabilities(&responsePayload, statusAccess.Snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationsReconcilePOST(ctx fiber.Ctx) error {
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	projectIDs, err := handler.ProjectRead.ProjectIDs(ctx.Context())
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	statusAccess, errResponse := access.ReadStatusAccess(ctx.Context(), authorizationHeader)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	allowedResources := statusAccess.ReadableResources
	projectIDs = access.FilterProjectIDsByAllowedResources(projectIDs, allowedResources)
	reconcileCtx, cancel := context.WithTimeout(ctx.Context(), 45*time.Second)
	defer cancel()
	if err := handler.ProjectSync.ReconcileOrganizations(reconcileCtx, authorizationHeader, projectIDs); err != nil {
		return core.WriteAppError(ctx, err)
	}
	responsePayload, err := handler.ProjectSync.BuildOrganizationsStatus(ctx.Context(), authorizationHeader, projectIDs, allowedResources)
	if err != nil {
		return core.WriteAppError(ctx, err)
	}
	projectpkg.ApplyOrganizationsStatusCapabilities(responsePayload.Organizations, statusAccess.Snapshot)
	return httputil.JSON(responsePayload, http.StatusOK).Write(ctx)
}
