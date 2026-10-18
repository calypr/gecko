package git

import (
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/upload"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectUploadSessionPOST(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	var requestBody upload.SessionCreateRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": resolved.ID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	session, files, err := handler.UploadService.CreateSession(ctx.Context(), authorizationHeader, resolved.ID, resolved.Organization, resolved.Name, resolved.Identity, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUploadSessionGET(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	session, files, err := handler.UploadService.GetSession(ctx.Context(), resolved.ID, sessionID)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUploadSessionFilesPOST(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	var requestBody upload.AttachFilesRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": resolved.ID, "session_id": sessionID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	session, files, err := handler.UploadService.AttachFiles(ctx.Context(), resolved.ID, sessionID, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUploadSessionFinalizePOST(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	var requestBody upload.FinalizeRequest
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": resolved.ID, "session_id": sessionID}); errResponse != nil {
			errResponse.WriteLog(handler.Logger)
			return errResponse.Write(ctx)
		}
	}
	session, files, err := handler.UploadService.Finalize(ctx.Context(), authorizationHeader, resolved.ID, resolved.Identity, sessionID, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}
