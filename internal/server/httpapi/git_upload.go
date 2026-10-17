package httpapi

import (
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/upload"
	"github.com/gofiber/fiber/v3"
)

func (handler *GitHandler) handleGitProjectUploadSessionPOST(ctx fiber.Ctx) error {
	organization, project, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	var requestBody upload.SessionCreateRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	session, files, err := handler.upload().CreateSession(ctx.Context(), authorizationHeader, projectID, organization, project, identity, requestBody)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectUploadSessionGET(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	session, files, err := handler.upload().GetSession(ctx.Context(), projectID, sessionID)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectUploadSessionFilesPOST(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	var requestBody upload.AttachFilesRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID, "session_id": sessionID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	session, files, err := handler.upload().AttachFiles(ctx.Context(), projectID, sessionID, requestBody)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectUploadSessionFinalizePOST(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	var requestBody upload.FinalizeRequest
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID, "session_id": sessionID}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	session, files, err := handler.upload().Finalize(ctx.Context(), authorizationHeader, projectID, identity, sessionID, requestBody)
	if err != nil {
		return WriteAppError(ctx, err, handler.logger)
	}
	return httputil.JSON(upload.BuildSessionResponse(*session, files), http.StatusOK).Write(ctx)
}
