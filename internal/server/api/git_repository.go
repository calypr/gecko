package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectRefsGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	if authorizationHeader != "" {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s refs: %v", projectID, err)
		}
	}
	repo, err := git.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refsResponse, err := git.BuildGitRefsResponse(projectID, state.DefaultBranch.String, repo)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to read git refs: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refsResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectTreeGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	if authorizationHeader != "" {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s tree: %v", projectID, err)
		}
	}
	repo, err := git.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if git.RepositoryIsEmpty(repo) {
		refName := strings.TrimSpace(ctx.Query("ref"))
		if refName == "" {
			refName = state.DefaultBranch.String
		}
		return httputil.JSON(&git.GitProjectTreeResponse{
			ProjectID: projectID,
			Ref:       refName,
			Path:      strings.Trim(ctx.Params("*"), "/"),
			Entries:   []git.GitTreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := git.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	treeResponse, err := git.BuildGitTreeResponse(projectID, refName, path, repo, hash)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git tree: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(treeResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectFileGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, contentBytes, err := handler.gitService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to read git file: %s", err)
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			code = statusErr.Code
			message = statusErr.Message
		}
		response := httputil.NewError(apierror.Type(code), message, statusCode, map[string]any{"project_id": projectID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	fileResponse := git.BuildGitHubFileResponse(projectID, requestedRef, path, metadata, contentBytes)
	return httputil.JSON(fileResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectDownloadGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, _, err := handler.gitService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to download git file: %s", err)
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			code = statusErr.Code
			message = statusErr.Message
		}
		response := httputil.NewError(apierror.Type(code), message, statusCode, map[string]any{"project_id": projectID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if metadata == nil || strings.TrimSpace(metadata.GetDownloadURL()) == "" {
		response := httputil.NewError("integration_error", "github download url is unavailable for this file", http.StatusBadGateway, map[string]any{"project_id": projectID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return ctx.Redirect().To(strings.TrimSpace(metadata.GetDownloadURL()))
}
