package git

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
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
			ProjectID:  projectID,
			Ref:        refName,
			Path:       strings.Trim(ctx.Params("*"), "/"),
			EntryCount: 0,
			Entries:    []git.GitTreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := git.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	treeOptions, treeOptionErr := buildGitTreeResponseOptions(ctx)
	if treeOptionErr != nil {
		response := httputil.NewError("invalid_request", treeOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	treeResponse, err := git.BuildGitTreeResponse(projectID, refName, path, repo, hash, treeOptions)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git tree: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(treeResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectManifestGET(ctx fiber.Ctx) error {
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
			handler.logger.Warning("failed to warm git mirror for %s manifest: %v", projectID, err)
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
		return httputil.JSON(&git.GitProjectManifestResponse{
			ProjectID:  projectID,
			Ref:        refName,
			Path:       strings.Trim(ctx.Params("*"), "/"),
			EntryCount: 0,
			HasMore:    false,
			Entries:    []git.GitTreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := git.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	manifestOptions, manifestOptionErr := buildGitManifestResponseOptions(ctx)
	if manifestOptionErr != nil {
		response := httputil.NewError("invalid_request", manifestOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	manifestResponse, err := git.BuildGitManifestResponse(projectID, refName, path, repo, hash, manifestOptions)
	if err != nil {
		statusCode := http.StatusNotFound
		if strings.Contains(strings.ToLower(err.Error()), "cursor") || strings.Contains(strings.ToLower(err.Error()), "limit") {
			statusCode = http.StatusBadRequest
		}
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git manifest: %s", err), statusCode, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		if statusCode == http.StatusBadRequest {
			response = httputil.NewError("invalid_request", err.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		}
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(manifestResponse, http.StatusOK).Write(ctx)
}

func parseOptionalBoolQuery(ctx fiber.Ctx, key string, defaultValue bool) (bool, error) {
	value := strings.TrimSpace(ctx.Query(key))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	return parsed, nil
}

func buildGitTreeResponseOptions(ctx fiber.Ctx) (git.GitTreeResponseOptions, error) {
	view := strings.TrimSpace(ctx.Query("view"))
	if strings.EqualFold(view, "manifest") {
		return git.GitTreeResponseOptions{}, nil
	}

	includeSize, err := parseOptionalBoolQuery(ctx, "include_size", false)
	if err != nil {
		return git.GitTreeResponseOptions{}, err
	}
	includeLastModified, err := parseOptionalBoolQuery(ctx, "include_last_modified", false)
	if err != nil {
		return git.GitTreeResponseOptions{}, err
	}
	includeLFSPointer, err := parseOptionalBoolQuery(ctx, "include_lfs_pointer", false)
	if err != nil {
		return git.GitTreeResponseOptions{}, err
	}

	limitValue := strings.TrimSpace(ctx.Query("limit"))
	limit := 0
	if limitValue != "" {
		parsedLimit, parseErr := strconv.Atoi(limitValue)
		if parseErr != nil || parsedLimit < 0 {
			return git.GitTreeResponseOptions{}, fmt.Errorf("limit must be a non-negative integer")
		}
		limit = parsedLimit
	}

	return git.GitTreeResponseOptions{
		IncludeSize:         includeSize,
		IncludeLastModified: includeLastModified,
		IncludeLFSPointer:   includeLFSPointer,
		Limit:               limit,
	}, nil
}

func buildGitManifestResponseOptions(ctx fiber.Ctx) (git.GitManifestResponseOptions, error) {
	filesOnly, err := parseOptionalBoolQuery(ctx, "files_only", true)
	if err != nil {
		return git.GitManifestResponseOptions{}, err
	}
	limit := 5000
	limitValue := strings.TrimSpace(ctx.Query("limit"))
	if limitValue != "" {
		parsedLimit, parseErr := strconv.Atoi(limitValue)
		if parseErr != nil || parsedLimit <= 0 {
			return git.GitManifestResponseOptions{}, fmt.Errorf("limit must be a positive integer")
		}
		if parsedLimit > 20000 {
			return git.GitManifestResponseOptions{}, fmt.Errorf("limit must be less than or equal to 20000")
		}
		limit = parsedLimit
	}
	return git.GitManifestResponseOptions{
		Limit:     limit,
		Cursor:    strings.TrimSpace(ctx.Query("cursor")),
		FilesOnly: filesOnly,
	}, nil
}

func (handler *Handler) handleGitProjectFileGET(ctx fiber.Ctx) error {
	organization, project, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, contentBytes, err := handler.gitService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, organization, project, identity, requestedRef, path)
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
	organization, project, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, _, err := handler.gitService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, organization, project, identity, requestedRef, path)
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
