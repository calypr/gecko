package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/repo"
	"github.com/gofiber/fiber/v3"
)

func (handler *GitHandler) handleGitProjectRefsGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(ctx.Context(), projectID, identity)
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
		refreshCtx, cancel := context.WithTimeout(ctx.Context(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state, false)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s refs: %v", projectID, err)
		}
	}
	mirror, err := repo.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refsResponse, err := repo.BuildRefsResponse(projectID, state.DefaultBranch.String, mirror)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to read git refs: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refsResponse, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectTreeGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(ctx.Context(), projectID, identity)
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
		refreshCtx, cancel := context.WithTimeout(ctx.Context(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state, false)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s tree: %v", projectID, err)
		}
	}
	mirror, err := repo.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if repo.RepositoryIsEmpty(mirror) {
		refName := strings.TrimSpace(ctx.Query("ref"))
		if refName == "" {
			refName = state.DefaultBranch.String
		}
		path, pathErr := decodeGitWildcardPath(ctx)
		if pathErr != nil {
			response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		return httputil.JSON(&repo.TreeResponse{
			ProjectID:  projectID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			Entries:    []repo.TreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := repo.ResolveGitReference(mirror, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	treeOptions, treeOptionErr := buildGitTreeResponseOptions(ctx)
	if treeOptionErr != nil {
		response := httputil.NewError("invalid_request", treeOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	treeResponse, err := repo.BuildTreeResponse(projectID, refName, path, mirror, hash, treeOptions)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git tree: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(treeResponse, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectManifestGET(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := handler.loadGitProjectState(ctx.Context(), projectID, identity)
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
		refreshCtx, cancel := context.WithTimeout(ctx.Context(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state, false)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s manifest: %v", projectID, err)
		}
	}
	mirror, err := repo.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if repo.RepositoryIsEmpty(mirror) {
		refName := strings.TrimSpace(ctx.Query("ref"))
		if refName == "" {
			refName = state.DefaultBranch.String
		}
		path, pathErr := decodeGitWildcardPath(ctx)
		if pathErr != nil {
			response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		return httputil.JSON(&repo.ManifestResponse{
			ProjectID:  projectID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			HasMore:    false,
			Entries:    []repo.TreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := repo.ResolveGitReference(mirror, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	manifestOptions, manifestOptionErr := buildGitManifestResponseOptions(ctx)
	if manifestOptionErr != nil {
		response := httputil.NewError("invalid_request", manifestOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	manifestResponse, err := repo.BuildManifestResponse(projectID, refName, path, mirror, hash, manifestOptions)
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

func buildGitTreeResponseOptions(ctx fiber.Ctx) (repo.TreeResponseOptions, error) {
	view := strings.TrimSpace(ctx.Query("view"))
	if strings.EqualFold(view, "manifest") {
		return repo.TreeResponseOptions{}, nil
	}

	includeSize, err := parseOptionalBoolQuery(ctx, "include_size", false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}
	includeLastModified, err := parseOptionalBoolQuery(ctx, "include_last_modified", false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}
	includeLFSPointer, err := parseOptionalBoolQuery(ctx, "include_lfs_pointer", false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}

	limitValue := strings.TrimSpace(ctx.Query("limit"))
	limit := 0
	if limitValue != "" {
		parsedLimit, parseErr := strconv.Atoi(limitValue)
		if parseErr != nil || parsedLimit < 0 {
			return repo.TreeResponseOptions{}, fmt.Errorf("limit must be a non-negative integer")
		}
		limit = parsedLimit
	}

	return repo.TreeResponseOptions{
		IncludeSize:         includeSize,
		IncludeLastModified: includeLastModified,
		IncludeLFSPointer:   includeLFSPointer,
		Limit:               limit,
	}, nil
}

func buildGitManifestResponseOptions(ctx fiber.Ctx) (repo.ManifestResponseOptions, error) {
	filesOnly, err := parseOptionalBoolQuery(ctx, "files_only", true)
	if err != nil {
		return repo.ManifestResponseOptions{}, err
	}
	limit := 5000
	limitValue := strings.TrimSpace(ctx.Query("limit"))
	if limitValue != "" {
		parsedLimit, parseErr := strconv.Atoi(limitValue)
		if parseErr != nil || parsedLimit <= 0 {
			return repo.ManifestResponseOptions{}, fmt.Errorf("limit must be a positive integer")
		}
		if parsedLimit > 20000 {
			return repo.ManifestResponseOptions{}, fmt.Errorf("limit must be less than or equal to 20000")
		}
		limit = parsedLimit
	}
	return repo.ManifestResponseOptions{
		Limit:     limit,
		Cursor:    strings.TrimSpace(ctx.Query("cursor")),
		FilesOnly: filesOnly,
	}, nil
}

func (handler *GitHandler) handleGitProjectFileGET(ctx fiber.Ctx) error {
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
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, contentBytes, err := handler.repoService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, organization, project, identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to read git file: %s", err)
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			code = statusErr.Code
			message = statusErr.Message
		}
		response := httputil.NewError(apierror.Type(code), message, statusCode, map[string]any{"project_id": projectID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	fileResponse := repo.BuildFileResponse(projectID, requestedRef, path, metadata, contentBytes)
	return httputil.JSON(fileResponse, http.StatusOK).Write(ctx)
}

func (handler *GitHandler) handleGitProjectDownloadGET(ctx fiber.Ctx) error {
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
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, _, err := handler.repoService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, organization, project, identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to download git file: %s", err)
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
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

func decodeGitWildcardPath(ctx fiber.Ctx) (string, error) {
	rawPath := strings.Trim(ctx.Params("*"), "/")
	if rawPath == "" {
		return "", nil
	}
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", fmt.Errorf("invalid git path")
	}
	return strings.Trim(decodedPath, "/"), nil
}
