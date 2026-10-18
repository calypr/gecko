package git

import (
	"errors"
	"fmt"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/repo"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectRefsGET(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	mirror, err := handler.ProjectRead.OpenMirror(ctx.Context(), strings.TrimSpace(ctx.Get("Authorization")), resolved, false)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	refsResponse, err := repo.BuildRefsResponse(resolved.ID, mirror.DefaultBranch, mirror.Repository)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to read git refs: %s", err), http.StatusBadGateway, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refsResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectTreeGET(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	mirror, err := handler.ProjectRead.OpenMirror(ctx.Context(), strings.TrimSpace(ctx.Get("Authorization")), resolved, false)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	if mirror.Empty {
		refName := strings.TrimSpace(ctx.Query("ref"))
		if refName == "" {
			refName = mirror.DefaultBranch
		}
		path, pathErr := decodeGitWildcardPath(ctx)
		if pathErr != nil {
			response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
			response.WriteLog(handler.Logger)
			return response.Write(ctx)
		}
		return httputil.JSON(&repo.TreeResponse{
			ProjectID:  resolved.ID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			Entries:    []repo.TreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := repo.ResolveGitReference(mirror.Repository, strings.TrimSpace(ctx.Query("ref")), mirror.DefaultBranch)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": resolved.ID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	treeOptions, treeOptionErr := buildGitTreeResponseOptions(ctx)
	if treeOptionErr != nil {
		response := httputil.NewError("invalid_request", treeOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	treeResponse, err := repo.BuildTreeResponse(resolved.ID, refName, path, mirror.Repository, hash, treeOptions)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git tree: %s", err), http.StatusNotFound, map[string]any{"project_id": resolved.ID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	return httputil.JSON(treeResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectManifestGET(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	mirror, err := handler.ProjectRead.OpenMirror(ctx.Context(), strings.TrimSpace(ctx.Get("Authorization")), resolved, false)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	if mirror.Empty {
		refName := strings.TrimSpace(ctx.Query("ref"))
		if refName == "" {
			refName = mirror.DefaultBranch
		}
		path, pathErr := decodeGitWildcardPath(ctx)
		if pathErr != nil {
			response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
			response.WriteLog(handler.Logger)
			return response.Write(ctx)
		}
		return httputil.JSON(&repo.ManifestResponse{
			ProjectID:  resolved.ID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			HasMore:    false,
			Entries:    []repo.TreeEntry{},
		}, http.StatusOK).Write(ctx)
	}
	refName, hash, err := repo.ResolveGitReference(mirror.Repository, strings.TrimSpace(ctx.Query("ref")), mirror.DefaultBranch)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": resolved.ID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	manifestOptions, manifestOptionErr := buildGitManifestResponseOptions(ctx)
	if manifestOptionErr != nil {
		response := httputil.NewError("invalid_request", manifestOptionErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	manifestResponse, err := repo.BuildManifestResponse(resolved.ID, refName, path, mirror.Repository, hash, manifestOptions)
	if err != nil {
		details := map[string]any{"project_id": resolved.ID, "ref": refName, "path": path}
		if errors.Is(err, repo.ErrInvalidManifestCursor) {
			response := httputil.NewError("invalid_request", err.Error(), http.StatusBadRequest, details, nil)
			response.WriteLog(handler.Logger)
			return response.Write(ctx)
		}
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git manifest: %s", err), http.StatusNotFound, details, nil)
		response.WriteLog(handler.Logger)
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

func (handler *Handler) handleGitProjectFileGET(ctx fiber.Ctx) error {
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
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, contentBytes, err := handler.RepoService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to read git file: %s", err)
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			code = statusErr.Code
			message = statusErr.Message
		}
		response := httputil.NewError(apierror.Type(code), message, statusCode, map[string]any{"project_id": resolved.ID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	fileResponse := repo.BuildFileResponse(resolved.ID, requestedRef, path, metadata, contentBytes)
	return httputil.JSON(fileResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectDownloadGET(ctx fiber.Ctx) error {
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
	path, pathErr := decodeGitWildcardPath(ctx)
	if pathErr != nil {
		response := httputil.NewError("invalid_request", pathErr.Error(), http.StatusBadRequest, map[string]any{"project_id": resolved.ID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	requestedRef := strings.TrimSpace(ctx.Query("ref"))
	metadata, _, err := handler.RepoService.GetGitHubFileMetadata(ctx.Context(), authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, requestedRef, path)
	if err != nil {
		statusCode := http.StatusNotFound
		code := "not_found"
		message := fmt.Sprintf("failed to download git file: %s", err)
		if statusErr, ok := err.(*domain.HTTPStatusError); ok {
			statusCode = statusErr.StatusCode
			code = statusErr.Code
			message = statusErr.Message
		}
		response := httputil.NewError(apierror.Type(code), message, statusCode, map[string]any{"project_id": resolved.ID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	if metadata == nil || strings.TrimSpace(metadata.GetDownloadURL()) == "" {
		response := httputil.NewError("integration_error", "github download url is unavailable for this file", http.StatusBadGateway, map[string]any{"project_id": resolved.ID, "ref": requestedRef, "path": path}, nil)
		response.WriteLog(handler.Logger)
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
