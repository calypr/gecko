package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/domain"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
)

func (s *Server) resolveGitProject(ctx context.Context, organization, project string) (projectpkg.ResolvedProject, *opError) {
	resolved, err := s.ProjectRead.Resolve(ctx, organization, project)
	if err != nil {
		return projectpkg.ResolvedProject{}, fromAppError(err)
	}
	return resolved, nil
}

// ListGitProjects implements GET /git/projects.
func (s *Server) ListGitProjects(ctx context.Context, request gecko.ListGitProjectsRequestObject) (gecko.ListGitProjectsResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	allowedResources, errResponse := access.AllowedReadResources(ctx, AuthorizationFromContext(ctx))
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	statuses, err := s.ProjectRead.ListStatuses(ctx, allowedResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	out := make([]gecko.GitProjectStatusResponse, 0, len(statuses))
	for _, status := range statuses {
		item, mapErr := toMap(status)
		if mapErr != nil {
			return nil, mapErr
		}
		out = append(out, item)
	}
	return gecko.ListGitProjects200JSONResponse(out), nil
}

// GetGitProject implements GET /git/projects/{orgTitle}/{projectTitle}.
func (s *Server) GetGitProject(ctx context.Context, request gecko.GetGitProjectRequestObject) (gecko.GetGitProjectResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	status, err := s.ProjectRead.Status(ctx, organization, project, strings.TrimSpace(AuthorizationFromContext(ctx)))
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(status)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProject200JSONResponse(m), nil
}

// GetGitProjectRefs implements GET /git/projects/{orgTitle}/{projectTitle}/refs.
func (s *Server) GetGitProjectRefs(ctx context.Context, request gecko.GetGitProjectRefsRequestObject) (gecko.GetGitProjectRefsResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	mirror, err := s.ProjectRead.OpenMirror(ctx, strings.TrimSpace(AuthorizationFromContext(ctx)), resolved, false)
	if err != nil {
		return nil, fromAppError(err)
	}
	refsResponse, err := repo.BuildRefsResponse(resolved.ID, mirror.DefaultBranch, mirror.Repository)
	if err != nil {
		return nil, newOpError(http.StatusBadGateway, "integration_error", fmt.Sprintf("failed to read git refs: %s", err), map[string]any{"project_id": resolved.ID})
	}
	m, err := toMap(refsResponse)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectRefs200JSONResponse(m), nil
}

// GetGitProjectTree implements GET /git/projects/{orgTitle}/{projectTitle}/tree.
func (s *Server) GetGitProjectTree(ctx context.Context, request gecko.GetGitProjectTreeRequestObject) (gecko.GetGitProjectTreeResponseObject, error) {
	m, opErr := s.buildTreeResponse(ctx, strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle), "", queryValue(request.Params.Ref), treeQueryParams{
		view:              queryValue(request.Params.View),
		includeSize:       queryValue(request.Params.IncludeSize),
		includeLastMod:    queryValue(request.Params.IncludeLastModified),
		includeLfsPointer: queryValue(request.Params.IncludeLfsPointer),
		limit:             queryInt(request.Params.Limit),
	})
	if opErr != nil {
		return nil, opErr
	}
	return gecko.GetGitProjectTree200JSONResponse(m), nil
}

// GetGitProjectTreeSubpath implements GET /git/projects/{orgTitle}/{projectTitle}/tree/{path}.
func (s *Server) GetGitProjectTreeSubpath(ctx context.Context, request gecko.GetGitProjectTreeSubpathRequestObject) (gecko.GetGitProjectTreeSubpathResponseObject, error) {
	m, opErr := s.buildTreeResponse(ctx, strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle), decodePathParam(request.Path), queryValue(request.Params.Ref), treeQueryParams{
		view:              queryValue(request.Params.View),
		includeSize:       queryValue(request.Params.IncludeSize),
		includeLastMod:    queryValue(request.Params.IncludeLastModified),
		includeLfsPointer: queryValue(request.Params.IncludeLfsPointer),
		limit:             queryInt(request.Params.Limit),
	})
	if opErr != nil {
		return nil, opErr
	}
	return gecko.GetGitProjectTreeSubpath200JSONResponse(m), nil
}

type treeQueryParams struct {
	view              string
	includeSize       string
	includeLastMod    string
	includeLfsPointer string
	limit             string
}

func (s *Server) buildTreeResponse(ctx context.Context, organization, project, path, ref string, params treeQueryParams) (map[string]interface{}, error) {
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	mirror, err := s.ProjectRead.OpenMirror(ctx, strings.TrimSpace(AuthorizationFromContext(ctx)), resolved, false)
	if err != nil {
		return nil, fromAppError(err)
	}
	if mirror.Empty {
		refName := strings.TrimSpace(ref)
		if refName == "" {
			refName = mirror.DefaultBranch
		}
		return toMap(&repo.TreeResponse{
			ProjectID:  resolved.ID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			Entries:    []repo.TreeEntry{},
		})
	}
	refName, hash, err := repo.ResolveGitReference(mirror.Repository, strings.TrimSpace(ref), mirror.DefaultBranch)
	if err != nil {
		return nil, newOpError(http.StatusNotFound, "not_found", fmt.Sprintf("failed to resolve git ref: %s", err), map[string]any{"project_id": resolved.ID, "ref": ref})
	}
	treeOptions, optErr := buildTreeOptions(params)
	if optErr != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", optErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	treeResponse, err := repo.BuildTreeResponse(resolved.ID, refName, path, mirror.Repository, hash, treeOptions)
	if err != nil {
		return nil, newOpError(http.StatusNotFound, "not_found", fmt.Sprintf("failed to read git tree: %s", err), map[string]any{"project_id": resolved.ID, "ref": refName, "path": path})
	}
	return toMap(treeResponse)
}

func buildTreeOptions(params treeQueryParams) (repo.TreeResponseOptions, error) {
	if strings.EqualFold(params.view, "manifest") {
		return repo.TreeResponseOptions{}, nil
	}
	includeSize, err := parseOptionalBool(params.includeSize, false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}
	includeLastModified, err := parseOptionalBool(params.includeLastMod, false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}
	includeLFSPointer, err := parseOptionalBool(params.includeLfsPointer, false)
	if err != nil {
		return repo.TreeResponseOptions{}, err
	}
	limit := 0
	if params.limit != "" {
		parsedLimit, parseErr := strconv.Atoi(params.limit)
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

// GetGitProjectManifest implements GET /git/projects/{orgTitle}/{projectTitle}/manifest.
func (s *Server) GetGitProjectManifest(ctx context.Context, request gecko.GetGitProjectManifestRequestObject) (gecko.GetGitProjectManifestResponseObject, error) {
	m, opErr := s.buildManifestResponse(ctx, strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle), "", queryValue(request.Params.Ref), manifestQueryParams{
		filesOnly: queryValue(request.Params.FilesOnly),
		limit:     queryInt(request.Params.Limit),
		cursor:    queryValue(request.Params.Cursor),
	})
	if opErr != nil {
		return nil, opErr
	}
	return gecko.GetGitProjectManifest200JSONResponse(m), nil
}

// GetGitProjectManifestWildcard implements GET /git/projects/{orgTitle}/{projectTitle}/manifest/{path}.
func (s *Server) GetGitProjectManifestWildcard(ctx context.Context, request gecko.GetGitProjectManifestWildcardRequestObject) (gecko.GetGitProjectManifestWildcardResponseObject, error) {
	m, opErr := s.buildManifestResponse(ctx, strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle), decodePathParam(request.Path), queryValue(request.Params.Ref), manifestQueryParams{
		filesOnly: queryValue(request.Params.FilesOnly),
		limit:     queryInt(request.Params.Limit),
		cursor:    queryValue(request.Params.Cursor),
	})
	if opErr != nil {
		return nil, opErr
	}
	return gecko.GetGitProjectManifestWildcard200JSONResponse(m), nil
}

type manifestQueryParams struct {
	filesOnly string
	limit     string
	cursor    string
}

func (s *Server) buildManifestResponse(ctx context.Context, organization, project, path, ref string, params manifestQueryParams) (map[string]interface{}, error) {
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	mirror, err := s.ProjectRead.OpenMirror(ctx, strings.TrimSpace(AuthorizationFromContext(ctx)), resolved, false)
	if err != nil {
		return nil, fromAppError(err)
	}
	if mirror.Empty {
		refName := strings.TrimSpace(ref)
		if refName == "" {
			refName = mirror.DefaultBranch
		}
		return toMap(&repo.ManifestResponse{
			ProjectID:  resolved.ID,
			Ref:        refName,
			Path:       path,
			EntryCount: 0,
			HasMore:    false,
			Entries:    []repo.TreeEntry{},
		})
	}
	refName, hash, err := repo.ResolveGitReference(mirror.Repository, strings.TrimSpace(ref), mirror.DefaultBranch)
	if err != nil {
		return nil, newOpError(http.StatusNotFound, "not_found", fmt.Sprintf("failed to resolve git ref: %s", err), map[string]any{"project_id": resolved.ID, "ref": ref})
	}
	manifestOptions, optErr := buildManifestOptions(params)
	if optErr != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", optErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	manifestResponse, err := repo.BuildManifestResponse(resolved.ID, refName, path, mirror.Repository, hash, manifestOptions)
	if err != nil {
		details := map[string]any{"project_id": resolved.ID, "ref": refName, "path": path}
		if errors.Is(err, repo.ErrInvalidManifestCursor) {
			return nil, newOpError(http.StatusBadRequest, "invalid_request", err.Error(), details)
		}
		return nil, newOpError(http.StatusNotFound, "not_found", fmt.Sprintf("failed to read git manifest: %s", err), details)
	}
	return toMap(manifestResponse)
}

func buildManifestOptions(params manifestQueryParams) (repo.ManifestResponseOptions, error) {
	filesOnly, err := parseOptionalBool(params.filesOnly, true)
	if err != nil {
		return repo.ManifestResponseOptions{}, err
	}
	limit := 5000
	if params.limit != "" {
		parsedLimit, parseErr := strconv.Atoi(params.limit)
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
		Cursor:    strings.TrimSpace(params.cursor),
		FilesOnly: filesOnly,
	}, nil
}

// GetGitProjectFile implements GET /git/projects/{orgTitle}/{projectTitle}/file/{path}.
func (s *Server) GetGitProjectFile(ctx context.Context, request gecko.GetGitProjectFileRequestObject) (gecko.GetGitProjectFileResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if tokenErr != nil {
		return nil, newOpError(http.StatusUnauthorized, "missing_authorization", tokenErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	path := decodePathParam(request.Path)
	requestedRef := strings.TrimSpace(queryValue(request.Params.Ref))
	metadata, contentBytes, err := s.RepoService.GetGitHubFileMetadata(ctx, authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, requestedRef, path)
	if err != nil {
		return nil, gitFileError(err, "failed to read git file", resolved.ID, requestedRef, path)
	}
	fileResponse := repo.BuildFileResponse(resolved.ID, requestedRef, path, metadata, contentBytes)
	m, err := toMap(fileResponse)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectFile200JSONResponse(m), nil
}

// DownloadGitProjectFile implements GET /git/projects/{orgTitle}/{projectTitle}/download/{path}.
func (s *Server) DownloadGitProjectFile(ctx context.Context, request gecko.DownloadGitProjectFileRequestObject) (gecko.DownloadGitProjectFileResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if tokenErr != nil {
		return nil, newOpError(http.StatusUnauthorized, "missing_authorization", tokenErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	path := decodePathParam(request.Path)
	requestedRef := strings.TrimSpace(queryValue(request.Params.Ref))
	metadata, _, err := s.RepoService.GetGitHubFileMetadata(ctx, authorizationHeader, resolved.Organization, resolved.Name, resolved.Identity, requestedRef, path)
	if err != nil {
		return nil, gitFileError(err, "failed to download git file", resolved.ID, requestedRef, path)
	}
	if metadata == nil || strings.TrimSpace(metadata.GetDownloadURL()) == "" {
		return nil, newOpError(http.StatusBadGateway, "integration_error", "github download url is unavailable for this file", map[string]any{"project_id": resolved.ID, "ref": requestedRef, "path": path})
	}
	return downloadRedirect{location: strings.TrimSpace(metadata.GetDownloadURL())}, nil
}

type downloadRedirect struct {
	location string
}

func (d downloadRedirect) VisitDownloadGitProjectFileResponse(w http.ResponseWriter) error {
	w.Header().Set("Location", d.location)
	w.WriteHeader(http.StatusFound)
	return nil
}

func gitFileError(err error, fallbackMessage, projectID, ref, path string) *opError {
	statusCode := http.StatusNotFound
	code := "not_found"
	message := fmt.Sprintf("%s: %s", fallbackMessage, err)
	if statusErr, ok := err.(*domain.HTTPStatusError); ok {
		statusCode = statusErr.StatusCode
		code = statusErr.Code
		message = statusErr.Message
	}
	return newOpError(statusCode, code, message, map[string]any{"project_id": projectID, "ref": ref, "path": path})
}

func queryValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func parseOptionalBool(raw string, defaultValue bool) (bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("value must be true or false")
	}
	return parsed, nil
}

// decodePathParam trims surrounding slashes from a captured subpath. The value
// already arrives percent-decoded: Go ServeMux's PathValue decodes it and the
// generated binding binds it with ValueIsUnescaped, so unescaping again here
// would double-decode filenames containing a literal '%'.
func decodePathParam(raw string) string {
	return strings.Trim(raw, "/")
}
