package apiserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apgen/server/gecko"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/thumbnail"
)

// PutCalyprProjectSetup implements PUT /git/projects/{orgTitle}/{projectTitle}/setup.
func (s *Server) PutCalyprProjectSetup(ctx context.Context, request gecko.PutCalyprProjectSetupRequestObject) (gecko.PutCalyprProjectSetupResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	// Validate the bearer format up front so a malformed header is a 401, not a
	// downstream 403 from Arborist, matching the legacy handler.
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), nil)
	}
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	var body projectpkg.CalyprProjectSetupRequest
	if err := decodeBody(*request.Body, &body, map[string]any{"organization": organization, "project": project}); err != nil {
		return nil, err
	}
	setupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	response, err := s.ProjectSetup.InitializeProject(setupCtx, authorizationHeader, organization, project, body)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PutCalyprProjectSetup200JSONResponse(m), nil
}

// PutCalyprProjectStorage implements PUT /git/projects/{orgTitle}/{projectTitle}/storage.
func (s *Server) PutCalyprProjectStorage(ctx context.Context, request gecko.PutCalyprProjectStorageRequestObject) (gecko.PutCalyprProjectStorageResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	if err := s.projectSetupAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	var body projectpkg.CalyprProjectStorageRequest
	if err := decodeBody(*request.Body, &body, map[string]any{"organization": organization, "project": project}); err != nil {
		return nil, err
	}
	setupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	response, err := s.ProjectSetup.PopulateStorage(setupCtx, AuthorizationFromContext(ctx), organization, project, body)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PutCalyprProjectStorage200JSONResponse(m), nil
}

// GetGitProjectThumbnail implements GET /git/projects/{orgTitle}/{projectTitle}/thumbnail.
func (s *Server) GetGitProjectThumbnail(ctx context.Context, request gecko.GetGitProjectThumbnailRequestObject) (gecko.GetGitProjectThumbnailResponseObject, error) {
	organization, project, projectID, opErr := s.resolveExistingProject(ctx, request.OrgTitle, request.ProjectTitle)
	if opErr != nil {
		return nil, opErr
	}
	path, contentType, err := s.ThumbnailStore.GetPath(organization, project)
	if err != nil {
		if errors.Is(err, thumbnail.ErrNoThumbnail) {
			return nil, newOpError(http.StatusNotFound, "not_found", fmt.Sprintf("no thumbnail found for %s", projectID), map[string]any{"project_id": projectID})
		}
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to load project thumbnail: %s", err), map[string]any{"project_id": projectID})
	}
	data, err := readFileBytes(path)
	if err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to read project thumbnail: %s", err), map[string]any{"project_id": projectID})
	}
	return gecko.GetGitProjectThumbnail200ImageResponse{
		Body:          bytes.NewReader(data),
		ContentType:   contentType,
		ContentLength: int64(len(data)),
	}, nil
}

// PutGitProjectThumbnail implements PUT /git/projects/{orgTitle}/{projectTitle}/thumbnail.
func (s *Server) PutGitProjectThumbnail(ctx context.Context, request gecko.PutGitProjectThumbnailRequestObject) (gecko.PutGitProjectThumbnailResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	project := strings.TrimSpace(request.ProjectTitle)
	// Authorize before resolving so an unknown project yields 401/403, not a
	// 404 existence oracle, matching the legacy middleware-first ordering.
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	_, _, projectID, opErr := s.resolveExistingProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	data, opErr := readMultipartThumbnail(request.Body)
	if opErr != nil {
		return nil, opErr
	}
	if int64(len(data)) > int64(thumbnail.MaxProjectThumbnailBytes) {
		return nil, newOpError(http.StatusRequestEntityTooLarge, "validation_failed", fmt.Sprintf("thumbnail image must be %d bytes or smaller", thumbnail.MaxProjectThumbnailBytes), map[string]any{"project_id": projectID})
	}
	_, contentType, err := s.ThumbnailStore.Save(organization, project, data)
	if err != nil {
		return nil, newOpError(http.StatusBadRequest, "validation_failed", fmt.Sprintf(
			"failed to save project thumbnail: %s. Allowed formats: PNG or JPG. Allowed dimensions: %dpx to %dpx. Maximum size: %d bytes",
			err,
			thumbnail.MinProjectThumbnailPixels,
			thumbnail.MaxProjectThumbnailPixels,
			thumbnail.MaxProjectThumbnailBytes,
		), map[string]any{"project_id": projectID})
	}
	success := true
	return gecko.PutGitProjectThumbnail200JSONResponse{
		ContentType: &contentType,
		ProjectId:   &projectID,
		Success:     &success,
	}, nil
}

// DeleteGitProjectThumbnail implements DELETE /git/projects/{orgTitle}/{projectTitle}/thumbnail.
func (s *Server) DeleteGitProjectThumbnail(ctx context.Context, request gecko.DeleteGitProjectThumbnailRequestObject) (gecko.DeleteGitProjectThumbnailResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	project := strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	_, _, projectID, opErr := s.resolveExistingProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	err := s.ThumbnailStore.Delete(organization, project)
	if err != nil && !errors.Is(err, thumbnail.ErrNoThumbnail) {
		return nil, newOpError(http.StatusBadGateway, "integration_error", fmt.Sprintf("failed to delete project thumbnail: %s", err), map[string]any{"project_id": projectID})
	}
	success := true
	return gecko.DeleteGitProjectThumbnail200JSONResponse{
		ProjectId: &projectID,
		Success:   &success,
	}, nil
}

// GetGitProjectPresentationConfig implements GET /git/projects/{orgTitle}/{projectTitle}/presentationConfig.
func (s *Server) GetGitProjectPresentationConfig(ctx context.Context, request gecko.GetGitProjectPresentationConfigRequestObject) (gecko.GetGitProjectPresentationConfigResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	project := strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "read"); err != nil {
		return nil, err
	}
	_, _, projectID, opErr := s.resolveExistingProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	if s.PresentationStore == nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", "presentation storage is not configured", map[string]any{"project_id": projectID})
	}
	content, err := s.PresentationStore.Get(organization, project)
	if err != nil {
		if errors.Is(err, presentation.ErrNoPresentation) {
			m, err := toMap(appconfig.PresentationConfig{})
			if err != nil {
				return nil, err
			}
			return gecko.GetGitProjectPresentationConfig200JSONResponse(m), nil
		}
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to load project presentation config: %s", err), map[string]any{"project_id": projectID})
	}
	m, err := toMap(appconfig.PresentationConfig{PresentationConfig: content})
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectPresentationConfig200JSONResponse(m), nil
}

// PutGitProjectPresentationConfig implements PUT /git/projects/{orgTitle}/{projectTitle}/presentationConfig.
func (s *Server) PutGitProjectPresentationConfig(ctx context.Context, request gecko.PutGitProjectPresentationConfigRequestObject) (gecko.PutGitProjectPresentationConfigResponseObject, error) {
	body, opErr := s.savePresentationConfig(ctx, request.OrgTitle, request.ProjectTitle, *request.Body)
	if opErr != nil {
		return nil, opErr
	}
	return gecko.PutGitProjectPresentationConfig200JSONResponse(body), nil
}

// PostGitProjectPresentationConfig implements POST /git/projects/{orgTitle}/{projectTitle}/presentationConfig.
func (s *Server) PostGitProjectPresentationConfig(ctx context.Context, request gecko.PostGitProjectPresentationConfigRequestObject) (gecko.PostGitProjectPresentationConfigResponseObject, error) {
	body, opErr := s.savePresentationConfig(ctx, request.OrgTitle, request.ProjectTitle, *request.Body)
	if opErr != nil {
		return nil, opErr
	}
	return gecko.PostGitProjectPresentationConfig200JSONResponse(body), nil
}

func (s *Server) savePresentationConfig(ctx context.Context, orgRaw, projectRaw string, body map[string]interface{}) (map[string]interface{}, *opError) {
	organization := strings.TrimSpace(orgRaw)
	project := strings.TrimSpace(projectRaw)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	_, _, projectID, opErr := s.resolveExistingProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	if s.PresentationStore == nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", "presentation storage is not configured", map[string]any{"project_id": projectID})
	}
	cfg := &appconfig.PresentationConfig{}
	if err := decodeBody(body, cfg, map[string]any{"project_id": projectID}); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, newOpError(http.StatusBadRequest, "validation_failed", fmt.Sprintf("body data validation failed: %s", err), map[string]any{"project_id": projectID})
	}
	if err := s.PresentationStore.Save(organization, project, cfg.PresentationConfig); err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to persist project presentation config: %s", err), map[string]any{"project_id": projectID})
	}
	return toMap(cfg)
}

// PostGitProjectEditConnect implements POST /git/projects/{orgTitle}/{projectTitle}/edit-connect.
func (s *Server) PostGitProjectEditConnect(ctx context.Context, request gecko.PostGitProjectEditConnectRequestObject) (gecko.PostGitProjectEditConnectResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	var body struct {
		RepositoryFullName string `json:"repository_full_name"`
	}
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"organization": organization, "project": project}); err != nil {
			return nil, err
		}
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if tokenErr != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), tokenErr.Error(), nil)
	}
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	mode, err := s.ProjectInstallation.ConnectProject(connectCtx, organization, project, authorizationHeader, body.RepositoryFullName)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(map[string]any{"mode": mode})
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectEditConnect200JSONResponse(m), nil
}

// PostGitProjectUpdate implements POST /git/projects/{orgTitle}/{projectTitle}/update.
func (s *Server) PostGitProjectUpdate(ctx context.Context, request gecko.PostGitProjectUpdateRequestObject) (gecko.PostGitProjectUpdateResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if tokenErr != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), tokenErr.Error(), nil)
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	refreshResponse, err := s.ProjectInstallation.UpdateProject(refreshCtx, resolved, authorizationHeader)
	if err != nil {
		return nil, fromAppError(err)
	}
	response := gecko.PostGitProjectUpdate200JSONResponse{
		Success:   &refreshResponse.Success,
		ProjectId: &refreshResponse.ProjectID,
		SyncState: &refreshResponse.SyncState,
	}
	if refreshResponse.DefaultBranch != "" {
		response.DefaultBranch = &refreshResponse.DefaultBranch
	}
	if refreshResponse.LastFetchedRef != "" {
		response.LastFetchedRef = &refreshResponse.LastFetchedRef
	}
	if refreshResponse.Error != "" {
		response.Error = &refreshResponse.Error
	}
	return response, nil
}

func (s *Server) resolveExistingProject(ctx context.Context, orgRaw, projectRaw string) (string, string, string, *opError) {
	organization := strings.TrimSpace(orgRaw)
	project := strings.TrimSpace(projectRaw)
	if organization == "" || project == "" {
		return "", "", "", newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	projectID := organization + "/" + project
	if _, err := s.ProjectRead.ProjectConfig(ctx, projectID); err != nil {
		return "", "", "", fromAppError(err)
	}
	return organization, project, projectID, nil
}

func readMultipartThumbnail(body *multipart.Reader) ([]byte, *opError) {
	if body == nil {
		return nil, newOpError(http.StatusBadRequest, "validation_failed", "thumbnail image is required", nil)
	}
	for {
		part, err := body.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, newOpError(http.StatusBadRequest, "invalid_request", fmt.Sprintf("failed to read thumbnail upload: %s", err), nil)
		}
		if part.FormName() != "thumbnail" {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(part, int64(thumbnail.MaxProjectThumbnailBytes)+1))
		if err != nil {
			return nil, newOpError(http.StatusBadRequest, "invalid_request", fmt.Sprintf("failed to read thumbnail upload: %s", err), nil)
		}
		return data, nil
	}
	return nil, newOpError(http.StatusBadRequest, "validation_failed", "thumbnail image is required", nil)
}

func readFileBytes(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return io.ReadAll(file)
}

