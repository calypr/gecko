package apiserver

import (
	"context"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/upload"
)

// PostGitProjectUploadSession implements POST /git/projects/{orgTitle}/{projectTitle}/uploads/session.
func (s *Server) PostGitProjectUploadSession(ctx context.Context, request gecko.PostGitProjectUploadSessionRequestObject) (gecko.PostGitProjectUploadSessionResponseObject, error) {
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
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), tokenErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	var body upload.SessionCreateRequest
	if err := decodeBody(*request.Body, &body, map[string]any{"project_id": resolved.ID}); err != nil {
		return nil, err
	}
	session, files, err := s.UploadService.CreateSession(ctx, authorizationHeader, resolved.ID, resolved.Organization, resolved.Name, resolved.Identity, body)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(upload.BuildSessionResponse(*session, files))
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectUploadSession200JSONResponse(m), nil
}

// GetGitProjectUploadSession implements GET /git/projects/{orgTitle}/{projectTitle}/uploads/session/{sessionID}.
func (s *Server) GetGitProjectUploadSession(ctx context.Context, request gecko.GetGitProjectUploadSessionRequestObject) (gecko.GetGitProjectUploadSessionResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	sessionID := strings.TrimSpace(request.SessionID)
	session, files, err := s.UploadService.GetSession(ctx, resolved.ID, sessionID)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(upload.BuildSessionResponse(*session, files))
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectUploadSession200JSONResponse(m), nil
}

// PostGitProjectUploadSessionFiles implements POST /git/projects/{orgTitle}/{projectTitle}/uploads/session/{sessionID}/files.
func (s *Server) PostGitProjectUploadSessionFiles(ctx context.Context, request gecko.PostGitProjectUploadSessionFilesRequestObject) (gecko.PostGitProjectUploadSessionFilesResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	sessionID := strings.TrimSpace(request.SessionID)
	if request.JSONBody == nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "request body is required", map[string]any{"project_id": resolved.ID, "session_id": sessionID})
	}
	var body upload.AttachFilesRequest
	if err := decodeBody(map[string]interface{}(*request.JSONBody), &body, map[string]any{"project_id": resolved.ID, "session_id": sessionID}); err != nil {
		return nil, err
	}
	session, files, err := s.UploadService.AttachFiles(ctx, resolved.ID, sessionID, body)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(upload.BuildSessionResponse(*session, files))
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectUploadSessionFiles200JSONResponse(m), nil
}

// PostGitProjectUploadSessionFinalize implements POST /git/projects/{orgTitle}/{projectTitle}/uploads/session/{sessionID}/finalize.
func (s *Server) PostGitProjectUploadSessionFinalize(ctx context.Context, request gecko.PostGitProjectUploadSessionFinalizeRequestObject) (gecko.PostGitProjectUploadSessionFinalizeResponseObject, error) {
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
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), tokenErr.Error(), map[string]any{"project_id": resolved.ID})
	}
	sessionID := strings.TrimSpace(request.SessionID)
	var body upload.FinalizeRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": resolved.ID, "session_id": sessionID}); err != nil {
			return nil, err
		}
	}
	session, files, err := s.UploadService.Finalize(ctx, authorizationHeader, resolved.ID, resolved.Identity, sessionID, body)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(upload.BuildSessionResponse(*session, files))
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectUploadSessionFinalize200JSONResponse(m), nil
}
