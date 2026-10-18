package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/project"
)

// GetGitOrganizationsStatus implements GET /git/organizations/status.
func (s *Server) GetGitOrganizationsStatus(ctx context.Context, request gecko.GetGitOrganizationsStatusRequestObject) (gecko.GetGitOrganizationsStatusResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	projectIDs, err := s.ProjectRead.ProjectIDs(ctx)
	if err != nil {
		return nil, fromAppError(err)
	}
	token := AuthorizationFromContext(ctx)
	statusAccess, errResponse := access.ReadStatusAccess(ctx, token)
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	payload, err := s.ProjectSync.BuildOrganizationsStatus(ctx, token, projectIDs, statusAccess.ReadableResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	project.ApplyOrganizationsStatusCapabilities(payload.Organizations, statusAccess.Snapshot)
	m, err := toMap(payload)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitOrganizationsStatus200JSONResponse(m), nil
}

// ReconcileGitOrganizations implements POST /git/organizations/reconcile.
func (s *Server) ReconcileGitOrganizations(ctx context.Context, request gecko.ReconcileGitOrganizationsRequestObject) (gecko.ReconcileGitOrganizationsResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), nil)
	}
	projectIDs, err := s.ProjectRead.ProjectIDs(ctx)
	if err != nil {
		return nil, fromAppError(err)
	}
	statusAccess, errResponse := access.ReadStatusAccess(ctx, authorizationHeader)
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	allowedResources := statusAccess.ReadableResources
	projectIDs = access.FilterProjectIDsByAllowedResources(projectIDs, allowedResources)
	reconcileCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := s.ProjectSync.ReconcileOrganizations(reconcileCtx, authorizationHeader, projectIDs); err != nil {
		return nil, fromAppError(err)
	}
	payload, err := s.ProjectSync.BuildOrganizationsStatus(ctx, authorizationHeader, projectIDs, allowedResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	project.ApplyOrganizationsStatusCapabilities(payload.Organizations, statusAccess.Snapshot)
	m, err := toMap(payload)
	if err != nil {
		return nil, err
	}
	return gecko.ReconcileGitOrganizations200JSONResponse(m), nil
}

// InitConnectGitOrganization implements POST /git/organizations/{orgTitle}/init-connect.
func (s *Server) InitConnectGitOrganization(ctx context.Context, request gecko.InitConnectGitOrganizationRequestObject) (gecko.InitConnectGitOrganizationResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	organization := request.OrgTitle
	if organization == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization is required", nil)
	}
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), nil)
	}
	var body struct {
		RedirectPath       string `json:"redirect_path"`
		Project            string `json:"project"`
		RepositoryFullName string `json:"repository_full_name"`
	}
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"organization": organization}); err != nil {
			return nil, err
		}
	}
	redirectPath := strings.TrimSpace(body.RedirectPath)
	if redirectPath == "" {
		redirectPath = "/git"
	}
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	redirectURL, err := s.ProjectInstallation.InitConnect(connectCtx, organization, authorizationHeader, redirectPath, body.RepositoryFullName)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(map[string]any{"mode": "redirect", "redirect_url": redirectURL})
	if err != nil {
		return nil, err
	}
	return gecko.InitConnectGitOrganization200JSONResponse(m), nil
}

// ConnectGitOrganization implements POST /git/organizations/{orgTitle}/connect.
func (s *Server) ConnectGitOrganization(ctx context.Context, request gecko.ConnectGitOrganizationRequestObject) (gecko.ConnectGitOrganizationResponseObject, error) {
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	organization := request.OrgTitle
	if organization == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization is required", nil)
	}
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), nil)
	}
	var body struct {
		InstallationID *int64 `json:"installation_id"`
		GitHubOwner    string `json:"github_owner"`
	}
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"organization": organization}); err != nil {
			return nil, err
		}
	}
	if body.InstallationID == nil || *body.InstallationID <= 0 {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "installation_id is required", map[string]any{"organization": organization})
	}
	githubOwner := strings.TrimSpace(body.GitHubOwner)
	if githubOwner == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "github_owner is required", map[string]any{"organization": organization})
	}
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	repositories, err := s.ProjectInstallation.ConnectOrganization(connectCtx, organization, authorizationHeader, githubOwner, *body.InstallationID)
	if err != nil {
		return nil, fromAppError(err)
	}
	m, err := toMap(map[string]any{
		"mode":            "select_repository",
		"installation_id": *body.InstallationID,
		"repositories":    repositories,
	})
	if err != nil {
		return nil, err
	}
	return gecko.ConnectGitOrganization200JSONResponse(m), nil
}

// GetGitOrganizationStatus implements GET /git/organizations/{orgTitle}/status.
func (s *Server) GetGitOrganizationStatus(ctx context.Context, request gecko.GetGitOrganizationStatusRequestObject) (gecko.GetGitOrganizationStatusResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	if err := s.orgReadAuthz(ctx, organization); err != nil {
		return nil, err
	}
	if organization == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization is required", nil)
	}
	projectIDs, err := s.ProjectRead.ProjectIDs(ctx)
	if err != nil {
		return nil, fromAppError(err)
	}
	token := AuthorizationFromContext(ctx)
	statusAccess, errResponse := access.ReadStatusAccess(ctx, token)
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	allowedResources := statusAccess.ReadableResources
	if !access.ResourceListAllowsOrganization(allowedResources, organization) {
		return nil, newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), map[string]any{"organization": organization, "method": "read"})
	}
	payload, err := s.ProjectSync.BuildSingleOrganizationStatus(ctx, token, organization, projectIDs, allowedResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	project.ApplyOrganizationStatusCapabilities(&payload, statusAccess.Snapshot)
	m, err := toMap(payload)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitOrganizationStatus200JSONResponse(m), nil
}

// ReconcileGitOrganization implements POST /git/organizations/{orgTitle}/reconcile.
func (s *Server) ReconcileGitOrganization(ctx context.Context, request gecko.ReconcileGitOrganizationRequestObject) (gecko.ReconcileGitOrganizationResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	if err := s.orgReadAuthz(ctx, organization); err != nil {
		return nil, err
	}
	if organization == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization is required", nil)
	}
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), nil)
	}
	statusAccess, errResponse := access.ReadStatusAccess(ctx, authorizationHeader)
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	allowedResources := statusAccess.ReadableResources
	if !access.ResourceListAllowsOrganization(allowedResources, organization) {
		return nil, newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), map[string]any{"organization": organization, "method": "read"})
	}
	projectIDs, err := s.ProjectRead.ProjectIDs(ctx)
	if err != nil {
		return nil, fromAppError(err)
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.ProjectSync.ReconcileOrganization(reconcileCtx, organization, authorizationHeader, projectIDs); err != nil {
		return nil, fromAppError(err)
	}
	statusAccess, errResponse = access.ReadStatusAccess(ctx, authorizationHeader)
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	allowedResources = statusAccess.ReadableResources
	payload, err := s.ProjectSync.BuildSingleOrganizationStatus(ctx, authorizationHeader, organization, projectIDs, allowedResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	project.ApplyOrganizationStatusCapabilities(&payload, statusAccess.Snapshot)
	m, err := toMap(payload)
	if err != nil {
		return nil, err
	}
	return gecko.ReconcileGitOrganization200JSONResponse(m), nil
}
