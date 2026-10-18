package git

import (
	"context"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

type gitOrganizationConnectResponse struct {
	Mode           string                                `json:"mode"`
	RedirectURL    string                                `json:"redirect_url,omitempty"`
	InstallationID *int64                                `json:"installation_id,omitempty"`
	Repositories   []domain.GitHubInstallationRepository `json:"repositories,omitempty"`
}

func (handler *Handler) handleGitOrganizationInitConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	type initConnectRequest struct {
		RedirectPath       string `json:"redirect_path"`
		Project            string `json:"project"`
		RepositoryFullName string `json:"repository_full_name"`
	}
	requestBody := initConnectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization}); errResponse != nil {
			errResponse.WriteLog(handler.Logger)
			return errResponse.Write(ctx)
		}
	}
	redirectPath := strings.TrimSpace(requestBody.RedirectPath)
	if redirectPath == "" {
		redirectPath = "/git"
	}
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	redirectURL, err := handler.ProjectInstallation.InitConnect(connectCtx, organization, authorizationHeader, redirectPath, requestBody.RepositoryFullName)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(gitOrganizationConnectResponse{
		Mode:        "redirect",
		RedirectURL: redirectURL,
	}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitOrganizationConnectPOST(ctx fiber.Ctx) error {
	organization := ctx.Params("orgTitle")
	if organization == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization is required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	type connectRequest struct {
		InstallationID *int64 `json:"installation_id"`
		GitHubOwner    string `json:"github_owner"`
	}
	requestBody := connectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization}); errResponse != nil {
			errResponse.WriteLog(handler.Logger)
			return errResponse.Write(ctx)
		}
	}
	if requestBody.InstallationID == nil || *requestBody.InstallationID <= 0 {
		response := httputil.NewError(apierror.Type("invalid_request"), "installation_id is required", http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	githubOwner := strings.TrimSpace(requestBody.GitHubOwner)
	if githubOwner == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "github_owner is required", http.StatusBadRequest, map[string]any{"organization": organization}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	repositories, err := handler.ProjectInstallation.ConnectOrganization(connectCtx, organization, authorizationHeader, githubOwner, *requestBody.InstallationID)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(gitOrganizationConnectResponse{
		Mode:           "select_repository",
		InstallationID: requestBody.InstallationID,
		Repositories:   repositories,
	}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectEditConnectPOST(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError(apierror.Type("invalid_request"), "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	type editConnectRequest struct {
		RepositoryFullName string `json:"repository_full_name"`
	}
	requestBody := editConnectRequest{}
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"organization": organization, "project": project}); errResponse != nil {
			errResponse.WriteLog(handler.Logger)
			return errResponse.Write(ctx)
		}
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	connectCtx, cancel := context.WithTimeout(ctx.Context(), 30*time.Second)
	defer cancel()
	mode, err := handler.ProjectInstallation.ConnectProject(connectCtx, organization, project, authorizationHeader, requestBody.RepositoryFullName)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(gitOrganizationConnectResponse{Mode: mode}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUpdatePOST(ctx fiber.Ctx) error {
	resolved, err := handler.resolveGitProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	refreshCtx, cancel := context.WithTimeout(ctx.Context(), 60*time.Second)
	defer cancel()
	refreshResponse, err := handler.ProjectInstallation.UpdateProject(refreshCtx, resolved, authorizationHeader)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(refreshResponse, http.StatusOK).Write(ctx)
}
