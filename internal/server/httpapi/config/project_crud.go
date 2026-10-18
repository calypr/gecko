package config

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) buildProjectSummaryResponse(projectID string, cfg appconfig.ProjectConfig) ProjectSummaryResponse {
	projectOrganization, projectName, found := strings.Cut(projectID, "/")
	if !found {
		return ProjectSummaryResponse{}
	}

	organization := strings.TrimSpace(projectOrganization)
	project := strings.TrimSpace(projectName)
	title := strings.TrimSpace(cfg.Title)
	if title == "" {
		title = strings.TrimSpace(cfg.ProjectTitle)
	}
	if title == "" {
		title = project
	}

	summary := ProjectSummaryResponse{
		Organization: organization,
		Project:      project,
		Title:        title,
		ContactEmail: strings.TrimSpace(cfg.ContactEmail),
		Description:  strings.TrimSpace(cfg.Description),
	}

	if handler.ThumbnailStore != nil {
		if _, _, err := handler.ThumbnailStore.GetPath(organization, project); err == nil {
			summary.ThumbnailURL = fmt.Sprintf(
				"/gecko/git/projects/%s/%s/thumbnail",
				url.PathEscape(organization),
				url.PathEscape(project),
			)
		}
	}

	return summary
}

func (handler *Handler) handleProjectConfigGET(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigGETByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectSummaryGET(ctx fiber.Ctx) error {
	projects, err := handler.ConfigService.ListProjects(ctx.Context())
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	summaries := make([]ProjectSummaryResponse, 0, len(projects))
	for _, item := range projects {
		cfg := appconfig.ProjectConfig{}
		if item.Config != nil {
			cfg = *item.Config
		}
		summary := handler.buildProjectSummaryResponse(item.ID, cfg)
		summaries = append(summaries, summary)
	}

	sort.Slice(summaries, func(i, j int) bool {
		leftTitle := strings.ToLower(strings.TrimSpace(summaries[i].Title))
		rightTitle := strings.ToLower(strings.TrimSpace(summaries[j].Title))
		if leftTitle != rightTitle {
			return leftTitle < rightTitle
		}
		leftOrg := strings.ToLower(strings.TrimSpace(summaries[i].Organization))
		rightOrg := strings.ToLower(strings.TrimSpace(summaries[j].Organization))
		if leftOrg != rightOrg {
			return leftOrg < rightOrg
		}
		return strings.ToLower(strings.TrimSpace(summaries[i].Project)) <
			strings.ToLower(strings.TrimSpace(summaries[j].Project))
	})

	return httputil.JSON(summaries, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleProjectConfigPUT(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigPUTByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	if err := handler.DeleteService.DeleteProject(ctx.Context(), authorizationHeader, organization, project, configID); err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return handler.handleConfigDELETEByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectOrganizationDELETE(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		errResponse := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := access.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"organization": organization}, nil)
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	if err := handler.DeleteService.DeleteOrganization(ctx.Context(), authorizationHeader, organization); err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(map[string]any{"success": true}, http.StatusOK).Write(ctx)
}
