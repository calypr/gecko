package config

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

type ProjectSummaryResponse struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	Title        string `json:"title"`
	ContactEmail string `json:"contact_email"`
	Description  string `json:"description"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
}

type ProjectListResponse struct {
	ResourcePath string                  `json:"resourcePath"`
	ConfigData   appconfig.ProjectConfig `json:"configData"`
	Organization string                  `json:"organization"`
	Project      string                  `json:"project"`
	Title        string                  `json:"title"`
	ContactEmail string                  `json:"contact_email"`
	Description  string                  `json:"description"`
	ThumbnailURL string                  `json:"thumbnail_url,omitempty"`
}

func (handler *Handler) resolveConfigParams(ctx fiber.Ctx) (string, string) {
	return servermw.ResolveConfigParams(ctx)
}

// handleConfigListGET godoc
// @Summary List project IDs by default
// @Description The public list returns project IDs from the projects table. A type query parameter can select another configuration type; typed routes use their route type.
// @Tags Config
// @Accept json
// @Produce json
// @Param type query string false "Configuration Type"
// @Success 200 {array} string "List of config IDs"
// @Failure 400 {object} ErrorResponse "Invalid config type"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/list [get]
func (handler *Handler) handleConfigListGET(ctx fiber.Ctx) error {
	configType, _ := ctx.Locals("configType").(string)
	if configType == "" {
		configType = ctx.Query("type", string(appconfig.TypeProjects))
	}

	if !appconfig.IsKnownType(configType) {
		errResponse := httputil.NewError(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}

	if configType == string(appconfig.TypeProjects) && ctx.Path() == "/config/list" {
		projects, err := handler.ConfigService.ListProjectIdentities(ctx.Context())
		if err != nil {
			return core.WriteAppError(ctx, err, handler.Logger)
		}
		ids := make([]string, 0, len(projects))
		for _, item := range projects {
			ids = append(ids, item.ID)
		}
		return httputil.JSON(ids, http.StatusOK).Write(ctx)
	}

	if configType == string(appconfig.TypeProjects) {
		allowedResources, errResponse := access.AllowedReadResources(ctx.Context(), strings.TrimSpace(ctx.Get("Authorization")))
		if errResponse != nil {
			errResponse.WriteLog(handler.Logger)
			return errResponse.Write(ctx)
		}
		projects, err := handler.ConfigService.ListProjectsForResources(ctx.Context(), allowedResources)
		if err != nil {
			return core.WriteAppError(ctx, err, handler.Logger)
		}
		responses := make([]ProjectListResponse, 0, len(projects))
		for _, item := range projects {
			cfg := appconfig.ProjectConfig{}
			if item.Config != nil {
				cfg = *item.Config
			}
			summary := handler.buildProjectSummaryResponse(item.ID, cfg)
			responses = append(responses, ProjectListResponse{
				ResourcePath: access.ProgramProjectResourcePath(item.Organization, item.Name),
				ConfigData:   cfg,
				Organization: item.Organization,
				Project:      item.Name,
				Title:        summary.Title,
				ContactEmail: summary.ContactEmail,
				Description:  summary.Description,
				ThumbnailURL: summary.ThumbnailURL,
			})
		}
		return httputil.JSON(responses, http.StatusOK).Write(ctx)
	}

	configList, err := handler.ConfigService.ListIDs(ctx.Context(), configType)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(configList, http.StatusOK).Write(ctx)
}

// handleConfigTypesGET godoc
// @Summary List supported configuration types
// @Description Retrieve the set of supported config types.
// @Tags Config
// @Produce json
// @Success 200 {array} string "Supported config types"
// @Router /config/types [get]
func (handler *Handler) handleConfigTypesGET(ctx fiber.Ctx) error {
	return httputil.JSON(appconfig.KnownTypes(), http.StatusOK).Write(ctx)
}

func (handler *Handler) resolveProjectConfigParams(ctx fiber.Ctx) (string, string) {
	orgTitle := ctx.Params("orgTitle")
	projectTitle := ctx.Params("projectTitle")
	if orgTitle != "" && projectTitle != "" {
		return string(appconfig.TypeProjects), orgTitle + "/" + projectTitle
	}
	return handler.resolveConfigParams(ctx)
}
