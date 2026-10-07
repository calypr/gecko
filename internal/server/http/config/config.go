package config

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/project"
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
	ResourcePath string               `json:"resourcePath"`
	ConfigData   config.ProjectConfig `json:"configData"`
	Organization string               `json:"organization"`
	Project      string               `json:"project"`
	Title        string               `json:"title"`
	ContactEmail string               `json:"contact_email"`
	Description  string               `json:"description"`
	ThumbnailURL string               `json:"thumbnail_url,omitempty"`
}

func isKnownType(t string) bool {
	return config.IsKnownType(t)
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
		configType = ctx.Query("type", string(config.TypeProjects))
	}

	if !isKnownType(configType) {
		errResponse := httputil.NewError(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if configType == string(config.TypeProjects) && ctx.Path() == "/config/list" {
		projects, err := project.ListIdentities(ctx.Context(), handler.db)
		if err != nil {
			errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		ids := make([]string, 0, len(projects))
		for _, item := range projects {
			ids = append(ids, item.ID)
		}
		return httputil.JSON(ids, http.StatusOK).Write(ctx)
	}

	if configType == string(config.TypeProjects) {
		projects, err := project.List(ctx.Context(), handler.db)
		if err != nil {
			errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		allowedResources, errResponse := gitAllowedReadResources(strings.TrimSpace(ctx.Get("Authorization")))
		if errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		allowedIDs := make([]string, 0, len(projects))
		for _, item := range projects {
			allowedIDs = append(allowedIDs, item.ID)
		}
		allowedIDs = filterProjectIDsByAllowedResources(allowedIDs, allowedResources)
		allowed := make(map[string]bool, len(allowedIDs))
		for _, id := range allowedIDs {
			allowed[id] = true
		}
		projects = slices.DeleteFunc(projects, func(item project.Project) bool { return !allowed[item.ID] })
		responses := make([]ProjectListResponse, 0, len(projects))
		for _, item := range projects {
			cfg := config.ProjectConfig{}
			if item.Config != nil {
				cfg = *item.Config
			}
			summary, _ := handler.buildProjectSummaryResponse(item.ID, cfg)
			responses = append(responses, ProjectListResponse{
				ResourcePath: git.ProgramProjectResourcePath(item.Organization, item.Name),
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

	configList, err := geckodb.ConfigListByType(handler.db, configType)
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if configList == nil {
		configList = []string{}
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
	return httputil.JSON(config.KnownTypes(), http.StatusOK).Write(ctx)
}

func configForType(configType string) (config.Configurable, *httputil.ErrorResponse) {
	switch configType {
	case string(config.TypeExplorer):
		return &config.Config{}, nil
	case string(config.TypeNav):
		return &config.NavPageLayoutProps{}, nil
	case string(config.TypeFileSummary):
		return &config.FilesummaryConfig{}, nil
	case string(config.TypeProject), string(config.TypeProjects):
		return &config.ProjectConfig{}, nil
	default:
		return nil, httputil.NewError(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
	}
}

func (handler *Handler) resolveProjectConfigParams(ctx fiber.Ctx) (string, string) {
	orgTitle := ctx.Params("orgTitle")
	projectTitle := ctx.Params("projectTitle")
	if orgTitle != "" && projectTitle != "" {
		return string(config.TypeProjects), orgTitle + "/" + projectTitle
	}
	return handler.resolveConfigParams(ctx)
}
