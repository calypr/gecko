package config

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
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
// @Summary List configuration IDs
// @Description Retrieve a list of configuration IDs for a specific type. When mounted under a typed route, the route type is used; otherwise the `type` query parameter is used.
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
		configType = ctx.Query("type", string(config.TypeExplorer))
	}

	if !isKnownType(configType) {
		errResponse := httputil.NewError(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
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
	if configType == string(config.TypeProjects) {
		allowedResources, errResponse := gitAllowedReadResources(strings.TrimSpace(ctx.Get("Authorization")))
		if errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		configList = filterProjectIDsByAllowedResources(configList, allowedResources)

		projects := make([]ProjectListResponse, 0, len(configList))
		for _, projectID := range configList {
			var cfg config.ProjectConfig
			if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(config.TypeProjects), &cfg); err != nil {
				continue
			}

			summary, ok := handler.buildProjectSummaryResponse(projectID, cfg)
			if !ok {
				continue
			}

			projects = append(projects, ProjectListResponse{
				ResourcePath: git.ProgramProjectResourcePath(summary.Organization, summary.Project),
				ConfigData:   cfg,
				Organization: summary.Organization,
				Project:      summary.Project,
				Title:        summary.Title,
				ContactEmail: summary.ContactEmail,
				Description:  summary.Description,
				ThumbnailURL: summary.ThumbnailURL,
			})
		}

		return httputil.JSON(projects, http.StatusOK).Write(ctx)
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

func mergeErrorDetails(base map[string]any, extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return base
	}
	if base == nil {
		base = map[string]any{}
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}
