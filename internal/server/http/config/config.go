package config

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	gitapp "github.com/calypr/gecko/internal/git/app"
	gitappsetup "github.com/calypr/gecko/internal/git/app/setup"
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

func (handler *Handler) buildProjectSummaryResponse(projectID string, cfg config.ProjectConfig) (ProjectSummaryResponse, bool) {
	projectOrganization, projectName, found := strings.Cut(projectID, "/")
	if !found {
		return ProjectSummaryResponse{}, false
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

	if handler.gitService != nil {
		if _, _, err := handler.gitService.ProjectThumbnailPath(organization, project); err == nil {
			summary.ThumbnailURL = fmt.Sprintf(
				"/gecko/git/projects/%s/%s/thumbnail",
				url.PathEscape(organization),
				url.PathEscape(project),
			)
		}
	}

	return summary, true
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

// handleConfigGET godoc
// @Summary Get a specific configuration
// @Description Retrieve configuration by config type and config ID.
// @Tags Config
// @Produce json
// @Param configType path string true "Configuration Type"
// @Param configId path string true "Configuration ID"
// @Success 200 {object} map[string]interface{} "Configuration details"
// @Failure 400 {object} ErrorResponse "Invalid config type"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configType}/{configId} [get]
func (handler *Handler) handleConfigGET(ctx fiber.Ctx) error {
	configType, configID := handler.resolveConfigParams(ctx)
	return handler.handleConfigGETByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectConfigGET(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigGETByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectSummaryGET(ctx fiber.Ctx) error {
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(config.TypeProjects))
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": string(config.TypeProjects)}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	summaries := make([]ProjectSummaryResponse, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		var cfg config.ProjectConfig
		if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(config.TypeProjects), &cfg); err != nil {
			continue
		}
		summary, ok := handler.buildProjectSummaryResponse(projectID, cfg)
		if !ok {
			continue
		}
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
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if errResponse = httputil.ParseJSONBody(ctx.Body(), cfg, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	switch typed := cfg.(type) {
	case *config.ProjectConfig:
		projectOrganization, _, found := strings.Cut(configID, "/")
		if !found {
			errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("invalid project config id: %s", configID), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		typed.OrgTitle = strings.TrimSpace(projectOrganization)
		if err := typed.ValidateInitialization(); err != nil {
			errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	default:
		if validatable, ok := cfg.(interface{ Validate() error }); ok {
			if err := validatable.Validate(); err != nil {
				errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
				errResponse.WriteLog(handler.logger)
				return errResponse.Write(ctx)
			}
		}
	}
	if err := geckodb.ConfigPUTGeneric(handler.db, configID, configType, cfg); err != nil {
		errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if projectCfg, ok := cfg.(*config.ProjectConfig); ok {
		if handler.gitService != nil {
			if identity, err := git.ParseRepositoryIdentity(projectCfg.SrcRepo); err == nil {
				if existingState, existingErr := geckodb.GitProjectStateByProjectID(handler.db, configID); existingErr != nil {
					errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("read existing git project state failed: %s", existingErr), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
					errResponse.WriteLog(handler.logger)
					return errResponse.Write(ctx)
				} else if existingState != nil &&
					existingState.RepoHost == identity.Host &&
					existingState.RepoOwner == identity.Owner &&
					existingState.RepoName == identity.Repo {
					return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
				}
				state := geckodb.GitProjectState{
					ProjectID:  configID,
					RepoHost:   identity.Host,
					RepoOwner:  identity.Owner,
					RepoName:   identity.Repo,
					MirrorPath: handler.gitService.MirrorPathForIdentity(identity),
					SyncState:  git.GitSyncNeverSynced,
				}
				if upsertErr := geckodb.UpsertGitProjectState(handler.db, state); upsertErr != nil {
					errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("upsert git project state failed: %s", upsertErr), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
					errResponse.WriteLog(handler.logger)
					return errResponse.Write(ctx)
				}
			}
		}
	}

	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleProjectConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if errResponse := handler.deleteProject(ctx, authorizationHeader, configType, configID, organization, project); errResponse != nil {
		return writeAppError(ctx, handler.logger, errResponse)
	}
	return handler.handleConfigDELETEByID(ctx, configType, configID)
}

func (handler *Handler) deleteProject(ctx fiber.Ctx, authorizationHeader, configType, configID, organization, project string) error {
	resourcePath := git.ProgramProjectResourcePath(organization, project)
	handler.logger.Info(
		"starting project delete workflow: config_id=%s resource_path=%s organization=%s project=%s",
		configID,
		resourcePath,
		organization,
		project,
	)
	if handler.projectSetup != nil {
		if err := handler.projectSetup.CleanupProjectStorage(ctx.Context(), authorizationHeader, organization, project); err != nil {
			handler.logger.Error(
				"project delete failed during storage cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
				configID,
				resourcePath,
				organization,
				project,
				err,
			)
			return err
		}
	}
	if err := geckodb.DeleteGitProjectArtifacts(handler.db, configID); err != nil {
		handler.logger.Error(
			"project delete failed during gecko git artifact cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
			configID,
			resourcePath,
			organization,
			project,
			err,
		)
		return gitapp.WrapError(
			gitapp.ErrorKindDatabase,
			http.StatusInternalServerError,
			"failed during project deletion step gecko_git_artifact_cleanup",
			err,
			map[string]any{
				"config_type":   configType,
				"config_id":     configID,
				"organization":  organization,
				"project":       project,
				"resource_path": resourcePath,
				"delete_step":   "gecko_git_artifact_cleanup",
			},
		)
	}
	if handler.gitService != nil {
		if err := handler.gitService.DeleteProjectThumbnail(organization, project); err != nil && !errors.Is(err, os.ErrNotExist) {
			handler.logger.Error(
				"project delete failed during thumbnail cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
				configID,
				resourcePath,
				organization,
				project,
				err,
			)
			return gitapp.WrapError(
				gitapp.ErrorKindIntegration,
				http.StatusInternalServerError,
				"failed during project deletion step thumbnail_cleanup",
				err,
				map[string]any{
					"config_type":   configType,
					"config_id":     configID,
					"organization":  organization,
					"project":       project,
					"resource_path": resourcePath,
					"delete_step":   "thumbnail_cleanup",
				},
			)
		}
	}
	if err := gitappsetup.DeleteAuthzResource(ctx.Context(), authorizationHeader, resourcePath); err != nil {
		handler.logger.Error(
			"project delete failed during arborist resource delete: config_id=%s resource_path=%s organization=%s project=%s err=%v",
			configID,
			resourcePath,
			organization,
			project,
			err,
		)
		return gitapp.WrapError(
			gitapp.ErrorKindIntegration,
			http.StatusBadGateway,
			"failed during project deletion step arborist_resource_delete",
			err,
			map[string]any{
				"config_type":    configType,
				"config_id":      configID,
				"organization":   organization,
				"project":        project,
				"resource_path":  resourcePath,
				"delete_step":    "arborist_resource_delete",
				"upstream_error": err.Error(),
			},
		)
	}
	handler.logger.Info(
		"project delete downstream cleanup succeeded: config_id=%s resource_path=%s organization=%s project=%s",
		configID,
		resourcePath,
		organization,
		project,
	)
	return nil
}

func (handler *Handler) handleProjectOrganizationDELETE(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		errResponse := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"organization": organization}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(config.TypeProjects))
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to list projects for organization delete: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	for _, projectID := range projectIDs {
		projectOrganization, projectName, found := strings.Cut(projectID, "/")
		if !found || strings.TrimSpace(projectOrganization) != organization {
			continue
		}
		projectName = strings.TrimSpace(projectName)
		if projectName == "" {
			continue
		}
		if appErr := handler.deleteProject(ctx, authorizationHeader, string(config.TypeProjects), projectID, organization, projectName); appErr != nil {
			return writeAppError(ctx, handler.logger, appErr)
		}
		if _, err := geckodb.ConfigDELETEGeneric(handler.db, projectID, string(config.TypeProjects)); err != nil {
			errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to delete project config %s during organization delete: %s", projectID, err), http.StatusInternalServerError, map[string]any{"organization": organization, "project_id": projectID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	resourcePath := fmt.Sprintf("/programs/%s", organization)
	if err := gitappsetup.DeleteAuthzResource(ctx.Context(), authorizationHeader, resourcePath); err != nil {
		errResponse := httputil.NewError("integration_error", fmt.Sprintf("failed to delete arborist organization resource: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "resource_path": resourcePath}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleConfigGETByID(ctx fiber.Ctx, configType string, configID string) error {
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	err := geckodb.ConfigGETGeneric(handler.db, configID, configType, cfg)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			errResponse = httputil.NewError(apierror.TypeConfigNotFound, fmt.Sprintf("no config found with configId: %s of type: %s", configID, configType), http.StatusNotFound, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("config query failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	return httputil.JSON(cfg, http.StatusOK).Write(ctx)
}

// handleConfigDELETE godoc
// @Summary Delete a configuration
// @Description Delete configuration by config type and config ID.
// @Tags Config
// @Produce json
// @Param configType path string true "Configuration Type"
// @Param configId path string true "Configuration ID"
// @Success 200 {object} map[string]interface{} "Configuration deleted"
// @Failure 400 {object} ErrorResponse "Invalid config type"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configType}/{configId} [delete]
func (handler *Handler) handleConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := handler.resolveConfigParams(ctx)
	return handler.handleConfigDELETEByID(ctx, configType, configID)
}

func (handler *Handler) handleConfigDELETEByID(ctx fiber.Ctx, configType string, configID string) error {
	deleted, err := geckodb.ConfigDELETEGeneric(handler.db, configID, configType)
	if !deleted && err == nil {
		errResponse := httputil.NewError(apierror.TypeConfigNotFound, fmt.Sprintf("no configId found with configId: %s in type: %s", configID, configType), http.StatusNotFound, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("config query failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("DELETED: %s from type: %s", configID, configType)}, http.StatusOK).Write(ctx)
}

// handleConfigPUT godoc
// @Summary Update configuration
// @Description Replaces or updates the configuration for a given config ID in a specific type.
// @Tags Config
// @Accept json
// @Produce json
// @Param configType path string true "Configuration Type"
// @Param configId path string true "Configuration ID"
// @Param body body map[string]interface{} true "Configuration payload"
// @Success 200 {object} map[string]interface{} "Configuration successfully updated"
// @Failure 400 {object} ErrorResponse "Invalid request body"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /config/{configType}/{configId} [put]
func (handler *Handler) handleConfigPUT(ctx fiber.Ctx) error {
	configType, configID := handler.resolveConfigParams(ctx)
	return handler.handleConfigPUTByID(ctx, configType, configID)
}

func (handler *Handler) handleConfigPUTByID(ctx fiber.Ctx, configType string, configID string) error {
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if errResponse = httputil.ParseJSONBody(ctx.Body(), cfg, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if validatable, ok := cfg.(interface{ Validate() error }); ok {
		if err := validatable.Validate(); err != nil {
			errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	if err := geckodb.ConfigPUTGeneric(handler.db, configID, configType, cfg); err != nil {
		errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
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
