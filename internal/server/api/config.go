package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

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
	case string(config.TypeAppsPage):
		return &config.AppsConfig{}, nil
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

func (handler *Handler) handleProjectConfigPUT(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigPUTByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigDELETEByID(ctx, configType, configID)
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
