package server

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/gofiber/fiber/v3"
)

func isKnownType(t string) bool {
	return config.IsKnownType(t)
}

func (server *Server) resolveConfigParams(ctx fiber.Ctx) (string, string) {
	configType, _ := ctx.Locals("configType").(string)
	if configType == "" {
		configType = ctx.Params("configType")
	}
	configID := ctx.Params("configId")

	if configType == "" {
		configType = string(config.TypeExplorer)
	}
	if configID == "" {
		if configType == string(config.TypeAppsPage) {
			configID = config.AppsPageConfigID
		} else {
			configID = config.DefaultConfigID
		}
	}

	return configType, configID
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
func (server *Server) handleConfigListGET(ctx fiber.Ctx) error {
	configType, _ := ctx.Locals("configType").(string)
	if configType == "" {
		configType = ctx.Query("type", string(config.TypeExplorer))
	}

	if !isKnownType(configType) {
		errResponse := newTypedErrorResponse(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}

	configList, err := configListByType(server.db, configType)
	if err != nil {
		errResponse := newTypedErrorResponse(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	if configList == nil {
		configList = []string{}
	}
	return jsonResponseFrom(configList, http.StatusOK).write(ctx)
}

// handleConfigTypesGET godoc
// @Summary List supported configuration types
// @Description Retrieve the set of supported config types.
// @Tags Config
// @Produce json
// @Success 200 {array} string "Supported config types"
// @Router /config/types [get]
func (server *Server) handleConfigTypesGET(ctx fiber.Ctx) error {
	return jsonResponseFrom(config.KnownTypes(), http.StatusOK).write(ctx)
}

func configForType(configType string) (config.Configurable, *ErrorResponse) {
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
		return nil, newTypedErrorResponse(apierror.TypeInvalidConfigType, fmt.Sprintf("Unknown config type: %s", configType), http.StatusBadRequest, map[string]any{"config_type": configType}, nil)
	}
}

func (server *Server) resolveProjectConfigParams(ctx fiber.Ctx) (string, string) {
	orgTitle := ctx.Params("orgTitle")
	projectTitle := ctx.Params("projectTitle")
	if orgTitle != "" && projectTitle != "" {
		return string(config.TypeProjects), orgTitle + "/" + projectTitle
	}
	return server.resolveConfigParams(ctx)
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
func (server *Server) handleConfigGET(ctx fiber.Ctx) error {
	configType, configID := server.resolveConfigParams(ctx)
	return server.handleConfigGETByID(ctx, configType, configID)
}

func (server *Server) handleProjectConfigGET(ctx fiber.Ctx) error {
	configType, configID := server.resolveProjectConfigParams(ctx)
	return server.handleConfigGETByID(ctx, configType, configID)
}

func (server *Server) handleConfigGETByID(ctx fiber.Ctx, configType string, configID string) error {
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}

	err := configGETGeneric(server.db, configID, configType, cfg)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			errResponse = newTypedErrorResponse(apierror.TypeConfigNotFound, fmt.Sprintf("no config found with configId: %s of type: %s", configID, configType), http.StatusNotFound, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		errResponse = newTypedErrorResponse(apierror.TypeDatabaseError, fmt.Sprintf("config query failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}

	return jsonResponseFrom(cfg, http.StatusOK).write(ctx)
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
func (server *Server) handleConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := server.resolveConfigParams(ctx)
	deleted, err := configDELETEGeneric(server.db, configID, configType)
	if !deleted && err == nil {
		errResponse := newTypedErrorResponse(apierror.TypeConfigNotFound, fmt.Sprintf("no configId found with configId: %s in type: %s", configID, configType), http.StatusNotFound, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	if err != nil {
		errResponse := newTypedErrorResponse(apierror.TypeDatabaseError, fmt.Sprintf("config query failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("DELETED: %s from type: %s", configID, configType)}, http.StatusOK).write(ctx)
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
func (server *Server) handleConfigPUT(ctx fiber.Ctx) error {
	configType, configID := server.resolveConfigParams(ctx)
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}

	if errResponse = parseJSONBody(ctx.Body(), cfg, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	if validatable, ok := cfg.(interface{ Validate() error }); ok {
		if err := validatable.Validate(); err != nil {
			errResponse = newTypedErrorResponse(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
	}
	if err := configPUTGeneric(server.db, configID, configType, cfg); err != nil {
		errResponse = newTypedErrorResponse(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).write(ctx)
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
