package config

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

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
