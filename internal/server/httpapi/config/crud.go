package config

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/server/httpapi/core"
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
	cfg, err := handler.ConfigService.Get(ctx.Context(), configType, configID)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
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
	if err := handler.ConfigService.Delete(ctx.Context(), configType, configID); err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
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
	cfg, err := configuration.ConfigForType(configType)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	if errResponse := httputil.ParseJSONBody(ctx.Body(), cfg, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	if err := handler.ConfigService.Put(ctx.Context(), configType, configID, cfg); err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
}
