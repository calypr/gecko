package gecko

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/config"
	"github.com/kataras/iris/v12"
)

// handleConfigListGET godoc
// @Summary List all configuration IDs for a specific type
// @Description Retrieve a list of all available configuration IDs for the given type (table).
// @Tags Config
// @Accept json
// @Produce json
// @Param configType path string true "Configuration Type (table name)"
// @Success 200 {array} string "List of config IDs"
// @Failure 404 {object} ErrorResponse "No configs found for this type"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configType}/list [get]
func (server *Server) handleConfigListGET(ctx iris.Context) {
	configType := ctx.Params().Get("configType")

	configList, err := configListByType(server.db, configType)
	if configList == nil && err == nil {
		errResponse := newErrorResponse(fmt.Sprintf("No configs found for type: %s", configType), 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		errResponse := newErrorResponse(fmt.Sprintf("Database error: %s", err), 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(configList, http.StatusOK).write(ctx)
}

// handleConfigGET godoc
// @Summary Get a specific configuration
// @Description Retrieve configuration by configType and configId
// @Tags Config
// @Produce json
// @Param configType path string true "Configuration Type (table name)"
// @Param configId path string true "Configuration ID"
// @Success 200 {object} config.Config "Configuration details"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configType}/{configId} [get]
func (server *Server) handleConfigGET(ctx iris.Context) {
	configType := ctx.Params().Get("configType")
	configId := ctx.Params().Get("configId")

	var cfg config.Configurable // Use the interface type

	switch configType {
	case "explorer":
		cfg = &config.Config{}
	case "nav":
		cfg = &config.NavPageLayoutProps{}
	case "file_summary":
		cfg = &config.FilesummaryConfig{}
	case "apps_page":
		cfg = &config.AppsConfig{}
	default:
		msg := fmt.Sprintf("Unknown config type: %s", configType)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	// Pass configType to the generic GET function
	err := configGETGeneric(server.db, configId, configType, cfg)
	// returning 404 on an empty config might be a bit controversial,
	// but I think it will stock alot of edge cases
	if cfg.IsZero() && err == nil || errors.Is(err, sql.ErrNoRows) {
		msg := fmt.Sprintf("no config found with configId: %s of type: %s", configId, configType)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	if err != nil {
		msg := fmt.Sprintf("config query failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	// Send back the populated config struct
	jsonResponseFrom(cfg, http.StatusOK).write(ctx)
}

// handleConfigDELETE godoc
// @Summary Delete a configuration
// @Description Delete configuration by configType and configId
// @Tags Config
// @Produce json
// @Param configType path string true "Configuration Type (table name)"
// @Param configId path string true "Configuration ID" example:"config_123"
// @Success 200 {object} map[string]interface{} "Configuration deleted"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configType}/{configId} [delete]
func (server *Server) handleConfigDELETE(ctx iris.Context) {
	configType := ctx.Params().Get("configType")
	configId := ctx.Params().Get("configId")

	// Pass configType to the generic DELETE function
	deleted, err := configDELETEGeneric(server.db, configId, configType)
	if deleted == false && err == nil {
		msg := fmt.Sprintf("no configId found with configId: %s in type: %s", configId, configType)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		msg := fmt.Sprintf("config query failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponseFrom(
		map[string]any{
			"code":    200,
			"message": fmt.Sprintf("DELETED: %s from type: %s", configId, configType),
		},
		http.StatusOK,
	).write(ctx)
}

// handleConfigPUT updates a configuration by ID.
// @Summary Update configuration
// @Description Replaces or updates the configuration items for a given config ID in a specific type (table)
// @Tags Config
// @Accept json
// @Produce json
// @Param configType path string true "Configuration Type (table name)"
// @Param configId path string true "Configuration ID"
// @Param body body config.Config true "Configuration items to set"
// @Success 200 {object} jsonResponse "Configuration successfully updated"
// @Failure 400 {object} ErrorResponse "Invalid request body"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /config/{configType}/{configId} [put]
func (server *Server) handleConfigPUT(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	configType := ctx.Params().Get("configType")

	var cfg config.Configurable // Use the interface type

	switch configType {
	case "explorer":
		cfg = &config.Config{}
	case "nav":
		cfg = &config.NavPageLayoutProps{}
	case "file_summary":
		cfg = &config.FilesummaryConfig{}
	case "apps_page":
		cfg = &config.AppsConfig{}
	default:
		msg := fmt.Sprintf("Unknown config type: %s", configType)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	body, err := ctx.GetBody()
	if err != nil {
		msg := fmt.Sprintf("GetBody() failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if !json.Valid(body) {
		msg := "Invalid JSON format"
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	// Note: We unmarshal into the specific struct pointer held by cfg
	errResponse := unmarshal(body, cfg)
	if errResponse != nil {
		msg := fmt.Sprintf("body data unmarshal failed: %s", errResponse.err)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	// Pass configType to the generic PUT function
	err = configPUTGeneric(server.db, configId, configType, cfg)
	if err != nil {
		msg := fmt.Sprintf("configPut failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	okmsg := map[string]any{"code": 200, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configId, configType)}
	jsonResponseFrom(okmsg, http.StatusOK).write(ctx)
}
