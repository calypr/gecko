package gecko

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/config"
	"github.com/kataras/iris/v12"
)

// handleConfigListGET godoc
// @Summary List all configurations
// @Description Retrieve a list of all available configurations
// @Tags Config
// @Accept json
// @Produce json
// @Success 200 {array} config.Config
// @Failure 404 {object} ErrorResponse "No configs found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/list [get]
func (server *Server) handleConfigListGET(ctx iris.Context) {
	configList, err := configList(server.db)
	if configList == nil && err == nil {
		errResponse := newErrorResponse("No configs found", 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		errResponse := newErrorResponse(fmt.Sprintf("%s", err), 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(configList, http.StatusOK).write(ctx)
}

// handleConfigGET godoc
// @Summary Get a specific configuration
// @Description Retrieve configuration by ID
// @Tags Config
// @Produce json
// @Param configId path string true "Configuration ID"
// @Success 200 {object} config.Config "Configuration details"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configId} [get]
func (server *Server) handleConfigGET(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	doc, err := configGET(server.db, configId)
	if doc == nil && err == nil {
		msg := fmt.Sprintf("no configId found with configId: %s", configId)
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
	jsonResponseFrom(doc, http.StatusOK).write(ctx)
}

// handleConfigDELETE godoc
// @Summary Delete a configuration
// @Description Delete configuration by ID
// @Tags Config
// @Produce json
// @Param configId path string true "Configuration ID" example:"config_123"
// @Success 200 {object} map[string]interface{} "Configuration deleted"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/{configId} [delete]
func (server *Server) handleConfigDELETE(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	doc, err := configDELETE(server.db, configId)
	if doc == false && err == nil {
		msg := fmt.Sprintf("no configId found with configId: %s", configId)
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

	okmsg := map[string]any{"code": 200, "message": fmt.Sprintf("DELETED: %s", configId)}
	jsonResponseFrom(okmsg, http.StatusOK).write(ctx)
}

// handleConfigPUT updates a configuration by ID.
// @Summary Update configuration
// @Description Replaces or updates the configuration items for a given config ID
// @Tags Config
// @Accept  json
// @Produce  json
// @Param configId path string true "Configuration ID"
// @Param body body config.Config true "Configuration items to set"
// @Success 200 {object} jsonResponse "Configuration successfully updated"
// @Failure 400 {object} ErrorResponse "Invalid request body"
// @Failure 404 {object} ErrorResponse "Config not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /config/{configId} [put]
func (server *Server) handleConfigPUT(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	data := config.Config{}
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
	errResponse := unmarshal(body, &data)
	if errResponse != nil {
		msg := fmt.Sprintf("body data unmarshal failed: %s", errResponse.err)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	err = configPUT(server.db, configId, data)
	if err != nil {
		msg := fmt.Sprintf("configPut failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	okmsg := map[string]any{"code": 200, "message": fmt.Sprintf("ACCEPTED: %s", configId)}
	jsonResponseFrom(okmsg, http.StatusOK).write(ctx)
}
