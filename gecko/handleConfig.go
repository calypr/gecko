package gecko

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/config"
	"github.com/kataras/iris/v12"
)

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

func (server *Server) handleConfigPUT(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	data := []config.ConfigItem{}
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
