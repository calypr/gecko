package httputil

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/gofiber/fiber/v3"
	"github.com/uc-cdis/arborist/arborist"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type JSONResponse struct {
	content any
	code    int
}

type ErrorResponse struct {
	Error apierror.Error     `json:"error"`
	Err   error              `json:"-"`
	Log   geckologging.Cache `json:"-"`
}

func JSON(content any, code int) *JSONResponse {
	return &JSONResponse{content: content, code: code}
}

func (response *JSONResponse) Write(ctx fiber.Ctx) error {
	ctx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	ctx.Status(response.code)

	pretty := WantPrettyJSON(ctx)

	var body []byte
	var err error
	if msg, ok := response.content.(proto.Message); ok {
		opts := protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}
		if pretty {
			opts.Indent = "    "
		}
		body, err = opts.Marshal(msg)
	} else if pretty {
		body, err = json.MarshalIndent(response.content, "", "    ")
	} else {
		body, err = json.Marshal(response.content)
	}
	if err != nil {
		return err
	}
	return ctx.Send(body)
}

func WantPrettyJSON(ctx fiber.Ctx) bool {
	if ctx.Method() != fiber.MethodGet {
		return false
	}
	return ctx.Query("pretty") == "true" || ctx.Query("prettyJSON") == "true"
}

func NewError(errorType apierror.Type, message string, code int, details map[string]any, err *error) *ErrorResponse {
	response := &ErrorResponse{
		Error: apierror.Error{
			Type:    errorType,
			Message: message,
			Code:    code,
			Details: details,
		},
	}
	logged := message
	if err != nil {
		response.Err = *err
		logged = fmt.Sprintf("%s: %v", message, *err)
	}
	if code >= http.StatusInternalServerError {
		response.Log.Error("%s", logged)
	} else {
		response.Log.Info("%s", logged)
	}
	return response
}

func (response *ErrorResponse) WriteLog(logger arborist.Logger) {
	response.Log.Write(logger)
}

func (response *ErrorResponse) Write(ctx fiber.Ctx) error {
	ctx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	ctx.Status(response.Error.Code)

	var body []byte
	var err error
	if WantPrettyJSON(ctx) {
		body, err = json.MarshalIndent(response, "", "    ")
	} else {
		body, err = json.Marshal(response)
	}
	if err != nil {
		return err
	}
	return ctx.Send(body)
}

func NotFound(ctx fiber.Ctx) error {
	return NewError(apierror.TypeNotFound, "not found", http.StatusNotFound, nil, nil).Write(ctx)
}
