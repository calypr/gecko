package httputil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"

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

	var body []byte
	var err error
	if msg, ok := response.content.(proto.Message); ok {
		opts := protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}
		if WantPrettyJSON(ctx) {
			opts.Indent = "    "
		}
		body, err = opts.Marshal(msg)
	} else {
		if WantPrettyJSON(ctx) {
			body, err = json.MarshalIndent(response.content, "", "    ")
		} else {
			body, err = json.Marshal(response.content)
		}
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
	if err != nil {
		response.Err = *err
	}
	if code >= http.StatusInternalServerError {
		response.Log.Error("%s", message)
	} else {
		response.Log.Info("%s", message)
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

func ParseJSONBody(body []byte, x any, details map[string]any) *ErrorResponse {
	if !json.Valid(body) {
		return NewError(apierror.TypeInvalidJSON, "Invalid JSON format", http.StatusBadRequest, details, nil)
	}
	if errResponse := unmarshal(body, x); errResponse != nil {
		errResponse.Error.Details = MergeErrorDetails(errResponse.Error.Details, details)
		return errResponse
	}
	return nil
}

func unmarshal(body []byte, x any) *ErrorResponse {
	if len(body) == 0 {
		return NewError(apierror.TypeEmptyRequestBody, "empty request body", http.StatusBadRequest, nil, nil)
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	err := dec.Decode(x)
	if err != nil {
		structType := reflect.TypeOf(x)
		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}

		msg := fmt.Sprintf("could not parse %s from JSON; make sure input has correct types", structType)
		errDetails := map[string]any{
			"target_type": structType.String(),
			"body":        string(loggableJSON(body)),
		}

		switch e := err.(type) {
		case *json.SyntaxError:
			msg = "Malformed JSON syntax"
			errDetails["offset"] = e.Offset
			return NewError(apierror.TypeInvalidJSON, msg, http.StatusBadRequest, errDetails, &err)
		case *json.UnmarshalTypeError:
			msg = fmt.Sprintf("Invalid type for field %q; expected %s", e.Field, e.Type)
			errDetails["field"] = e.Field
			errDetails["expected_type"] = e.Type.String()
			return NewError(apierror.TypeInvalidRequestBody, msg, http.StatusBadRequest, errDetails, &err)
		default:
			return NewError(apierror.TypeInvalidRequestBody, msg, http.StatusBadRequest, errDetails, &err)
		}
	}
	return nil
}

func MergeErrorDetails(base map[string]any, extra map[string]any) map[string]any {
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

func loggableJSON(body []byte) []byte {
	return regWhitespace.ReplaceAll(body, []byte(""))
}

var regWhitespace = regexp.MustCompile(`\s`)
