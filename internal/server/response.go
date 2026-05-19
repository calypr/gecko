package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"

	"github.com/calypr/gecko/apierror"
	"github.com/gofiber/fiber/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type jsonResponse struct {
	content any
	code    int
}

type ErrorResponse struct {
	Error apierror.Error `json:"error"`
	err   error
	log   LogCache
}

func jsonResponseFrom(content any, code int) *jsonResponse {
	return &jsonResponse{content: content, code: code}
}

func (response *jsonResponse) write(ctx fiber.Ctx) error {
	ctx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	ctx.Status(response.code)

	var body []byte
	var err error
	if msg, ok := response.content.(proto.Message); ok {
		opts := protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}
		if wantPrettyJSON(ctx) {
			opts.Indent = "    "
		}
		body, err = opts.Marshal(msg)
	} else {
		if wantPrettyJSON(ctx) {
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

func wantPrettyJSON(ctx fiber.Ctx) bool {
	if ctx.Method() != fiber.MethodGet {
		return false
	}
	return ctx.Query("pretty") == "true" || ctx.Query("prettyJSON") == "true"
}

func newTypedErrorResponse(errorType apierror.Type, message string, code int, details map[string]any, err *error) *ErrorResponse {
	response := &ErrorResponse{
		Error: apierror.Error{
			Type:    errorType,
			Message: message,
			Code:    code,
			Details: details,
		},
	}
	if err != nil {
		response.err = *err
	}
	if code >= http.StatusInternalServerError {
		response.log.Error("%s", message)
	} else {
		response.log.Info("%s", message)
	}
	return response
}

func (errorResponse *ErrorResponse) write(ctx fiber.Ctx) error {
	ctx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	ctx.Status(errorResponse.Error.Code)

	var body []byte
	var err error
	if wantPrettyJSON(ctx) {
		body, err = json.MarshalIndent(errorResponse, "", "    ")
	} else {
		body, err = json.Marshal(errorResponse)
	}
	if err != nil {
		return err
	}
	return ctx.Send(body)
}

func handleNotFound(ctx fiber.Ctx) error {
	return newTypedErrorResponse(apierror.TypeNotFound, "not found", http.StatusNotFound, nil, nil).write(ctx)
}

func parseJSONBody(body []byte, x any, details map[string]any) *ErrorResponse {
	if !json.Valid(body) {
		return newTypedErrorResponse(apierror.TypeInvalidJSON, "Invalid JSON format", http.StatusBadRequest, details, nil)
	}
	if errResponse := unmarshal(body, x); errResponse != nil {
		errResponse.Error.Details = mergeErrorDetails(errResponse.Error.Details, details)
		return errResponse
	}
	return nil
}

func unmarshal(body []byte, x any) *ErrorResponse {
	if len(body) == 0 {
		return newTypedErrorResponse(apierror.TypeEmptyRequestBody, "empty request body", http.StatusBadRequest, nil, nil)
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
		response := newTypedErrorResponse(
			apierror.TypeInvalidRequestBody,
			msg,
			http.StatusBadRequest,
			map[string]any{"target_type": structType.String()},
			&err,
		)
		response.log.Info("tried to create %s but input was invalid; offending JSON: %s", structType, loggableJSON(body))
		return response
	}

	return nil
}

func loggableJSON(body []byte) []byte {
	return regWhitespace.ReplaceAll(body, []byte(""))
}

var regWhitespace = regexp.MustCompile(`\s`)
