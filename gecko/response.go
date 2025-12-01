package gecko

import (
	"encoding/json"
	"net/http"

	"github.com/kataras/iris/v12"
	"github.com/uc-cdis/arborist/arborist"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// jsonResponse represents a generic JSON response structure
// @Schema
type jsonResponse struct {
	content any
	code    int
}

// ErrorResponse represents an error response structure
// @Schema
type ErrorResponse struct {
	HTTPError arborist.HTTPError `json:"error"`
	// err stores an internal representation of an error in case it needs to be
	// tracked along with the http-ish version in `HTTPError`.
	err error
	// log embeds a LogCache so we can log things to the response and write it
	// out later to the server's logger.
	log LogCache
}

// jsonResponseFrom creates a JSON response
// @Summary Create a JSON response
// @Description Constructs a JSON response with content and HTTP status code, supporting both JSON and Protobuf marshaling
// @Param content body any true "Response content (JSON or Protobuf message)"
// @Param code body int true "HTTP status code"
// @Return jsonResponse
func jsonResponseFrom(content any, code int) *jsonResponse {
	return &jsonResponse{
		content: content,
		code:    code,
	}
}

// write serializes and writes the JSON response to the Iris context
// @Summary Write JSON response
// @Description Serializes the response content (JSON or Protobuf) and writes it to the HTTP response
// @Param ctx body iris.Context true "Iris context"
// @Return error
func (response *jsonResponse) write(ctx iris.Context) error {
	ctx.ContentType("application/json")
	if response.code > 0 {
		ctx.StatusCode(response.code)
	} else {
		ctx.StatusCode(http.StatusOK)
	}

	var bytes []byte
	var err error

	if msg, ok := response.content.(proto.Message); ok {
		opts := protojson.MarshalOptions{
			EmitUnpopulated: true,
			UseProtoNames:   true,
			Indent:          "",
		}
		if wantPrettyJSON(ctx.Request()) {
			opts.Indent = "    "
		}
		bytes, err = opts.Marshal(msg)
	} else {
		if wantPrettyJSON(ctx.Request()) {
			bytes, err = json.MarshalIndent(response.content, "", "    ")
		} else {
			bytes, err = json.Marshal(response.content)
		}
	}
	if err != nil {
		return err
	}

	_, err = ctx.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func wantPrettyJSON(r *http.Request) bool {
	prettyJSON := false
	if r.Method == "GET" {
		prettyJSON = prettyJSON || r.URL.Query().Get("pretty") == "true"
		prettyJSON = prettyJSON || r.URL.Query().Get("prettyJSON") == "true"
	}
	return prettyJSON
}

// newErrorResponse creates an error response
// @Summary Create an error response
// @Description Constructs an error response with a message, HTTP status code, and optional error
// @Param message body string true "Error message"
// @Param code body int true "HTTP status code"
// @Param err body error false "Optional internal error"
// @Return ErrorResponse
func newErrorResponse(message string, code int, err *error) *ErrorResponse {
	response := &ErrorResponse{
		HTTPError: arborist.HTTPError{
			Message: message,
			Code:    code,
		},
	}
	if err != nil {
		response.err = *err
	}
	if code >= 500 {
		response.log.Error("%s", message)
	} else {
		response.log.Info("%s", message)
	}
	return response
}

// write serializes and writes the error response to the Iris context
// @Summary Write error response
// @Description Serializes the error response and writes it to the HTTP response
// @Param ctx body iris.Context true "Iris context"
// @Return error
func (errorResponse *ErrorResponse) write(ctx iris.Context) error {
	var bytes []byte
	var err error

	prettyJSON := false
	if ctx.Method() == "GET" {
		prettyJSON = prettyJSON || ctx.URLParamDefault("pretty", "false") == "true"
		prettyJSON = prettyJSON || ctx.URLParamDefault("pretty", "false") == "true"
	}

	if prettyJSON {
		bytes, err = json.MarshalIndent(errorResponse, "", "    ")
	} else {
		bytes, err = json.Marshal(errorResponse)
	}
	if err != nil {
		return err
	}
	ctx.ContentType("application/json")
	ctx.StatusCode(errorResponse.HTTPError.Code)
	_, err = ctx.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
