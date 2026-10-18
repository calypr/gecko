package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
)

const apierrorMissingAuth = apierror.TypeMissingAuthorization

func statusResponse(message string) gecko.StatusResponse {
	code := http.StatusOK
	response := gecko.StatusResponse{Code: &code, Message: &message}
	return response
}

// toMap converts any JSON-encodable value into the map shape the spec uses for
// open object responses.
func toMap(value any) (map[string]interface{}, *opError) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to encode response: %s", err), nil)
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to encode response: %s", err), nil)
	}
	return out, nil
}

func toConfig(value any) (gecko.Config, *opError) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to encode response: %s", err), nil)
	}
	out := gecko.Config{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", fmt.Sprintf("failed to encode response: %s", err), nil)
	}
	return out, nil
}

func queryInt(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func jsonStringItem(value string) gecko.ListConfigs200JSONResponseBody_Item {
	item := gecko.ListConfigs200JSONResponseBody_Item{}
	_ = item.FromListConfigs200JSONResponseBody0(gecko.ListConfigs200JSONResponseBody0(value))
	return item
}

// decodeBody decodes a spec-decoded map body into a concrete request struct,
// reusing the legacy JSON validation path.
func decodeBody(body map[string]interface{}, target any, details map[string]any) *opError {
	raw, err := json.Marshal(body)
	if err != nil {
		return newOpError(http.StatusBadRequest, "invalid_request", "invalid request body", details)
	}
	if errResponse := httputil.ParseJSONBody(raw, target, details); errResponse != nil {
		return opErrorFromResponse(errResponse)
	}
	return nil
}

func (s *Server) logError(response *httputil.ErrorResponse) {
	if s.Logger != nil {
		response.WriteLog(s.Logger)
	}
}

func opErrorFromResponse(response *httputil.ErrorResponse) *opError {
	return &opError{
		status:  response.Error.Code,
		errType: string(response.Error.Type),
		message: response.Error.Message,
		details: response.Error.Details,
	}
}
