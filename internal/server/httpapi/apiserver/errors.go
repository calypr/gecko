package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/server/httpapi/core"
)

// opError is the uniform error returned by strict handlers. The response error
// handler translates it into the spec's ErrorResponse envelope with the
// original status code, so handlers do not need per-operation error variants.
type opError struct {
	status  int
	errType string
	message string
	details map[string]any
}

func (e *opError) Error() string { return e.message }

func newOpError(status int, errType, message string, details map[string]any) *opError {
	return &opError{status: status, errType: errType, message: message, details: details}
}

// fromAppError maps a service error exactly the way the legacy fiber handlers
// did through core.ErrorResponse.
func fromAppError(err error) *opError {
	response := core.ErrorResponse(err)
	return &opError{
		status:  response.Error.Code,
		errType: string(response.Error.Type),
		message: response.Error.Message,
		details: response.Error.Details,
	}
}

// requestErrorHandler covers decode failures produced by the generated router
// (malformed JSON body, bad form data) before a handler runs.
func (s *Server) requestErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	s.writeError(w, newOpError(http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid request: %s", err), nil))
}

// responseErrorHandler covers errors returned by handlers and write failures.
func (s *Server) responseErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	var oe *opError
	if !errors.As(err, &oe) {
		oe = newOpError(http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
	s.writeError(w, oe)
}

func (s *Server) writeError(w http.ResponseWriter, e *opError) {
	if s.Logger != nil {
		s.Logger.Error("openapi_request_error status=%d type=%s message=%s", e.status, e.errType, e.message)
	}
	body := gecko.ErrorResponse{}
	body.Error.Type = &e.errType
	body.Error.Message = &e.message
	body.Error.Code = &e.status
	if e.details != nil {
		body.Error.Details = &e.details
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(body)
}
