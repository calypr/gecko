package httputil

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/uc-cdis/arborist/arborist"
)

type ErrorResponse struct {
	Error apierror.Error     `json:"error"`
	Err   error              `json:"-"`
	Log   geckologging.Cache `json:"-"`
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
