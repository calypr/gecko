package core

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
)

// ErrorResponse converts an application error into the standard error envelope.
// Errors that are not *domain.Error become 500 so internal details stay internal.
func ErrorResponse(err error) *httputil.ErrorResponse {
	var response *httputil.ErrorResponse
	if appErr, ok := err.(*domain.Error); ok {
		statusCode := appErr.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		errorType := appErr.Type
		if errorType == "" {
			errorType = apierror.Type("internal_error")
			switch appErr.Kind {
			case domain.ErrorKindValidation:
				errorType = apierror.TypeValidationFailed
			case domain.ErrorKindForbidden:
				errorType = apierror.TypeForbidden
			case domain.ErrorKindIntegration:
				errorType = apierror.Type("integration_error")
			case domain.ErrorKindNotFound:
				errorType = apierror.TypeNotFound
			case domain.ErrorKindDatabase:
				errorType = apierror.TypeDatabaseError
			case domain.ErrorKindUnauthorized:
				errorType = apierror.TypeMissingAuthorization
			}
		}
		response = httputil.NewError(errorType, appErr.Error(), statusCode, appErr.Details, &err)
	} else {
		// Non-domain errors keep their detail in the oplog only; the client gets a
		// stable message so internal paths and upstream text are never echoed back.
		response = httputil.NewError(apierror.Type("internal_error"), "internal server error", http.StatusInternalServerError, nil, &err)
	}
	return response
}
