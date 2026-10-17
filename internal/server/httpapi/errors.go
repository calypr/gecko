package httpapi

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/uc-cdis/arborist/arborist"
)

func ConfigTypeMiddleware(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}

// WriteAppError converts a domain error into the standard error envelope.
// Errors that are not *domain.Error become 500 so internal details stay internal.
// Callers may pass a logger to preserve the error-log write behavior of handlers
// that previously built and logged an ErrorResponse by hand.
func WriteAppError(ctx fiber.Ctx, err error, loggers ...arborist.Logger) error {
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
	for _, logger := range loggers {
		if logger != nil {
			response.WriteLog(logger)
		}
	}
	return response.Write(ctx)
}
