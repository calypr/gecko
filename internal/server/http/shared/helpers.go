package shared

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func ConfigTypeMiddleware(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}

// WriteAppError converts a domain error into the standard error envelope.
// Errors that are not *git.Error become 500 so internal details stay internal.
func WriteAppError(ctx fiber.Ctx, err error) error {
	if appErr, ok := err.(*git.Error); ok {
		statusCode := appErr.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		errorType := apierror.Type("internal_error")
		switch appErr.Kind {
		case git.ErrorKindValidation:
			errorType = apierror.TypeValidationFailed
		case git.ErrorKindForbidden:
			errorType = apierror.TypeForbidden
		case git.ErrorKindIntegration:
			errorType = apierror.Type("integration_error")
		case git.ErrorKindNotFound:
			errorType = apierror.TypeNotFound
		case git.ErrorKindDatabase:
			errorType = apierror.TypeDatabaseError
		case git.ErrorKindUnauthorized:
			errorType = apierror.TypeMissingAuthorization
		}
		return httputil.NewError(errorType, appErr.Error(), statusCode, appErr.Details, &err).Write(ctx)
	}
	// Non-domain errors keep their detail in the oplog only; the client gets a
	// stable message so internal paths and upstream text are never echoed back.
	return httputil.NewError(apierror.Type("internal_error"), "internal server error", http.StatusInternalServerError, nil, &err).Write(ctx)
}
