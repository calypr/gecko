package git

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	gitapp "github.com/calypr/gecko/internal/git/app"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) authenticatedUserID(ctx fiber.Ctx) (string, *httputil.ErrorResponse) {
	return handler.AuthenticatedUserID(ctx)
}

func (handler *Handler) writeAppError(ctx fiber.Ctx, err error) error {
	return handler.WriteAppError(ctx, err)
}

func writeAppError(ctx fiber.Ctx, logger any, err error) error {
	_ = logger
	if appErr, ok := err.(*gitapp.Error); ok {
		statusCode := appErr.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		errorType := apierror.Type("internal_error")
		switch appErr.Kind {
		case gitapp.ErrorKindValidation:
			errorType = apierror.TypeValidationFailed
		case gitapp.ErrorKindForbidden:
			errorType = apierror.TypeForbidden
		case gitapp.ErrorKindIntegration:
			errorType = apierror.Type("integration_error")
		case gitapp.ErrorKindNotFound:
			errorType = apierror.TypeNotFound
		case gitapp.ErrorKindDatabase:
			errorType = apierror.TypeDatabaseError
		case gitapp.ErrorKindUnauthorized:
			errorType = apierror.TypeMissingAuthorization
		}
		return httputil.NewError(errorType, appErr.Error(), statusCode, appErr.Details, nil).Write(ctx)
	}
	return httputil.NewError(apierror.Type("internal_error"), err.Error(), http.StatusInternalServerError, nil, nil).Write(ctx)
}
