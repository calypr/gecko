package git

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
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
		return httputil.NewError(errorType, appErr.Error(), statusCode, appErr.Details, nil).Write(ctx)
	}
	return httputil.NewError(apierror.Type("internal_error"), err.Error(), http.StatusInternalServerError, nil, nil).Write(ctx)
}
