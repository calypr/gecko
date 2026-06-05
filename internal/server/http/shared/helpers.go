package shared

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	gitapp "github.com/calypr/gecko/internal/git/app"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func ConfigTypeMiddleware(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}

func (handler *Handler) WriteAppError(ctx fiber.Ctx, err error) error {
	if err == nil {
		return nil
	}
	appErr, ok := err.(*gitapp.Error)
	if !ok {
		response := httputil.NewError(apierror.Type("internal_error"), err.Error(), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
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
	statusCode := appErr.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}
	response := httputil.NewError(errorType, appErr.Error(), statusCode, appErr.Details, nil)
	response.WriteLog(handler.Logger)
	return response.Write(ctx)
}

func (handler *Handler) AuthenticatedUserID(ctx fiber.Ctx) (string, *httputil.ErrorResponse) {
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		return "", httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
	}
	if handler.JWTApp == nil {
		return "", httputil.NewError(apierror.TypeInvalidJWTHandler, "JWT validation is not configured", http.StatusUnauthorized, nil, nil)
	}
	claims, err := handler.JWTApp.Decode(git.CleanAccessToken(authorizationHeader))
	if err != nil {
		return "", httputil.NewError(apierror.TypeUnauthorized, fmt.Sprintf("failed to decode authorization token: %s", err), http.StatusUnauthorized, nil, nil)
	}
	for _, claim := range []string{"sub", "username", "email"} {
		if value, ok := (*claims)[claim].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), nil
		}
	}
	return "", httputil.NewError(apierror.TypeUnauthorized, "authorization token does not include a stable user id", http.StatusUnauthorized, nil, nil)
}
