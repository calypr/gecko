package authz

import (
	"fmt"
	"net/http"
	"slices"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func ProjectsFromToken(ctx fiber.Ctx, jwtHandler middleware.JWTHandler, method string, service string) ([]any, *httputil.ErrorResponse) {
	token := ctx.Get("Authorization")
	if token == "" {
		return nil, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
	}
	anyList, err := jwtHandler.GetAllowedResources(token, method, service)
	if err != nil {
		serverErr, ok := err.(*middleware.ServerError)
		if !ok {
			return nil, httputil.NewError(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil)
		}
		return nil, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
	}
	return anyList, nil
}

func ParseAccess(resourceList []string, resource string, method string) *httputil.ErrorResponse {
	if len(resourceList) == 0 {
		return httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on any resource path", method), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
	}
	if slices.Contains(resourceList, resource) {
		return nil
	}
	return httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on resource path: %s", method, resource), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
}

func ServiceErrorType(code int) apierror.Type {
	return serviceErrorType(code)
}

func serviceErrorType(code int) apierror.Type {
	switch code {
	case http.StatusUnauthorized:
		return apierror.TypeUnauthorized
	case http.StatusForbidden:
		return apierror.TypeForbidden
	case http.StatusNotFound:
		return apierror.TypeNotFound
	case http.StatusMethodNotAllowed:
		return apierror.TypeMethodNotAllowed
	default:
		return apierror.TypeAuthorizationServiceError
	}
}
