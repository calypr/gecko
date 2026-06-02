package authz

import (
	"fmt"
	"net/http"
	"slices"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func ProjectsFromToken(ctx fiber.Ctx, authzHandler servermw.ResourceAccessHandler, method string, service string) ([]any, *httputil.ErrorResponse) {
	token := ctx.Get("Authorization")
	if token == "" {
		return nil, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
	}
	anyList, err := authzHandler.GetAllowedResources(token, method, service)
	if err != nil {
		if serverErr, ok := err.(*servermw.AccessError); ok {
			return nil, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
		}
		return nil, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil)
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
