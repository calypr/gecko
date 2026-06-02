package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	gripmiddleware "github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/authz"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/uc-cdis/arborist/arborist"
)

type JWTAllowedResourceHandler interface {
	GetAllowedResources(token, method, service string) ([]any, error)
}

func RequestLogger(logger arborist.Logger) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		start := time.Now()
		err := ctx.Next()
		latency := time.Since(start)

		routePattern := "<unmatched>"
		if route := ctx.Route(); route != nil && route.Path != "" {
			routePattern = route.Path
		}

		logger.Info(
			"%s %s - Status: %d - Latency: %s - Host: %s - IP: %s - Path: %s - Query: %s - Route: %s - Params: %v - RequestID: %s",
			ctx.Method(),
			ctx.OriginalURL(),
			ctx.Response().StatusCode(),
			latency,
			ctx.Hostname(),
			ctx.IP(),
			ctx.Path(),
			string(ctx.Request().URI().QueryString()),
			routePattern,
			routeParams(ctx),
			ctx.Get("X-Request-Id"),
		)
		return err
	}
}

func routeParams(ctx fiber.Ctx) map[string]string {
	params := make(map[string]string)
	for _, name := range []string{"configType", "configId", "orgTitle", "projectTitle", "projectId", "collection", "id"} {
		if value := ctx.Params(name); value != "" {
			params[name] = value
		}
	}
	if len(params) == 0 {
		return nil
	}
	return params
}

func ResolveConfigParams(ctx fiber.Ctx) (string, string) {
	configType, _ := ctx.Locals("configType").(string)
	if configType == "" {
		configType = ctx.Params("configType")
	}
	configID := ctx.Params("configId")

	if configType == "" {
		configType = string(config.TypeExplorer)
	}
	if configID == "" {
		if configType == string(config.TypeAppsPage) {
			configID = config.AppsPageConfigID
		} else {
			configID = config.DefaultConfigID
		}
	}

	return configType, configID
}

func ConfigAuth(logger arborist.Logger, jwtHandler gripmiddleware.JWTHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		method := ctx.Method()
		configType, configID := ResolveConfigParams(ctx)

		if configType == string(config.TypeExplorer) {
			var permMethod string
			switch method {
			case fiber.MethodGet:
				permMethod = "read"
			case fiber.MethodPut, fiber.MethodDelete:
				permMethod = "create"
			default:
				return writeError(ctx, logger, httputil.NewError(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s on %s", method, ctx.Path()), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil))
			}
			ctx.Locals("projectId", configID)
			return GeneralAuth(logger, jwtHandler, permMethod, "*")(ctx)
		}

		if method == fiber.MethodGet {
			return ctx.Next()
		}
		if method == fiber.MethodPut || method == fiber.MethodDelete {
			return BaseConfigsAuth(logger, jwtHandler, "*", "*", "/programs")(ctx)
		}

		return writeError(ctx, logger, httputil.NewError(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s on %s", method, ctx.Path()), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil))
	}
}

func GeneralAuth(logger arborist.Logger, jwtHandler gripmiddleware.JWTHandler, method, service string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}

		projectID, _ := ctx.Locals("projectId").(string)
		if projectID == "" {
			projectID = ctx.Params("projectId")
		}
		projectSplit := strings.Split(projectID, "-")
		if len(projectSplit) != 2 {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeInvalidProjectID, fmt.Sprintf("Failed to parse request body: %v", fmt.Sprintf("incorrect path %s", ctx.Path())), http.StatusNotFound, map[string]any{"project_id": projectID}, nil))
		}

		anyList, err := jwtHandler.GetAllowedResources(authorizationHeader, method, service)
		if err != nil {
			serverErr, ok := err.(*gripmiddleware.ServerError)
			if !ok {
				return writeError(ctx, logger, httputil.NewError(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(authz.ServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
		}

		resourceList, convErr := convertAnyToStringSlice(anyList)
		if convErr != nil {
			return writeError(ctx, logger, convErr)
		}
		resource := "/programs/" + projectSplit[0] + "/projects/" + projectSplit[1]
		convErr = authz.ParseAccess(resourceList, resource, method)
		if convErr != nil {
			return writeError(ctx, logger, convErr)
		}
		return ctx.Next()
	}
}

func BaseConfigsAuth(logger arborist.Logger, jwtHandler gripmiddleware.JWTHandler, method, service, resourcePath string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}

		prodHandler, ok := jwtHandler.(*gripmiddleware.ProdJWTHandler)
		if !ok {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeInvalidJWTHandler, "Internal server error: Invalid JWT handler configuration for this route", http.StatusInternalServerError, nil, nil))
		}
		allowed, err := prodHandler.CheckResourceServiceAccess(authorizationHeader, method, service, resourcePath)
		if err != nil {
			serverErr, ok := err.(*gripmiddleware.ServerError)
			if !ok {
				return writeError(ctx, logger, httputil.NewError(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(authz.ServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
		}
		if !allowed {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User does not have required %s permission on resource %s", method, "/programs"), http.StatusForbidden, map[string]any{"resource": resourcePath, "method": method}, nil))
		}
		return ctx.Next()
	}
}

func AppCardAuth(logger arborist.Logger, jwtHandler gripmiddleware.JWTHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		method := ctx.Method()
		var permMethod string
		switch method {
		case fiber.MethodGet:
			permMethod = "read"
		case fiber.MethodPost, fiber.MethodDelete:
			permMethod = "create"
		default:
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s", method), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil))
		}

		projectID := ctx.Params("projectId")
		if projectID == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingProjectID, "Missing or empty projectId", http.StatusBadRequest, nil, nil))
		}
		ctx.Locals("projectId", projectID)
		return GeneralAuth(logger, jwtHandler, permMethod, "*")(ctx)
	}
}

func RequireAuthorization(logger arborist.Logger) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
		if authorizationHeader == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}
		return ctx.Next()
	}
}

func GitProjectAuth(logger arborist.Logger, jwtHandler JWTAllowedResourceHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		if jwtHandler == nil {
			return ctx.Next()
		}
		permission := "read"
		token := ctx.Get("Authorization")
		if token == "" {
			return writeError(ctx, logger, httputil.NewError("missing_authorization", "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}
		organization := strings.TrimSpace(ctx.Params("orgTitle"))
		project := strings.TrimSpace(ctx.Params("projectTitle"))
		if organization == "" || project == "" {
			return writeError(ctx, logger, httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil))
		}
		resources, conversionErr := GitAllowedResources(jwtHandler, token, permission)
		if conversionErr != nil {
			return writeError(ctx, logger, conversionErr)
		}
		if GitProjectReadable(resources, organization, project) {
			return ctx.Next()
		}
		return writeError(ctx, logger, httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to %s on project %s/%s", permission, organization, project), http.StatusForbidden, map[string]any{
			"organization":                 organization,
			"project":                      project,
			"method":                       permission,
			"resource_path":                git.ProgramProjectResourcePath(organization, project),
			"request_access":               true,
			"request_access_resource_path": git.ProgramProjectResourcePath(organization, project),
		}, nil))
	}
}

func GitOrganizationAuth(logger arborist.Logger, jwtHandler JWTAllowedResourceHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		if jwtHandler == nil {
			return ctx.Next()
		}
		token := ctx.Get("Authorization")
		if token == "" {
			return writeError(ctx, logger, httputil.NewError("missing_authorization", "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}
		organization := strings.TrimSpace(ctx.Params("orgTitle"))
		if organization == "" {
			return writeError(ctx, logger, httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil))
		}
		allowed, err := jwtHandler.GetAllowedResources(token, "read", "*")
	if err != nil {
			return writeError(ctx, logger, httputil.NewError("authorization_service_error", fmt.Sprintf("authorization lookup failed: %s", err), http.StatusForbidden, nil, nil))
		}
		resources, conversionErr := convertAnyToStringSlice(allowed)
		if conversionErr != nil {
			return writeError(ctx, logger, conversionErr)
		}
		if git.ResourceListAllowsOrganization(resources, organization) {
			return ctx.Next()
		}
		return writeError(ctx, logger, httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil))
	}
}

func convertAnyToStringSlice(anySlice []any) ([]string, *httputil.ErrorResponse) {
	var stringSlice []string
	for _, v := range anySlice {
		str, ok := v.(string)
		if !ok {
			return nil, httputil.NewError(apierror.TypeInvalidAuthorizationResponse, fmt.Sprintf("Element %v is not a string", v), http.StatusInternalServerError, nil, nil)
		}
		stringSlice = append(stringSlice, str)
	}
	return stringSlice, nil
}

func normalizeGitResourcePath(resource string) string {
	return git.NormalizeResourcePath(resource)
}

func GitAllowedResources(jwtHandler JWTAllowedResourceHandler, token string, permission string) ([]string, *httputil.ErrorResponse) {
	allowed, err := jwtHandler.GetAllowedResources(token, permission, "*")
	if err != nil {
		return nil, httputil.NewError("authorization_service_error", fmt.Sprintf("authorization lookup failed: %s", err), http.StatusForbidden, nil, nil)
	}
	return convertAnyToStringSlice(allowed)
}

func GitProjectReadable(resources []string, organization string, project string) bool {
	return git.ResourceListAllowsProject(resources, organization, project)
}

func writeError(ctx fiber.Ctx, logger arborist.Logger, response *httputil.ErrorResponse) error {
	response.WriteLog(logger)
	return response.Write(ctx)
}
