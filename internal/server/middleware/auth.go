package middleware

import (
	"fmt"
	"net/http"
	"strings"

	ggmw "github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/uc-cdis/arborist/arborist"
)

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
		configID = config.DefaultConfigID
	}

	return configType, configID
}

func ConfigAuth(logger arborist.Logger, authzHandler ResourceAccessHandler) fiber.Handler {
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
			return GeneralAuth(logger, authzHandler, permMethod, "*")(ctx)
		}

		if method == fiber.MethodGet {
			return ctx.Next()
		}
		if method == fiber.MethodPut || method == fiber.MethodDelete {
			return writeError(ctx, logger, httputil.NewError(
				apierror.TypeForbidden,
				fmt.Sprintf("Route %s %s must use route-specific authorization; refusing global /programs fallback", method, ctx.Path()),
				http.StatusForbidden,
				map[string]any{"method": method, "path": ctx.Path(), "config_type": configType},
				nil,
			))
		}

		return writeError(ctx, logger, httputil.NewError(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s on %s", method, ctx.Path()), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil))
	}
}

func GeneralAuth(logger arborist.Logger, authzHandler ResourceAccessHandler, method, service string) fiber.Handler {
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

		anyList, err := authzHandler.GetAllowedResources(authorizationHeader, method, service)
		if err != nil {
			if serverErr, ok := err.(*ggmw.ServerError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
			}
			if accessErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(accessErr.StatusCode), accessErr.Message, accessErr.StatusCode, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(apierror.TypeNotFound, "expecting error to be serverError type", http.StatusNotFound, nil, nil))
		}

		resourceList, convErr := convertAnyToStringSlice(anyList)
		if convErr != nil {
			return writeError(ctx, logger, convErr)
		}
		resource := "/programs/" + projectSplit[0] + "/projects/" + projectSplit[1]
		if len(resourceList) == 0 {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on any resource path", method), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil))
		}
		allowed := false
		for _, candidate := range resourceList {
			if candidate == resource {
				allowed = true
				break
			}
		}
		if !allowed {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on resource path: %s", method, resource), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil))
		}
		return ctx.Next()
	}
}

func BaseConfigsAuth(logger arborist.Logger, authzHandler ResourceAccessHandler, method, service, resourcePath string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}

		if _, ok := authzHandler.(*FenceUserAccessHandler); !ok {
			return writeError(ctx, logger, httputil.NewError("internal_server_error", "Invalid JWT handler configuration", http.StatusInternalServerError, nil, nil))
		}

		allowed, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, method, service, resourcePath)
		if err != nil {
			if serverErr, ok := err.(*ggmw.ServerError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
			}
			if accessErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(accessErr.StatusCode), accessErr.Message, accessErr.StatusCode, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil))
		}
		if !allowed {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User does not have required %s permission on resource %s", method, "/programs"), http.StatusForbidden, map[string]any{"resource": resourcePath, "method": method}, nil))
		}
		return ctx.Next()
	}
}

func ProjectConfigAuth(logger arborist.Logger, authzHandler ResourceAccessHandler, method string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil))
		}
		organization := strings.TrimSpace(ctx.Params("orgTitle"))
		project := strings.TrimSpace(ctx.Params("projectTitle"))
		if organization == "" || project == "" {
			return writeError(ctx, logger, httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil))
		}
		resourcePath := ProgramProjectResourcePath(organization, project)
		allowed, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, method, "*", resourcePath)
		if err != nil {
			if serverErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil))
		}
		if !allowed {
			anyList, listErr := authzHandler.GetAllowedResources(authorizationHeader, method, "*")
			if listErr != nil {
				if serverErr, ok := listErr.(*AccessError); ok {
					return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
				}
				return writeError(ctx, logger, httputil.NewError(apierror.TypeAuthorizationServiceError, listErr.Error(), http.StatusForbidden, nil, nil))
			}
			resources, conversionErr := convertAnyToStringSlice(anyList)
			if conversionErr != nil {
				return writeError(ctx, logger, conversionErr)
			}
			allowed = resourceListAllowsProjectAdminAction(resources, organization, project)
		}
		if !allowed {
			return writeError(ctx, logger, httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User does not have required %s permission on resource %s", method, resourcePath), http.StatusForbidden, map[string]any{
				"resource":     resourcePath,
				"method":       method,
				"organization": organization,
				"project":      project,
			}, nil))
		}
		return ctx.Next()
	}
}

func resourceListAllowsProjectAdminAction(resources []string, organization string, project string) bool {
	projectResource := ProgramProjectResourcePath(organization, project)
	projectCollectionResource := fmt.Sprintf("/programs/%s/projects", organization)
	organizationResource := fmt.Sprintf("/programs/%s", organization)

	for _, resource := range resources {
		switch resource {
		case "*", "/", "/programs", organizationResource, projectCollectionResource, projectResource:
			return true
		}
	}
	return false
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

func GitProjectAuth(logger arborist.Logger, jwtHandler ResourceAccessHandler) fiber.Handler {
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
			"resource_path":                ProgramProjectResourcePath(organization, project),
			"request_access":               true,
			"request_access_resource_path": ProgramProjectResourcePath(organization, project),
		}, nil))
	}
}

func GitOrganizationAuth(logger arborist.Logger, jwtHandler ResourceAccessHandler) fiber.Handler {
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
		if ResourceListAllowsOrganization(resources, organization) {
			return ctx.Next()
		}
		return writeError(ctx, logger, httputil.NewError("forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), http.StatusForbidden, map[string]any{"organization": organization, "method": "read"}, nil))
	}
}

func GitAllowedResources(jwtHandler ResourceAccessHandler, token string, permission string) ([]string, *httputil.ErrorResponse) {
	allowed, err := jwtHandler.GetAllowedResources(token, permission, "*")
	if err != nil {
		if serverErr, ok := err.(*AccessError); ok {
			return nil, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
		}
		return nil, httputil.NewError("authorization_service_error", fmt.Sprintf("authorization lookup failed: %s", err), http.StatusForbidden, nil, nil)
	}
	return convertAnyToStringSlice(allowed)
}

func GitProjectReadable(resources []string, organization string, project string) bool {
	return ResourceListAllowsProject(resources, organization, project)
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

func writeError(ctx fiber.Ctx, logger arborist.Logger, response *httputil.ErrorResponse) error {
	response.WriteLog(logger)
	return response.Write(ctx)
}

// Migrated from internal/authz
func GetProjectsFromToken(ctx fiber.Ctx, authzHandler ResourceAccessHandler, method string, service string) ([]any, *httputil.ErrorResponse) {
	token := ctx.Get("Authorization")
	if token == "" {
		return nil, httputil.NewError(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
	}
	anyList, err := authzHandler.GetAllowedResources(token, method, service)
	if err != nil {
		if serverErr, ok := err.(*AccessError); ok {
			return nil, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
		}
		return nil, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil)
	}
	return anyList, nil
}

// Migrated from internal/authz
func ParseAccess(resourceList []string, resource string, method string) *httputil.ErrorResponse {
	if len(resourceList) == 0 {
		return httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on any resource path", method), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
	}
	for _, v := range resourceList {
		if v == resource {
			return nil
		}
	}
	return httputil.NewError(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on resource path: %s", method, resource), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
}

func CleanAccessToken(raw string) string {
	token := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("bearer "):])
	}
	return token
}

func ValidateAccessToken(raw string) (string, error) {
	token := CleanAccessToken(raw)
	if token == "" {
		return "", fmt.Errorf("git access token is required")
	}
	return token, nil
}

func ValidateAuthorizationHeader(raw string) (string, error) {
	token := strings.TrimSpace(raw)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return "", fmt.Errorf("authorization header must use bearer auth")
	}
	return token, nil
}
