package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/uc-cdis/arborist/arborist"
)

type AccessError struct {
	StatusCode int
	Message    string
}

func (e *AccessError) Error() string {
	return e.Message
}

type ResourceAccessHandler interface {
	GetAllowedResources(token, method, service string) ([]any, error)
	CheckResourceServiceAccess(token, method, service, resourcePath string) (bool, error)
}

type FenceUserAccessHandler struct {
	client *http.Client
}

func NewFenceUserAccessHandler(client *http.Client) *FenceUserAccessHandler {
	if client == nil {
		client = http.DefaultClient
	}
	return &FenceUserAccessHandler{client: client}
}

func (h *FenceUserAccessHandler) CheckResourceServiceAccess(token, method, service, resourcePath string) (bool, error) {
	allowed, err := h.GetAllowedResources(token, method, service)
	if err != nil {
		return false, err
	}
	resources, convErr := convertAnyToStringSlice(allowed)
	if convErr != nil {
		return false, &AccessError{StatusCode: http.StatusInternalServerError, Message: "authorization snapshot returned a non-string resource"}
	}
	for _, resource := range resources {
		if resource == resourcePath {
			return true, nil
		}
	}
	return false, nil
}

func (h *FenceUserAccessHandler) GetAllowedResources(token, method, service string) ([]any, error) {
	endpoint, err := fenceUserEndpoint(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: err.Error()}
	}
	validAuthorizationHeader, err := git.ValidateAuthorizationHeader(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: err.Error()}
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("failed to build authorization snapshot request: %s", err)}
	}
	req.Header.Set("Authorization", validAuthorizationHeader)

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("authorization snapshot request failed: %s", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("failed to read authorization snapshot response: %s", err)}
	}
	if resp.StatusCode >= 400 {
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = fmt.Sprintf("authorization snapshot request failed with status %d", resp.StatusCode)
		}
		return nil, &AccessError{StatusCode: resp.StatusCode, Message: message}
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("invalid authorization snapshot response: %s", err)}
	}
	resourceAccess, ok := payload["authz"].(map[string]any)
	if !ok || len(resourceAccess) == 0 {
		resourceAccess, ok = payload["project_access"].(map[string]any)
		if !ok {
			return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: "authorization snapshot response did not include authz/project_access"}
		}
	}

	out := make([]any, 0, len(resourceAccess))
	for resource, raw := range resourceAccess {
		if snapshotAllows(raw, method, service) {
			out = append(out, resource)
		}
	}
	return out, nil
}

func snapshotAllows(raw any, method, service string) bool {
	entries, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, entry := range entries {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		entryMethod, _ := record["method"].(string)
		entryService, _ := record["service"].(string)
		if entryMethod != method {
			continue
		}
		if entryService == "*" || service == "*" || entryService == service {
			return true
		}
	}
	return false
}

func fenceUserEndpoint(authorizationHeader string) (string, error) {
	token := git.CleanAccessToken(authorizationHeader)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(token, claims); err != nil {
		return "", fmt.Errorf("failed to parse authorization token: %w", err)
	}
	iss, _ := claims["iss"].(string)
	iss = strings.TrimSpace(iss)
	if iss == "" {
		return "", fmt.Errorf("authorization token does not include iss")
	}
	return strings.TrimRight(iss, "/") + "/user", nil
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
			if serverErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil))
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

		allowed, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, method, service, resourcePath)
		if err != nil {
			if serverErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
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
		resourcePath := git.ProgramProjectResourcePath(organization, project)
		allowed, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, method, "*", resourcePath)
		if err != nil {
			if serverErr, ok := err.(*AccessError); ok {
				return writeError(ctx, logger, httputil.NewError(serviceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil))
			}
			return writeError(ctx, logger, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), http.StatusForbidden, nil, nil))
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

func AppCardAuth(logger arborist.Logger, authzHandler ResourceAccessHandler) fiber.Handler {
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
		return GeneralAuth(logger, authzHandler, permMethod, "*")(ctx)
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
			"resource_path":                git.ProgramProjectResourcePath(organization, project),
			"request_access":               true,
			"request_access_resource_path": git.ProgramProjectResourcePath(organization, project),
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
	return git.ResourceListAllowsProject(resources, organization, project)
}

func writeError(ctx fiber.Ctx, logger arborist.Logger, response *httputil.ErrorResponse) error {
	response.WriteLog(logger)
	return response.Write(ctx)
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
