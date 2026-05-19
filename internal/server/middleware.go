package server

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/gofiber/fiber/v3"
)

func (server *Server) logRequestMiddleware(ctx fiber.Ctx) error {
	start := time.Now()
	err := ctx.Next()
	latency := time.Since(start)

	routePattern := "<unmatched>"
	if route := ctx.Route(); route != nil {
		if route.Path != "" {
			routePattern = route.Path
		}
	}

	server.Logger.Info(
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
		ctx.AllParams(),
		ctx.Get("X-Request-Id"),
	)
	return err
}

func (server *Server) GetProjectsFromToken(ctx fiber.Ctx, jwtHandler middleware.JWTHandler, method string, service string) ([]any, *ErrorResponse) {
	token := ctx.Get("Authorization")
	if token == "" {
		return nil, newTypedErrorResponse(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
	}
	anyList, err := jwtHandler.GetAllowedResources(token, method, service)
	if err != nil {
		serverErr, ok := err.(*middleware.ServerError)
		if !ok {
			return nil, newTypedErrorResponse(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil)
		}
		return nil, newTypedErrorResponse(authorizationServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
	}
	return anyList, nil
}

func ParseAccess(resourceList []string, resource string, method string) *ErrorResponse {
	if len(resourceList) == 0 {
		return newTypedErrorResponse(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on any resource path", method), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
	}
	if slices.Contains(resourceList, resource) {
		return nil
	}
	return newTypedErrorResponse(apierror.TypeForbidden, fmt.Sprintf("User is not allowed to %s on resource path: %s", method, resource), http.StatusForbidden, map[string]any{"resource": resource, "method": method}, nil)
}

func convertAnyToStringSlice(anySlice []any) ([]string, *ErrorResponse) {
	var stringSlice []string
	for _, v := range anySlice {
		str, ok := v.(string)
		if !ok {
			return nil, newTypedErrorResponse(apierror.TypeInvalidAuthorizationResponse, fmt.Sprintf("Element %v is not a string", v), http.StatusInternalServerError, nil, nil)
		}
		stringSlice = append(stringSlice, str)
	}
	return stringSlice, nil
}

func authorizationServiceErrorType(code int) apierror.Type {
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

func (server *Server) ConfigAuthMiddleware(jwtHandler middleware.JWTHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		method := ctx.Method()
		configType, configID := server.resolveConfigParams(ctx)

		if configType == string(config.TypeExplorer) {
			var permMethod string
			switch method {
			case fiber.MethodGet:
				permMethod = "read"
			case fiber.MethodPut, fiber.MethodDelete:
				permMethod = "create"
			default:
				errResp := newTypedErrorResponse(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s on %s", method, ctx.Path()), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil)
				errResp.log.write(server.Logger)
				return errResp.write(ctx)
			}
			ctx.Locals("projectId", configID)
			return server.GeneralAuthMware(jwtHandler, permMethod, "*")(ctx)
		}

		if method == fiber.MethodGet {
			return ctx.Next()
		}
		if method == fiber.MethodPut || method == fiber.MethodDelete {
			return server.BaseConfigsAuthMiddleware(jwtHandler, "*", "*", "/programs")(ctx)
		}

		errResp := newTypedErrorResponse(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s on %s", method, ctx.Path()), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil)
		errResp.log.write(server.Logger)
		return errResp.write(ctx)
	}
}

func (server *Server) GeneralAuthMware(jwtHandler middleware.JWTHandler, method, service string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			errResponse := newTypedErrorResponse(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}

		projectID, _ := ctx.Locals("projectId").(string)
		if projectID == "" {
			projectID = ctx.Params("projectId")
		}
		projectSplit := strings.Split(projectID, "-")
		if len(projectSplit) != 2 {
			errResponse := newTypedErrorResponse(apierror.TypeInvalidProjectID, fmt.Sprintf("Failed to parse request body: %v", fmt.Sprintf("incorrect path %s", ctx.Path())), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}

		anyList, err := jwtHandler.GetAllowedResources(authorizationHeader, method, service)
		if err != nil {
			serverErr, ok := err.(*middleware.ServerError)
			if !ok {
				errResponse := newTypedErrorResponse(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil)
				errResponse.log.write(server.Logger)
				return errResponse.write(ctx)
			}
			errResponse := newTypedErrorResponse(authorizationServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}

		resourceList, convErr := convertAnyToStringSlice(anyList)
		if convErr != nil {
			convErr.log.write(server.Logger)
			return convErr.write(ctx)
		}
		resource := "/programs/" + projectSplit[0] + "/projects/" + projectSplit[1]
		convErr = ParseAccess(resourceList, resource, method)
		if convErr != nil {
			convErr.log.write(server.Logger)
			return convErr.write(ctx)
		}
		return ctx.Next()
	}
}

func (server *Server) BaseConfigsAuthMiddleware(jwtHandler middleware.JWTHandler, method, service, resourcePath string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		authorizationHeader := ctx.Get("Authorization")
		if authorizationHeader == "" {
			errResponse := newTypedErrorResponse(apierror.TypeMissingAuthorization, "Authorization token not provided", http.StatusUnauthorized, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}

		prodHandler, ok := jwtHandler.(*middleware.ProdJWTHandler)
		if !ok {
			errResponse := newTypedErrorResponse(apierror.TypeInvalidJWTHandler, "Internal server error: Invalid JWT handler configuration for this route", http.StatusInternalServerError, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		allowed, err := prodHandler.CheckResourceServiceAccess(authorizationHeader, method, service, resourcePath)
		if err != nil {
			serverErr, ok := err.(*middleware.ServerError)
			if !ok {
				errResponse := newTypedErrorResponse(apierror.TypeInvalidAuthorizationResponse, "expecting error to be serverError type", http.StatusNotFound, nil, nil)
				errResponse.log.write(server.Logger)
				return errResponse.write(ctx)
			}
			errResponse := newTypedErrorResponse(authorizationServiceErrorType(serverErr.StatusCode), serverErr.Message, serverErr.StatusCode, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		if !allowed {
			errResponse := newTypedErrorResponse(apierror.TypeForbidden, fmt.Sprintf("User does not have required %s permission on resource %s", method, "/programs"), http.StatusForbidden, map[string]any{"resource": resourcePath, "method": method}, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		return ctx.Next()
	}
}

func (server *Server) AppCardAuthMiddleware(jwtHandler middleware.JWTHandler) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		method := ctx.Method()
		var permMethod string
		switch method {
		case fiber.MethodGet:
			permMethod = "read"
		case fiber.MethodPost, fiber.MethodDelete:
			permMethod = "create"
		default:
			errResp := newTypedErrorResponse(apierror.TypeMethodNotAllowed, fmt.Sprintf("Unsupported HTTP method %s", method), http.StatusMethodNotAllowed, map[string]any{"method": method}, nil)
			errResp.log.write(server.Logger)
			return errResp.write(ctx)
		}

		projectID := ctx.Params("projectId")
		if projectID == "" {
			errResponse := newTypedErrorResponse(apierror.TypeMissingProjectID, "Missing or empty projectId", http.StatusBadRequest, nil, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		ctx.Locals("projectId", projectID)
		return server.GeneralAuthMware(jwtHandler, permMethod, "*")(ctx)
	}
}
