package gecko

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/kataras/iris/v12"
)

func (server *Server) logRequestMiddleware(ctx iris.Context) {
	start := time.Now()
	ctx.Next()
	latency := time.Since(start)
	method := ctx.Request().Method
	path := ctx.Request().URL.Path
	status := ctx.ResponseWriter().StatusCode()

	server.logger.Info("%s %s - Status: %d - Latency: %s", method, path, status, latency)
}

func (server *Server) ProjLevelAuthMware(jwtHandler middleware.JWTHandler) iris.Handler {
	return func(ctx iris.Context) {
		projectIDParam := ctx.Params().Get("project_id")
		project_split := strings.Split(projectIDParam, "-")
		if len(project_split) != 2 {
			errResponse := newErrorResponse(fmt.Sprintf("Failed to parse request body: %v", fmt.Sprintf("incorrect path %s", ctx.Request().URL)), http.StatusNotFound, nil)
			errResponse.log.write(server.logger)
			_ = errResponse.write(ctx)
			ctx.StopExecution()
			return
		}

		project_id := "/programs/" + project_split[0] + "/projects/" + project_split[1]
		authorizationHeader := ctx.GetHeader("Authorization")

		if authorizationHeader != "" {
			Token := authorizationHeader
			anyList, err := jwtHandler.HandleJWTToken(Token, "read")
			if err != nil {
				val, ok := err.(*middleware.ServerError)
				if !ok {
					errResponse := newErrorResponse(fmt.Sprintf("expecting error to be serverError type"), http.StatusNotFound, nil)
					errResponse.log.write(server.logger)
					_ = errResponse.write(ctx)
					ctx.StopExecution()
					return
				}
				errResponse := newErrorResponse(val.Message, val.StatusCode, nil)
				errResponse.log.write(server.logger)
				_ = errResponse.write(ctx)
				ctx.StopExecution()
				return
			}

			resourceList, convErr := convertAnyToStringSlice(anyList)
			if convErr != nil {
				convErr.log.write(server.logger)
				_ = convErr.write(ctx)
				ctx.StopExecution()
			}

			convErr = ParseAccess(resourceList, project_id, "read")
			if err != nil {
				convErr.log.write(server.logger)
				_ = convErr.write(ctx)
				ctx.StopExecution()
				return
			}
		} else {
			errResponse := newErrorResponse("Authorization token not provided", http.StatusBadRequest, nil)
			errResponse.log.write(server.logger)
			_ = errResponse.write(ctx)
			ctx.StopExecution()
			return
		}
		ctx.Next()
	}
}

func (server *Server) GetProjectsFromToken(ctx iris.Context, jwtHandler middleware.JWTHandler) ([]any, *ErrorResponse) {
	Token := ctx.GetHeader("Authorization")
	if Token != "" {
		anyList, err := jwtHandler.HandleJWTToken(Token, "read")
		if err != nil {
			fmt.Println("ERR: ", err)
			val, ok := err.(*middleware.ServerError)
			if !ok {
				return nil, newErrorResponse(fmt.Sprintf("expecting error to be serverError type"), http.StatusNotFound, nil)

			}
			return nil, newErrorResponse(val.Message, val.StatusCode, nil)
		}
		return anyList, nil
	}
	return nil, newErrorResponse("Auth Token not provided", 401, nil)
}

func ParseAccess(resourceList []string, resource string, method string) *ErrorResponse {
	/*  Iterates through a list of Gen3 resoures and returns true if
	    resource matches the allowable list of resource types for the provided method */

	if len(resourceList) == 0 {
		return newErrorResponse(fmt.Sprintf("User is not allowed to %s on any resource path", method), 403, nil)
	}
	if slices.Contains(resourceList, resource) {
		return nil
	}
	return newErrorResponse(fmt.Sprintf("User is not allowed to %s on resource path: %s", method, resource), 403, nil)
}

func convertAnyToStringSlice(anySlice []any) ([]string, *ErrorResponse) {
	/* converts []any to []string */
	var stringSlice []string
	for _, v := range anySlice {
		str, ok := v.(string)
		if !ok {
			return nil, newErrorResponse(fmt.Sprintf("Element %v is not a string", v), 500, nil)
		}
		stringSlice = append(stringSlice, str)
	}
	return stringSlice, nil
}
