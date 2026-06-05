package config

import (
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

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

func gitAllowedReadResources(token string) ([]string, *httputil.ErrorResponse) {
	if token == "" {
		return nil, nil
	}
	resources, err := servermw.GitAllowedResources(servermw.NewFenceUserAccessHandler(nil), token, "read")
	if err != nil {
		return nil, err
	}
	return resources, nil
}

func filterProjectIDsByAllowedResources(projectIDs []string, allowedResources []string) []string {
	if len(allowedResources) == 0 {
		return []string{}
	}
	filtered := make([]string, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 {
			continue
		}
		projectParts := strings.SplitN(parts[1], "/", 2)
		if len(projectParts) != 1 || projectParts[0] == "" {
			continue
		}
		if servermw.GitProjectReadable(allowedResources, parts[0], projectParts[0]) || servermw.ResourceListAllowsProject(allowedResources, parts[0], projectParts[0]) {
			filtered = append(filtered, projectID)
		}
	}
	return filtered
}
