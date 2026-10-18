package project

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/domain"
)

func integrationError(err error, details map[string]any, fallback string) error {
	if statusErr, ok := err.(*domain.HTTPStatusError); ok {
		return domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type(statusErr.Code),
			statusErr.StatusCode,
			statusErr.Message,
			details,
		)
	}
	return domain.NewTypedError(
		domain.ErrorKindIntegration,
		apierror.Type("integration_error"),
		http.StatusBadGateway,
		fmt.Sprintf("%s: %s", fallback, err),
		details,
	)
}

func projectRepositoryError(projectID string, operation string, err error) error {
	return domain.NewTypedError(
		domain.ErrorKindDatabase,
		apierror.TypeDatabaseError,
		http.StatusInternalServerError,
		fmt.Sprintf("%s: %s", operation, err),
		map[string]any{"project_id": projectID},
	)
}
