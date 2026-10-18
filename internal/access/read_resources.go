package access

import (
	"context"
	"errors"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
)

// AllowedReadResources returns the Fence resource paths the caller may read.
// A missing token yields no resources and no error, so callers fall through to
// per-project filtering instead of failing the whole request.
func AllowedReadResources(ctx context.Context, token string) ([]string, *httputil.ErrorResponse) {
	if token == "" {
		return nil, nil
	}
	resources, err := AllowedResources(ctx, NewFenceUserAccessHandler(nil), token, "read", "*")
	if err != nil {
		return nil, authorizationErrorResponse(err)
	}
	return resources, nil
}

func authorizationErrorResponse(err error) *httputil.ErrorResponse {
	var accessErr *AccessError
	if errors.As(err, &accessErr) {
		return httputil.NewError(apierror.TypeForStatus(accessErr.StatusCode), accessErr.Message, accessErr.StatusCode, nil, nil)
	}
	return httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), 403, nil, nil)
}
