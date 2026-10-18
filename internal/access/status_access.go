package access

import (
	"context"
	"sort"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
)

type StatusAccess struct {
	ReadableResources []string
	Snapshot          ResourceAccessSnapshot
}

func ReadStatusAccess(ctx context.Context, token string) (*StatusAccess, *httputil.ErrorResponse) {
	if token == "" {
		return &StatusAccess{}, nil
	}
	handler := NewFenceUserAccessHandler(nil)
	snapshot, err := handler.GetResourceAccess(ctx, token)
	if err != nil {
		if accessErr, ok := err.(*AccessError); ok {
			return nil, httputil.NewError(apierror.TypeForStatus(accessErr.StatusCode), accessErr.Message, accessErr.StatusCode, nil, nil)
		}
		return nil, httputil.NewError(apierror.TypeAuthorizationServiceError, err.Error(), 403, nil, nil)
	}
	readableResources := make([]string, 0, len(snapshot))
	for resourcePath := range snapshot {
		if ResourceAccessAllows(snapshot, resourcePath, "read", "*") {
			readableResources = append(readableResources, resourcePath)
		}
	}
	sort.Strings(readableResources)
	return &StatusAccess{ReadableResources: readableResources, Snapshot: snapshot}, nil
}
