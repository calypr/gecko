package git

import (
	"context"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
)

// AllowedReadResources returns the Fence resource paths the caller may read.
// A missing token yields no resources and no error, so callers fall through to
// per-project filtering instead of failing the whole request.
func AllowedReadResources(ctx context.Context, token string) ([]string, *httputil.ErrorResponse) {
	if token == "" {
		return nil, nil
	}
	resources, err := servermw.GitAllowedResources(ctx, servermw.NewFenceUserAccessHandler(nil), token, "read")
	if err != nil {
		return nil, err
	}
	return resources, nil
}

// FilterProjectIDsByAllowedResources keeps only the "org/project" IDs the caller
// may read. IDs that are not exactly "org/project" are dropped. The result is
// sorted so callers get a stable order regardless of input order.
func FilterProjectIDsByAllowedResources(projectIDs []string, allowedResources []string) []string {
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
		if servermw.GitProjectReadable(allowedResources, parts[0], projectParts[0]) {
			filtered = append(filtered, projectID)
		}
	}
	sort.Strings(filtered)
	return filtered
}
