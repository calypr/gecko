package config

import (
	"strings"

	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
)

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
