package access

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

func ProgramProjectResourcePath(organization, project string) string {
	return fmt.Sprintf("/programs/%s/projects/%s", organization, project)
}

func ProjectAccessResourcePaths(organization, project string) []string {
	return []string{ProgramProjectResourcePath(organization, project)}
}

func NormalizeResourcePath(resource string) string {
	trimmed := strings.TrimSpace(resource)
	if trimmed == "" {
		return ""
	}
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Path != "" {
		trimmed = parsed.Path
	}
	trimmed = "/" + strings.Trim(trimmed, "/")
	return strings.TrimSuffix(trimmed, "/")
}

func ResourceListAllowsProject(resources []string, organization, project string) bool {
	expected := ProjectAccessResourcePaths(organization, project)
	for _, resource := range resources {
		normalized := NormalizeResourcePath(resource)
		for _, candidate := range expected {
			if normalized == candidate {
				return true
			}
		}
	}
	return false
}

func ResourcePathOrganization(resource string) (string, bool) {
	normalized := NormalizeResourcePath(resource)
	parts := strings.Split(normalized, "/")
	if len(parts) < 3 {
		return "", false
	}
	switch parts[1] {
	case "programs":
		if parts[2] != "" {
			return parts[2], true
		}
	}
	return "", false
}

func ResourceListAllowsOrganization(resources []string, organization string) bool {
	for _, resource := range resources {
		if resourceOrganization, ok := ResourcePathOrganization(resource); ok && resourceOrganization == organization {
			return true
		}
	}
	return false
}

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
		if ResourceListAllowsProject(allowedResources, parts[0], projectParts[0]) {
			filtered = append(filtered, projectID)
		}
	}
	sort.Strings(filtered)
	return filtered
}
