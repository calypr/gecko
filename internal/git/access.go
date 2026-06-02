package git

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
	case "programs", "organization":
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

func ResourceListAllowedOrganizations(resources []string) []string {
	seen := make(map[string]struct{})
	for _, resource := range resources {
		if organization, ok := ResourcePathOrganization(resource); ok {
			seen[organization] = struct{}{}
		}
	}
	organizations := make([]string, 0, len(seen))
	for organization := range seen {
		organizations = append(organizations, organization)
	}
	sort.Strings(organizations)
	return organizations
}
