package git

import (
	"fmt"
	"net/url"
	"strings"
)

func OrganizationProjectResourcePath(organization, project string) string {
	return fmt.Sprintf("/organization/%s/project/%s", organization, project)
}

func ProgramProjectResourcePath(organization, project string) string {
	return fmt.Sprintf("/programs/%s/projects/%s", organization, project)
}

func ProjectAccessResourcePaths(organization, project string) []string {
	return []string{
		OrganizationProjectResourcePath(organization, project),
		ProgramProjectResourcePath(organization, project),
	}
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
