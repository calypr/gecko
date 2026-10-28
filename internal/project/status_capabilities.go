package project

import (
	"fmt"

	"github.com/calypr/gecko/internal/access"
)

func ApplyOrganizationStatusCapabilities(response *GitOrganizationStatusResponse, snapshot access.ResourceAccessSnapshot) {
	if response == nil {
		return
	}
	response.CanAccessSettings = true
	orgResource := fmt.Sprintf("/programs/%s", response.Organization)
	orgProjectsResource := fmt.Sprintf("/programs/%s/projects", response.Organization)
	response.CanCreateProjects = access.ResourceAccessAllows(snapshot, orgProjectsResource, "create-descendant", "arborist")
	response.CanManagePeople = access.ResourceAccessAllows(snapshot, orgResource, "manage-owners", "arborist")
	response.CanDeleteOrg = response.CanManagePeople || access.ResourceAccessAllows(snapshot, orgResource, "delete", "*")
	for i := range response.Projects {
		response.Projects[i].CanManageSettings = access.ResourceAccessAllows(snapshot, response.Projects[i].ResourcePath, "update", "*")
	}
}

func ApplyOrganizationsStatusCapabilities(responses []GitOrganizationStatusResponse, snapshot access.ResourceAccessSnapshot) {
	for i := range responses {
		ApplyOrganizationStatusCapabilities(&responses[i], snapshot)
	}
}
