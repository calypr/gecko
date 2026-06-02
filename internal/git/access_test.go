package git

import "testing"

func TestResourceListAllowsProjectMatchesProgramResource(t *testing.T) {
	resources := []string{
		"/programs/Ellrott_Lab/projects/embedding_rotation",
		"/programs/Ellrott_Lab/projects/git_drs_test",
	}
	if !ResourceListAllowsProject(resources, "Ellrott_Lab", "git_drs_test") {
		t.Fatal("expected project access to be allowed")
	}
	if ResourceListAllowsProject(resources, "Ellrott_Lab", "missing") {
		t.Fatal("expected unrelated project access to be denied")
	}
}

func TestResourceListAllowsOrganizationMatchesProgramAndLegacyResources(t *testing.T) {
	resources := []string{
		"/programs/Ellrott_Lab/projects",
		"/programs/Ellrott_Lab/projects/git_drs_test",
		"/organization/HTAN_INT/project/BForePC",
	}

	if !ResourceListAllowsOrganization(resources, "Ellrott_Lab") {
		t.Fatal("expected program-scoped organization access to be allowed")
	}
	if !ResourceListAllowsOrganization(resources, "HTAN_INT") {
		t.Fatal("expected legacy organization access to be allowed")
	}
	if ResourceListAllowsOrganization(resources, "gdc_mirror") {
		t.Fatal("expected unrelated organization access to be denied")
	}
}
