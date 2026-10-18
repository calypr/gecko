package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/access"
)

// The helpers below enforce per-route authorization inline so the generated
// router applies the same policy the legacy fiber middleware did: require a
// token, scope reads to the project/organization resource, and gate mutations
// (config, setup, org) on the matching Arborist access check.

func (s *Server) requireToken(ctx context.Context) *opError {
	if AuthorizationFromContext(ctx) == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	return nil
}

func (s *Server) projectReadAuthz(ctx context.Context, organization, project string) *opError {
	if s.Authz == nil {
		return nil
	}
	token := AuthorizationFromContext(ctx)
	if token == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	resources, err := access.AllowedResources(ctx, s.Authz, token, "read", "*")
	if err != nil {
		return fromAccessError(err)
	}
	if access.ResourceListAllowsProject(resources, organization, project) {
		return nil
	}
	return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to read on project %s/%s", organization, project), map[string]any{
		"organization":                 organization,
		"project":                      project,
		"method":                       "read",
		"resource_path":                access.ProgramProjectResourcePath(organization, project),
		"request_access":               true,
		"request_access_resource_path": access.ProgramProjectResourcePath(organization, project),
	})
}

func (s *Server) projectConfigAuthz(ctx context.Context, organization, project, method string) *opError {
	token := AuthorizationFromContext(ctx)
	if token == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	resourcePath := access.ProgramProjectResourcePath(organization, project)
	allowed, err := s.Authz.CheckResourceServiceAccess(ctx, token, method, "*", resourcePath)
	if err != nil {
		return fromAccessError(err)
	}
	if !allowed {
		anyList, listErr := s.Authz.GetAllowedResources(ctx, token, method, "*")
		if listErr != nil {
			return fromAccessError(listErr)
		}
		resources, convErr := toStringSlice(anyList)
		if convErr != nil {
			return convErr
		}
		allowed = resourceListAllowsProjectAdminAction(resources, organization, project)
	}
	if !allowed {
		return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User does not have required %s permission on resource %s", method, resourcePath), map[string]any{
			"resource":     resourcePath,
			"method":       method,
			"organization": organization,
			"project":      project,
		})
	}
	return nil
}

func (s *Server) projectSetupAuthz(ctx context.Context, organization, project string) *opError {
	token := AuthorizationFromContext(ctx)
	if token == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	projectResource := access.ProgramProjectResourcePath(organization, project)
	orgProjectsResource := fmt.Sprintf("/programs/%s/projects", organization)
	orgResource := fmt.Sprintf("/programs/%s", organization)
	checks := []struct {
		method   string
		service  string
		resource string
	}{
		{method: "read", service: "*", resource: projectResource},
		{method: "create-descendant", service: "arborist", resource: orgProjectsResource},
		{method: "manage-owners", service: "arborist", resource: orgResource},
	}
	for _, check := range checks {
		allowed, err := s.Authz.CheckResourceServiceAccess(ctx, token, check.method, check.service, check.resource)
		if err != nil {
			return fromAccessError(err)
		}
		if allowed {
			return nil
		}
	}
	return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User does not have required setup permission on resource %s", projectResource), map[string]any{
		"resource":      projectResource,
		"organization":  organization,
		"project":       project,
		"org_resource":  orgResource,
		"projects_path": orgProjectsResource,
	})
}

func (s *Server) orgReadAuthz(ctx context.Context, organization string) *opError {
	if s.Authz == nil {
		return nil
	}
	token := AuthorizationFromContext(ctx)
	if token == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	allowed, err := s.Authz.GetAllowedResources(ctx, token, "read", "*")
	if err != nil {
		return newOpError(http.StatusForbidden, "authorization_service_error", fmt.Sprintf("authorization lookup failed: %s", err), nil)
	}
	resources, convErr := toStringSlice(allowed)
	if convErr != nil {
		return convErr
	}
	if access.ResourceListAllowsOrganization(resources, organization) {
		return nil
	}
	return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to read organization %s", organization), map[string]any{"organization": organization, "method": "read"})
}

// explorerConfigAuthz mirrors GeneralAuth for the explorer config group: the
// config id must be an "org-project" pair and the caller must hold permMethod
// on the exact project resource.
func (s *Server) explorerConfigAuthz(ctx context.Context, configID, permMethod string) *opError {
	token := AuthorizationFromContext(ctx)
	if token == "" {
		return newOpError(http.StatusUnauthorized, "missing_authorization", "Authorization token not provided", nil)
	}
	parts := strings.Split(configID, "-")
	if len(parts) != 2 {
		return newOpError(http.StatusBadRequest, "invalid_project_id", "organization and project are required", map[string]any{"project_id": configID})
	}
	anyList, err := s.Authz.GetAllowedResources(ctx, token, permMethod, "*")
	if err != nil {
		return fromAccessError(err)
	}
	resourceList, convErr := toStringSlice(anyList)
	if convErr != nil {
		return convErr
	}
	resource := "/programs/" + parts[0] + "/projects/" + parts[1]
	if len(resourceList) == 0 {
		return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to %s on any resource path", permMethod), map[string]any{"resource": resource, "method": permMethod})
	}
	for _, candidate := range resourceList {
		if candidate == resource {
			return nil
		}
	}
	return newOpError(http.StatusForbidden, "forbidden", fmt.Sprintf("User is not allowed to %s on resource path: %s", permMethod, resource), map[string]any{"resource": resource, "method": permMethod})
}

func resourceListAllowsProjectAdminAction(resources []string, organization, project string) bool {
	projectResource := access.ProgramProjectResourcePath(organization, project)
	projectCollectionResource := fmt.Sprintf("/programs/%s/projects", organization)
	organizationResource := fmt.Sprintf("/programs/%s", organization)
	for _, resource := range resources {
		switch resource {
		case "*", "/", "/programs", organizationResource, projectCollectionResource, projectResource:
			return true
		}
	}
	return false
}

func fromAccessError(err error) *opError {
	if accessErr, ok := err.(*access.AccessError); ok {
		return newOpError(accessErr.StatusCode, string(apierror.TypeForStatus(accessErr.StatusCode)), accessErr.Message, nil)
	}
	return newOpError(http.StatusForbidden, string(apierror.TypeAuthorizationServiceError), err.Error(), nil)
}

func toStringSlice(anyList []any) ([]string, *opError) {
	out := make([]string, 0, len(anyList))
	for _, value := range anyList {
		item, ok := value.(string)
		if !ok {
			return nil, newOpError(http.StatusInternalServerError, "invalid_authorization_response", fmt.Sprintf("Element %v is not a string", value), nil)
		}
		out = append(out, item)
	}
	return out, nil
}
