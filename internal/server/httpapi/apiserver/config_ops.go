package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/calypr/gecko/apgen/server/gecko"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/httputil"
)

// ListConfigs implements GET /config/list.
func (s *Server) ListConfigs(ctx context.Context, request gecko.ListConfigsRequestObject) (gecko.ListConfigsResponseObject, error) {
	configType := ""
	if request.Params.Type != nil {
		configType = string(*request.Params.Type)
	}
	if configType == "" {
		configType = string(appconfig.TypeProjects)
	}
	if !appconfig.IsKnownType(configType) {
		return nil, newOpError(http.StatusBadRequest, "invalid_config_type", fmt.Sprintf("Unknown config type: %s", configType), map[string]any{"config_type": configType})
	}
	if configType == string(appconfig.TypeProjects) {
		projects, err := s.ConfigService.ListProjectIdentities(ctx)
		if err != nil {
			return nil, fromAppError(err)
		}
		items := make([]gecko.ListConfigs200JSONResponseBody_Item, 0, len(projects))
		for _, item := range projects {
			items = append(items, jsonStringItem(item.ID))
		}
		return gecko.ListConfigs200JSONResponse(items), nil
	}
	ids, err := s.ConfigService.ListIDs(ctx, configType)
	if err != nil {
		return nil, fromAppError(err)
	}
	items := make([]gecko.ListConfigs200JSONResponseBody_Item, 0, len(ids))
	for _, id := range ids {
		items = append(items, jsonStringItem(id))
	}
	return gecko.ListConfigs200JSONResponse(items), nil
}

// ListTypedConfigs implements GET /config/{configType}/list.
func (s *Server) ListTypedConfigs(ctx context.Context, request gecko.ListTypedConfigsRequestObject) (gecko.ListTypedConfigsResponseObject, error) {
	configType := request.ConfigType
	ids, err := s.ConfigService.ListIDs(ctx, configType)
	if err != nil {
		return nil, fromAppError(err)
	}
	return gecko.ListTypedConfigs200JSONResponse(ids), nil
}

// GetExplorerConfig implements GET /config/explorer. The legacy route resolves
// the missing config id to the default id, which the explorer authorization
// check rejects because it is not an org-project pair, so this operation fails
// validation the same way.
func (s *Server) GetExplorerConfig(ctx context.Context, request gecko.GetExplorerConfigRequestObject) (gecko.GetExplorerConfigResponseObject, error) {
	if err := s.explorerConfigAuthz(ctx, appconfig.DefaultConfigID, "read"); err != nil {
		return nil, err
	}
	return nil, fromAppError(fmt.Errorf("unreachable"))
}

// GetConfig implements GET /config/{configType}/{configId}.
func (s *Server) GetConfig(ctx context.Context, request gecko.GetConfigRequestObject) (gecko.GetConfigResponseObject, error) {
	configType, configID := request.ConfigType, request.ConfigId
	if configType == string(appconfig.TypeExplorer) {
		if err := s.explorerConfigAuthz(ctx, configID, "read"); err != nil {
			return nil, err
		}
	}
	config, err := s.ConfigService.Get(ctx, configType, configID)
	if err != nil {
		return nil, fromAppError(err)
	}
	cfg, err := toConfig(config)
	if err != nil {
		return nil, err
	}
	return gecko.GetConfig200JSONResponse(cfg), nil
}

// PutConfig implements PUT /config/{configType}/{configId}.
func (s *Server) PutConfig(ctx context.Context, request gecko.PutConfigRequestObject) (gecko.PutConfigResponseObject, error) {
	configType, configID := request.ConfigType, request.ConfigId
	if configType == string(appconfig.TypeExplorer) {
		if err := s.explorerConfigAuthz(ctx, configID, "create"); err != nil {
			return nil, err
		}
	} else {
		return nil, newOpError(http.StatusForbidden, "forbidden", "Route must use route-specific authorization; refusing global /programs fallback", map[string]any{"method": "PUT", "config_type": configType})
	}
	target, err := configuration.ConfigForType(configType)
	if err != nil {
		return nil, fromAppError(err)
	}
	bodyBytes, err := json.Marshal(request.Body)
	if err != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "invalid request body", map[string]any{"config_type": configType, "config_id": configID})
	}
	if errResponse := httputil.ParseJSONBody(bodyBytes, target, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	if err := s.ConfigService.Put(ctx, configType, configID, target); err != nil {
		return nil, fromAppError(err)
	}
	return gecko.PutConfig200JSONResponse(statusResponse(fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType))), nil
}

// DeleteConfig implements DELETE /config/{configType}/{configId}.
func (s *Server) DeleteConfig(ctx context.Context, request gecko.DeleteConfigRequestObject) (gecko.DeleteConfigResponseObject, error) {
	configType, configID := request.ConfigType, request.ConfigId
	if configType == string(appconfig.TypeExplorer) {
		if err := s.explorerConfigAuthz(ctx, configID, "create"); err != nil {
			return nil, err
		}
	} else {
		return nil, newOpError(http.StatusForbidden, "forbidden", "Route must use route-specific authorization; refusing global /programs fallback", map[string]any{"method": "DELETE", "config_type": configType})
	}
	if err := s.ConfigService.Delete(ctx, configType, configID); err != nil {
		return nil, fromAppError(err)
	}
	return gecko.DeleteConfig200JSONResponse(statusResponse(fmt.Sprintf("DELETED: %s from type: %s", configID, configType))), nil
}

// ListProjects implements GET /config/projects.
func (s *Server) ListProjects(ctx context.Context, request gecko.ListProjectsRequestObject) (gecko.ListProjectsResponseObject, error) {
	projects, err := s.listProjectsForCaller(ctx)
	if err != nil {
		return nil, err
	}
	return gecko.ListProjects200JSONResponse(projects), nil
}

// ListProjectsAlias implements GET /config/projects/list.
func (s *Server) ListProjectsAlias(ctx context.Context, request gecko.ListProjectsAliasRequestObject) (gecko.ListProjectsAliasResponseObject, error) {
	projects, err := s.listProjectsForCaller(ctx)
	if err != nil {
		return nil, err
	}
	return gecko.ListProjectsAlias200JSONResponse(projects), nil
}

func (s *Server) listProjectsForCaller(ctx context.Context) ([]gecko.ProjectListResponse, error) {
	allowedResources, errResponse := access.AllowedReadResources(ctx, AuthorizationFromContext(ctx))
	if errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	projects, err := s.ConfigService.ListProjectsForResources(ctx, allowedResources)
	if err != nil {
		return nil, fromAppError(err)
	}
	out := make([]gecko.ProjectListResponse, 0, len(projects))
	for _, item := range projects {
		cfg := appconfig.ProjectConfig{}
		if item.Config != nil {
			cfg = *item.Config
		}
		summary := s.buildProjectSummary(item.ID, cfg)
		data, mapErr := toMap(cfg)
		if mapErr != nil {
			return nil, mapErr
		}
		entry := gecko.ProjectListResponse{
			ResourcePath: access.ProgramProjectResourcePath(item.Organization, item.Name),
			ConfigData:   data,
			Organization: item.Organization,
			Project:      item.Name,
			Title:        summary.Title,
			ContactEmail: summary.ContactEmail,
			Description:  summary.Description,
		}
		if summary.ThumbnailURL != "" {
			entry.ThumbnailUrl = &summary.ThumbnailURL
		}
		out = append(out, entry)
	}
	return out, nil
}

// ListProjectSummaries implements GET /config/projects/summary.
func (s *Server) ListProjectSummaries(ctx context.Context, request gecko.ListProjectSummariesRequestObject) (gecko.ListProjectSummariesResponseObject, error) {
	projects, err := s.ConfigService.ListProjects(ctx)
	if err != nil {
		return nil, fromAppError(err)
	}
	summaries := make([]gecko.ProjectSummaryResponse, 0, len(projects))
	for _, item := range projects {
		cfg := appconfig.ProjectConfig{}
		if item.Config != nil {
			cfg = *item.Config
		}
		summary := s.buildProjectSummary(item.ID, cfg)
		entry := gecko.ProjectSummaryResponse{
			Organization: summary.Organization,
			Project:      summary.Project,
			Title:        summary.Title,
			ContactEmail: summary.ContactEmail,
			Description:  summary.Description,
		}
		if summary.ThumbnailURL != "" {
			entry.ThumbnailUrl = &summary.ThumbnailURL
		}
		summaries = append(summaries, entry)
	}
	sort.Slice(summaries, func(i, j int) bool {
		leftTitle := strings.ToLower(strings.TrimSpace(summaries[i].Title))
		rightTitle := strings.ToLower(strings.TrimSpace(summaries[j].Title))
		if leftTitle != rightTitle {
			return leftTitle < rightTitle
		}
		leftOrg := strings.ToLower(strings.TrimSpace(summaries[i].Organization))
		rightOrg := strings.ToLower(strings.TrimSpace(summaries[j].Organization))
		if leftOrg != rightOrg {
			return leftOrg < rightOrg
		}
		return strings.ToLower(strings.TrimSpace(summaries[i].Project)) <
			strings.ToLower(strings.TrimSpace(summaries[j].Project))
	})
	return gecko.ListProjectSummaries200JSONResponse(summaries), nil
}

// DeleteOrganizationProjects implements DELETE /config/projects/{orgTitle}.
func (s *Server) DeleteOrganizationProjects(ctx context.Context, request gecko.DeleteOrganizationProjectsRequestObject) (gecko.DeleteOrganizationProjectsResponseObject, error) {
	organization := strings.TrimSpace(request.OrgTitle)
	if organization == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization is required", nil)
	}
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), map[string]any{"organization": organization})
	}
	if err := s.DeleteService.DeleteOrganization(ctx, authorizationHeader, organization); err != nil {
		return nil, fromAppError(err)
	}
	success := true
	return gecko.DeleteOrganizationProjects200JSONResponse{Success: &success}, nil
}

// GetProjectConfig implements GET /config/projects/{orgTitle}/{projectTitle}.
func (s *Server) GetProjectConfig(ctx context.Context, request gecko.GetProjectConfigRequestObject) (gecko.GetProjectConfigResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	if err := s.projectConfigAuthz(ctx, organization, project, "read"); err != nil {
		return nil, err
	}
	config, err := s.ConfigService.Get(ctx, string(appconfig.TypeProjects), organization+"/"+project)
	if err != nil {
		return nil, fromAppError(err)
	}
	cfg, err := toConfig(config)
	if err != nil {
		return nil, err
	}
	return gecko.GetProjectConfig200JSONResponse(cfg), nil
}

// PutProjectConfig implements PUT /config/projects/{orgTitle}/{projectTitle}.
func (s *Server) PutProjectConfig(ctx context.Context, request gecko.PutProjectConfigRequestObject) (gecko.PutProjectConfigResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	configID := organization + "/" + project
	target, err := configuration.ConfigForType(string(appconfig.TypeProjects))
	if err != nil {
		return nil, fromAppError(err)
	}
	bodyBytes, err := json.Marshal(request.Body)
	if err != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "invalid request body", map[string]any{"config_type": string(appconfig.TypeProjects), "config_id": configID})
	}
	if errResponse := httputil.ParseJSONBody(bodyBytes, target, map[string]any{"config_type": string(appconfig.TypeProjects), "config_id": configID}); errResponse != nil {
		s.logError(errResponse)
		return nil, opErrorFromResponse(errResponse)
	}
	if err := s.ConfigService.Put(ctx, string(appconfig.TypeProjects), configID, target); err != nil {
		return nil, fromAppError(err)
	}
	return gecko.PutProjectConfig200JSONResponse(statusResponse(fmt.Sprintf("ACCEPTED: %s for type: %s", configID, appconfig.TypeProjects))), nil
}

// DeleteProjectConfig implements DELETE /config/projects/{orgTitle}/{projectTitle}.
func (s *Server) DeleteProjectConfig(ctx context.Context, request gecko.DeleteProjectConfigRequestObject) (gecko.DeleteProjectConfigResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if organization == "" || project == "" {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "organization and project are required", nil)
	}
	if err := s.projectConfigAuthz(ctx, organization, project, "delete"); err != nil {
		return nil, err
	}
	configID := organization + "/" + project
	authorizationHeader, err := access.ValidateAuthorizationHeader(AuthorizationFromContext(ctx))
	if err != nil {
		return nil, newOpError(http.StatusUnauthorized, string(apierrorMissingAuth), err.Error(), map[string]any{"config_type": string(appconfig.TypeProjects), "config_id": configID})
	}
	if err := s.DeleteService.DeleteProject(ctx, authorizationHeader, organization, project, configID); err != nil {
		return nil, fromAppError(err)
	}
	return gecko.DeleteProjectConfig200JSONResponse(statusResponse(fmt.Sprintf("DELETED: %s from type: %s", configID, appconfig.TypeProjects))), nil
}

type projectSummary struct {
	Organization string
	Project      string
	Title        string
	ContactEmail string
	Description  string
	ThumbnailURL string
}

// buildProjectSummary mirrors config.Handler.buildProjectSummaryResponse.
func (s *Server) buildProjectSummary(projectID string, cfg appconfig.ProjectConfig) projectSummary {
	organization, projectName, found := strings.Cut(projectID, "/")
	if !found {
		return projectSummary{}
	}
	organization = strings.TrimSpace(organization)
	projectName = strings.TrimSpace(projectName)
	title := strings.TrimSpace(cfg.Title)
	if title == "" {
		title = strings.TrimSpace(cfg.ProjectTitle)
	}
	if title == "" {
		title = projectName
	}
	summary := projectSummary{
		Organization: organization,
		Project:      projectName,
		Title:        title,
		ContactEmail: strings.TrimSpace(cfg.ContactEmail),
		Description:  strings.TrimSpace(cfg.Description),
	}
	if s.ThumbnailStore != nil {
		if _, _, err := s.ThumbnailStore.GetPath(organization, projectName); err == nil {
			summary.ThumbnailURL = fmt.Sprintf("/gecko/git/projects/%s/%s/thumbnail", url.PathEscape(organization), url.PathEscape(projectName))
		}
	}
	return summary
}
