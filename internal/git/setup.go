package git

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/integrations/syfon"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
)

type SetupService struct {
	db           *sqlx.DB
	gitService   *GitService
	storage      *syfon.Manager
	accessChecks *servermw.FenceUserAccessHandler
}

func NewSetupService(db *sqlx.DB, gitService *GitService, storage *syfon.Manager, accessChecks *servermw.FenceUserAccessHandler) *SetupService {
	return &SetupService{
		db:           db,
		gitService:   gitService,
		storage:      storage,
		accessChecks: accessChecks,
	}
}

func (service *SetupService) InitializeProject(ctx context.Context, authorizationHeader, organization, project string, request CalyprProjectSetupRequest) (*CalyprProjectInitializeResponse, error) {
	projectID := strings.TrimSpace(organization) + "/" + strings.TrimSpace(project)
	request.Config.OrgTitle = organization
	if strings.TrimSpace(request.Config.ProjectTitle) == "" {
		request.Config.ProjectTitle = project
	}
	if strings.TrimSpace(request.Config.Title) == "" {
		request.Config.Title = project
	}
	if err := request.Config.ValidateInitialization(); err != nil {
		return nil, NewError(ErrorKindValidation, http.StatusBadRequest, fmt.Sprintf("body data validation failed: %s", err), map[string]any{"project_id": projectID})
	}

	createdResourcePaths, err := service.ensureProjectOwnershipResources(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, WrapError(ErrorKindForbidden, http.StatusForbidden, "failed to ensure arborist ownership resources", err, map[string]any{"project_id": projectID})
	}

	if request.Storage != nil {
		request.Storage.Organization = organization
		request.Storage.ProjectID = project
		if err := service.PopulateStorageIntent(ctx, authorizationHeader, request.Storage); err != nil {
			if len(createdResourcePaths) > 0 {
				bestEffortDeleteAuthzResources(ctx, authorizationHeader, createdResourcePaths)
			}
			return nil, err
		}
	}

	if err := geckodb.ConfigPUTGeneric(service.db, projectID, string(config.TypeProjects), &request.Config); err != nil {
		if len(createdResourcePaths) > 0 {
			bestEffortDeleteAuthzResources(ctx, authorizationHeader, createdResourcePaths)
		}
		return nil, WrapError(ErrorKindDatabase, http.StatusInternalServerError, "configPut failed", err, map[string]any{"config_type": string(config.TypeProjects), "config_id": projectID})
	}

	return &CalyprProjectInitializeResponse{
		Success:      true,
		ProjectID:    projectID,
		ResourcePath: ProgramProjectResourcePath(organization, project),
	}, nil
}

func (service *SetupService) PopulateStorage(ctx context.Context, authorizationHeader, organization, project string, request CalyprProjectStorageRequest) (*CalyprProjectStorageResponse, error) {
	if request.Storage == nil {
		return nil, NewError(ErrorKindValidation, http.StatusBadRequest, "storage configuration is required", map[string]any{"organization": organization, "project": project})
	}
	request.Storage.Organization = organization
	request.Storage.ProjectID = project
	if err := service.PopulateStorageIntent(ctx, authorizationHeader, request.Storage); err != nil {
		return nil, err
	}
	storageStatus, err := service.StorageCheck(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	return &CalyprProjectStorageResponse{
		Success:      true,
		ProjectID:    organization + "/" + project,
		ResourcePath: ProgramProjectResourcePath(organization, project),
		Storage:      storageStatus,
	}, nil
}

func (service *SetupService) PopulateStorageIntent(ctx context.Context, authorizationHeader string, intent *CalyprProjectStorageIntent) error {
	if intent == nil {
		return nil
	}
	storageConfig := storageConfigFromIntent(intent)
	if err := service.storage.PutBucket(ctx, authorizationHeader, storageConfig); err != nil {
		return WrapError(ErrorKindIntegration, http.StatusBadGateway, "failed to configure syfon bucket", err, map[string]any{"project_id": strings.TrimSpace(intent.Organization) + "/" + strings.TrimSpace(intent.ProjectID)})
	}
	if err := service.storage.AddScope(ctx, authorizationHeader, storageConfig); err != nil {
		return WrapError(ErrorKindIntegration, http.StatusBadGateway, "failed to configure syfon scope", err, map[string]any{"project_id": strings.TrimSpace(intent.Organization) + "/" + strings.TrimSpace(intent.ProjectID)})
	}
	return nil
}

func storageConfigFromIntent(intent *CalyprProjectStorageIntent) StorageConfig {
	if intent == nil {
		return StorageConfig{}
	}
	return StorageConfig{
		Bucket:              strings.TrimSpace(intent.Bucket),
		Provider:            strings.TrimSpace(intent.Provider),
		Endpoint:            strings.TrimSpace(intent.Endpoint),
		Region:              strings.TrimSpace(intent.Region),
		AccessKey:           strings.TrimSpace(intent.AccessKey),
		SecretKey:           strings.TrimSpace(intent.SecretKey),
		Organization:        strings.TrimSpace(intent.Organization),
		ProjectID:           strings.TrimSpace(intent.ProjectID),
		Path:                strings.TrimSpace(intent.Path),
		PathPrefix:          strings.TrimSpace(intent.PathPrefix),
		OrganizationSubPath: strings.TrimSpace(intent.OrganizationSubPath),
		ProjectSubPath:      strings.TrimSpace(intent.ProjectSubPath),
	}
}

func (service *SetupService) StorageCheck(ctx context.Context, authorizationHeader, organization, project string) (ProjectIntegrationCheck, error) {
	buckets, err := service.storage.ListBuckets(ctx, authorizationHeader)
	if err != nil {
		return ProjectIntegrationCheck{
			Pass:    false,
			Reason:  "missing_storage_scope",
			Details: err.Error(),
		}, nil
	}
	return deriveStorageSetupCheck(buckets, organization, project), nil
}

func (service *SetupService) CleanupProjectStorage(ctx context.Context, authorizationHeader, organization, project string) error {
	if err := service.storage.CleanupProject(ctx, authorizationHeader, organization, project); err != nil {
		return WrapError(ErrorKindIntegration, http.StatusBadGateway, "failed to delete syfon project state", err, map[string]any{"project_id": organization + "/" + project})
	}
	return nil
}

func deriveStorageSetupCheck(buckets map[string]StorageBucket, organization string, project string) ProjectIntegrationCheck {
	check := ProjectIntegrationCheck{
		Pass:   false,
		Reason: "missing_storage_scope",
	}
	expectedPrograms := fmt.Sprintf("/programs/%s/projects/%s", strings.TrimSpace(organization), strings.TrimSpace(project))
	expectedOrganization := fmt.Sprintf("/organization/%s/project/%s", strings.TrimSpace(organization), strings.TrimSpace(project))
	for _, metadata := range buckets {
		for _, resource := range metadata.Resources {
			normalized := strings.TrimSpace(resource)
			if normalized == expectedPrograms || normalized == expectedOrganization {
				check.Pass = true
				check.Reason = ""
				check.Details = ""
				return check
			}
		}
	}
	check.Details = "No Syfon bucket scope matched this project"
	return check
}

type arboristOwnedDescendantRequest struct {
	Name       string `json:"name"`
	ParentPath string `json:"parent_path"`
	Template   string `json:"template"`
}

func fenceIssuerBaseURL(authorizationHeader string) (string, error) {
	token := servermw.CleanAccessToken(authorizationHeader)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(token, claims); err != nil {
		return "", fmt.Errorf("failed to parse authorization token: %w", err)
	}
	iss, _ := claims["iss"].(string)
	iss = strings.TrimSpace(iss)
	if iss == "" {
		return "", fmt.Errorf("authorization token does not include iss")
	}
	return strings.TrimSuffix(strings.TrimSuffix(iss, "/user"), "/"), nil
}

func arboristOwnedDescendantURL(authorizationHeader string) (string, error) {
	baseURL, err := fenceIssuerBaseURL(authorizationHeader)
	if err != nil {
		return "", err
	}
	return baseURL + "/authz/ownership/descendant", nil
}

func arboristOwnershipResourceURL(authorizationHeader, resourcePath string) (string, error) {
	baseURL, err := fenceIssuerBaseURL(authorizationHeader)
	if err != nil {
		return "", err
	}
	return baseURL + "/authz/ownership/resource?resource_path=" + url.QueryEscape(strings.TrimSpace(resourcePath)), nil
}

func createAuthzOwnedDescendant(ctx context.Context, authorizationHeader string, request arboristOwnedDescendantRequest) error {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal arborist descendant request: %w", err)
	}
	endpoint, err := arboristOwnedDescendantURL(authorizationHeader)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("build arborist descendant request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request arborist descendant create: %w", err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("arborist descendant create failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func DeleteAuthzResource(ctx context.Context, authorizationHeader, resourcePath string) error {
	endpoint, err := arboristOwnershipResourceURL(authorizationHeader, resourcePath)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build arborist resource delete request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request arborist resource delete: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("arborist resource delete failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func bestEffortDeleteAuthzResources(ctx context.Context, authorizationHeader string, resourcePaths []string) {
	for i := len(resourcePaths) - 1; i >= 0; i-- {
		_ = DeleteAuthzResource(ctx, authorizationHeader, resourcePaths[i])
	}
}

func (service *SetupService) ensureProjectOwnershipResources(ctx context.Context, authorizationHeader, organization, project string) ([]string, error) {
	created := []string{}
	projectResource := ProgramProjectResourcePath(organization, project)
	orgProjectsResource := fmt.Sprintf("/programs/%s/projects", organization)
	orgResource := fmt.Sprintf("/programs/%s", organization)

	projectReadable, err := service.accessChecks.CheckResourceServiceAccess(authorizationHeader, "read", "*", projectResource)
	if err != nil {
		return nil, err
	}
	if projectReadable {
		return created, nil
	}

	orgCanCreateProject, err := service.accessChecks.CheckResourceServiceAccess(authorizationHeader, "create-descendant", "arborist", orgProjectsResource)
	if err != nil {
		return nil, err
	}
	orgManageOwners, err := service.accessChecks.CheckResourceServiceAccess(authorizationHeader, "manage-owners", "arborist", orgResource)
	if err != nil {
		return nil, err
	}
	if !orgCanCreateProject && !orgManageOwners {
		if err := createAuthzOwnedDescendant(ctx, authorizationHeader, arboristOwnedDescendantRequest{
			Name:       organization,
			ParentPath: "/programs",
			Template:   "gen3-program",
		}); err != nil {
			return created, err
		}
		created = append(created, orgResource)
	}

	if err := createAuthzOwnedDescendant(ctx, authorizationHeader, arboristOwnedDescendantRequest{
		Name:       project,
		ParentPath: fmt.Sprintf("/programs/%s/projects", organization),
		Template:   "gen3-project",
	}); err != nil {
		if len(created) > 0 {
			bestEffortDeleteAuthzResources(ctx, authorizationHeader, created)
		}
		return nil, err
	}
	created = append(created, projectResource)
	return created, nil
}
