package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
)

const syfonRefreshAuthzHeader = "X-Syfon-Refresh-Authz"

func syfonDataAPIBaseURL() string {
	return strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL"))
}

type syfonBucketListResponse map[string]struct {
	Bucket   string   `json:"bucket"`
	Programs []string `json:"programs"`
}

type syfonBucketListEnvelope struct {
	S3Buckets syfonBucketListResponse `json:"S3_BUCKETS"`
}

func fetchSyfonBuckets(ctx context.Context, authorizationHeader string) (syfonBucketListResponse, error) {
	baseURL := syfonDataAPIBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/buckets", nil)
	if err != nil {
		return nil, fmt.Errorf("build syfon bucket list request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set(syfonRefreshAuthzHeader, "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request syfon bucket list: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("syfon bucket list failed with status %d", resp.StatusCode)
	}

	var envelope syfonBucketListEnvelope
	if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.S3Buckets) > 0 {
		return envelope.S3Buckets, nil
	}

	var buckets syfonBucketListResponse
	if err := json.Unmarshal(body, &buckets); err != nil {
		return nil, fmt.Errorf("decode syfon bucket list response: %w", err)
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("decode syfon bucket list response: no buckets found")
	}
	return buckets, nil
}

func hasSyfonProjectScopeFromBuckets(buckets syfonBucketListResponse, organization string, project string) bool {
	org := strings.TrimSpace(organization)
	proj := strings.TrimSpace(project)
	expectedPrograms := fmt.Sprintf("/programs/%s/projects/%s", org, proj)
	expectedOrganization := fmt.Sprintf("/organization/%s/project/%s", org, proj)
	for _, metadata := range buckets {
		for _, resource := range metadata.Programs {
			normalized := strings.TrimSpace(resource)
			if normalized == expectedPrograms || normalized == expectedOrganization {
				return true
			}
		}
	}
	return false
}

func upsertSyfonScope(ctx context.Context, authorizationHeader string, intent *git.CalyprProjectStorageIntent) error {
	if intent == nil {
		return nil
	}
	baseURL := syfonDataAPIBaseURL()
	if baseURL == "" {
		return fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}
	putBody, err := json.Marshal(buildSyfonPutBucketRequest(intent))
	if err != nil {
		return fmt.Errorf("marshal syfon bucket upsert request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(baseURL, "/")+"/buckets", bytes.NewReader(putBody))
	if err != nil {
		return fmt.Errorf("build syfon bucket upsert request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(syfonRefreshAuthzHeader, "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request syfon bucket upsert: %w", err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("syfon bucket upsert failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	scopeBody, err := json.Marshal(buildSyfonAddBucketScopeRequest(intent))
	if err != nil {
		return fmt.Errorf("marshal syfon bucket scope request: %w", err)
	}
	scopeURL := strings.TrimRight(baseURL, "/") + "/buckets/" + url.PathEscape(strings.TrimSpace(intent.Bucket)) + "/scopes"
	scopeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, scopeURL, bytes.NewReader(scopeBody))
	if err != nil {
		return fmt.Errorf("build syfon bucket scope request: %w", err)
	}
	scopeReq.Header.Set("Authorization", authorizationHeader)
	scopeReq.Header.Set("Content-Type", "application/json")
	scopeReq.Header.Set(syfonRefreshAuthzHeader, "true")
	scopeResp, err := http.DefaultClient.Do(scopeReq)
	if err != nil {
		return fmt.Errorf("request syfon add bucket scope: %w", err)
	}
	defer scopeResp.Body.Close()
	scopeResponseBody, _ := io.ReadAll(scopeResp.Body)
	if scopeResp.StatusCode >= 400 {
		return fmt.Errorf("syfon add bucket scope failed with status %d: %s", scopeResp.StatusCode, strings.TrimSpace(string(scopeResponseBody)))
	}
	return nil
}

func deleteSyfonProjectCleanup(ctx context.Context, authorizationHeader, organization, projectID string) error {
	baseURL := syfonDataAPIBaseURL()
	if baseURL == "" {
		return fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}
	cleanupURL := strings.TrimRight(baseURL, "/") +
		"/projects/" +
		url.PathEscape(strings.TrimSpace(organization)) +
		"/" +
		url.PathEscape(strings.TrimSpace(projectID))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, cleanupURL, nil)
	if err != nil {
		return fmt.Errorf("build syfon project cleanup request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set(syfonRefreshAuthzHeader, "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request syfon project cleanup: %w", err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("syfon project cleanup failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

type syfonPutBucketRequest struct {
	AccessKey    string `json:"access_key,omitempty"`
	Bucket       string `json:"bucket"`
	Endpoint     string `json:"endpoint,omitempty"`
	Organization string `json:"organization"`
	Path         string `json:"path,omitempty"`
	ProjectID    string `json:"project_id"`
	Provider     string `json:"provider,omitempty"`
	Region       string `json:"region,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
}

type syfonAddBucketScopeRequest struct {
	Organization string `json:"organization"`
	Path         string `json:"path,omitempty"`
	ProjectID    string `json:"project_id"`
}

func buildSyfonPutBucketRequest(intent *git.CalyprProjectStorageIntent) syfonPutBucketRequest {
	request := syfonPutBucketRequest{
		AccessKey:    strings.TrimSpace(intent.AccessKey),
		Bucket:       strings.TrimSpace(intent.Bucket),
		Endpoint:     strings.TrimSpace(intent.Endpoint),
		Organization: strings.TrimSpace(intent.Organization),
		ProjectID:    strings.TrimSpace(intent.ProjectID),
		Provider:     strings.TrimSpace(intent.Provider),
		Region:       strings.TrimSpace(intent.Region),
		SecretKey:    strings.TrimSpace(intent.SecretKey),
	}
	return request
}

func buildSyfonAddBucketScopeRequest(intent *git.CalyprProjectStorageIntent) syfonAddBucketScopeRequest {
	request := syfonAddBucketScopeRequest{
		Organization: strings.TrimSpace(intent.Organization),
		ProjectID:    strings.TrimSpace(intent.ProjectID),
	}
	if explicitPath := strings.TrimSpace(intent.Path); explicitPath != "" {
		request.Path = explicitPath
		return request
	}
	if pathPrefix := strings.Trim(strings.TrimSpace(intent.PathPrefix), "/"); pathPrefix != "" {
		request.Path = syfonBucketPath(intent.Provider, intent.Bucket, pathPrefix)
		return request
	}
	organizationSubPath := strings.Trim(strings.TrimSpace(intent.OrganizationSubPath), "/")
	projectSubPath := strings.Trim(strings.TrimSpace(intent.ProjectSubPath), "/")
	if organizationSubPath == "" && projectSubPath == "" {
		return request
	}
	request.Path = syfonBucketPath(intent.Provider, intent.Bucket, path.Join(organizationSubPath, projectSubPath))
	return request
}

func syfonBucketPath(provider string, bucket string, prefix string) string {
	cleanBucket := strings.TrimSpace(bucket)
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "gcs", "gs":
		if cleanPrefix == "" {
			return "gs://" + cleanBucket
		}
		return "gs://" + cleanBucket + "/" + cleanPrefix
	case "azure", "azblob", "az":
		if cleanPrefix == "" {
			return "azblob://" + cleanBucket
		}
		return "azblob://" + cleanBucket + "/" + cleanPrefix
	case "file":
		if cleanPrefix == "" {
			return "file://" + cleanBucket
		}
		return "file://" + cleanBucket + "/" + cleanPrefix
	default:
		if cleanPrefix == "" {
			return "s3://" + cleanBucket
		}
		return "s3://" + cleanBucket + "/" + cleanPrefix
	}
}

type arboristOwnedDescendantRequest struct {
	Name       string `json:"name"`
	ParentPath string `json:"parent_path"`
	Template   string `json:"template"`
}

func fenceIssuerBaseURL(authorizationHeader string) (string, error) {
	token := git.CleanAccessToken(authorizationHeader)
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
	if resp.StatusCode >= 400 {
		return fmt.Errorf("arborist descendant create failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func deleteAuthzResource(ctx context.Context, authorizationHeader, resourcePath string) error {
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
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("arborist resource delete failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func bestEffortDeleteAuthzResources(ctx context.Context, authorizationHeader string, resourcePaths []string) {
	for i := len(resourcePaths) - 1; i >= 0; i-- {
		_ = deleteAuthzResource(ctx, authorizationHeader, resourcePaths[i])
	}
}

func ensureProjectOwnershipResources(ctx context.Context, authorizationHeader, organization, project string) ([]string, error) {
	authzHandler := servermw.NewFenceUserAccessHandler(nil)
	created := []string{}
	projectResource := git.ProgramProjectResourcePath(organization, project)
	orgProjectsResource := fmt.Sprintf("/programs/%s/projects", organization)
	orgResource := fmt.Sprintf("/programs/%s", organization)

	projectReadable, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, "read", "*", projectResource)
	if err != nil {
		return nil, err
	}
	if projectReadable {
		return created, nil
	}

	orgCanCreateProject, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, "create-descendant", "arborist", orgProjectsResource)
	if err != nil {
		return nil, err
	}
	orgManageOwners, err := authzHandler.CheckResourceServiceAccess(authorizationHeader, "manage-owners", "arborist", orgResource)
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

func (handler *Handler) deriveProjectReadiness(
	ctx context.Context,
	authorizationHeader string,
	projectID string,
	cfg config.ProjectConfig,
	knownGitInstalled *bool,
	syfonBuckets syfonBucketListResponse,
	syfonBucketsErr error,
) git.CalyprProjectReadiness {
	readiness := git.CalyprProjectReadiness{
		Config: git.CalyprReadinessCheck{Pass: false, Reason: "missing_config"},
		Git:    git.CalyprReadinessCheck{Pass: false, Reason: "missing_repo_installation"},
		Syfon:  git.CalyprReadinessCheck{Pass: false, Reason: "missing_storage_scope"},
	}
	if strings.TrimSpace(cfg.SrcRepo) == "" {
		readiness.Config.Details = "src_repo is required"
		return readiness
	}
	if _, err := git.ParseRepositoryIdentity(cfg.SrcRepo); err != nil {
		readiness.Config.Details = fmt.Sprintf("invalid src_repo: %s", err)
		return readiness
	}
	readiness.Config = git.CalyprReadinessCheck{Pass: true}

	parts := strings.SplitN(projectID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		readiness.Config = git.CalyprReadinessCheck{Pass: false, Reason: "invalid_project_id", Details: "project ID must be <org>/<project>"}
		return readiness
	}
	organization := parts[0]
	project := parts[1]

	identity, _ := git.ParseRepositoryIdentity(cfg.SrcRepo)
	// Only trust positive installation state from upstream status assembly.
	// If known state is false, still do a live check to avoid stale-false drift.
	if knownGitInstalled != nil && *knownGitInstalled {
		readiness.Git = git.CalyprReadinessCheck{Pass: true}
	} else {
		installStatus, err := handler.gitService.RequestInstallationStatus(ctx, authorizationHeader, organization, identity)
		if err != nil {
			readiness.Git.Details = err.Error()
		} else if installStatus.Installed {
			readiness.Git = git.CalyprReadinessCheck{Pass: true}
		}
	}

	if syfonBucketsErr != nil {
		readiness.Syfon.Details = syfonBucketsErr.Error()
	} else if hasSyfonProjectScopeFromBuckets(syfonBuckets, organization, project) {
		readiness.Syfon = git.CalyprReadinessCheck{Pass: true}
	}
	return readiness
}

func (handler *Handler) handleCalyprProjectSetupPUT(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	var request git.CalyprProjectSetupRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &request, map[string]any{"organization": organization, "project": project}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	request.Config.OrgTitle = organization
	if strings.TrimSpace(request.Config.ProjectTitle) == "" {
		request.Config.ProjectTitle = project
	}
	if strings.TrimSpace(request.Config.Title) == "" {
		request.Config.Title = project
	}
	if strings.TrimSpace(request.Config.IconName) == "" {
		request.Config.IconName = "binoculars"
	}
	if err := request.Config.Validate(); err != nil {
		response := httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"organization": organization, "project": project}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	projectID := organization + "/" + project
	setupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	createdResourcePaths, err := ensureProjectOwnershipResources(setupCtx, authorizationHeader, organization, project)
	if err != nil {
		response := httputil.NewError("forbidden", fmt.Sprintf("failed to ensure arborist ownership resources: %s", err), http.StatusForbidden, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	if request.Storage != nil {
		request.Storage.Organization = organization
		request.Storage.ProjectID = project
		if err := upsertSyfonScope(setupCtx, authorizationHeader, request.Storage); err != nil {
			if len(createdResourcePaths) > 0 {
				bestEffortDeleteAuthzResources(setupCtx, authorizationHeader, createdResourcePaths)
			}
			response := httputil.NewError("integration_error", fmt.Sprintf("failed to configure syfon scope: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	}
	syfonBuckets, syfonBucketsErr := fetchSyfonBuckets(setupCtx, authorizationHeader)

	if err := geckodb.ConfigPUTGeneric(handler.db, projectID, string(config.TypeProjects), &request.Config); err != nil {
		if len(createdResourcePaths) > 0 {
			bestEffortDeleteAuthzResources(setupCtx, authorizationHeader, createdResourcePaths)
		}
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": string(config.TypeProjects), "config_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	pendingRepoID := strings.TrimSpace(request.PendingRepoID)
	if pendingRepoID != "" {
		pendingRepo, pendingErr := geckodb.GitPendingRepositoryByID(handler.db, pendingRepoID)
		if pendingErr != nil {
			if len(createdResourcePaths) > 0 {
				bestEffortDeleteAuthzResources(setupCtx, authorizationHeader, createdResourcePaths)
			}
			response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("lookup pending repository failed: %s", pendingErr), http.StatusInternalServerError, map[string]any{"project_id": projectID, "pending_repo_id": pendingRepoID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		if pendingRepo != nil {
			identity, _ := git.ParseRepositoryIdentity(request.Config.SrcRepo)
			state := geckodb.GitProjectState{
				ProjectID:  projectID,
				RepoHost:   identity.Host,
				RepoOwner:  identity.Owner,
				RepoName:   identity.Repo,
				MirrorPath: handler.gitService.MirrorPathForIdentity(identity),
				SyncState:  git.GitSyncNeverSynced,
				InstallationID: sql.NullInt64{
					Int64: pendingRepo.InstallationID,
					Valid: pendingRepo.InstallationID > 0,
				},
				InstallationTargetType: sql.NullString{String: "Organization", Valid: true},
				InstallationTarget:     sql.NullString{String: pendingRepo.Organization, Valid: pendingRepo.Organization != ""},
			}
			if err := geckodb.UpsertGitProjectState(handler.db, state); err != nil {
				if len(createdResourcePaths) > 0 {
					bestEffortDeleteAuthzResources(setupCtx, authorizationHeader, createdResourcePaths)
				}
				response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("upsert git project state failed: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
				response.WriteLog(handler.logger)
				return response.Write(ctx)
			}
			if err := geckodb.ResolveGitPendingRepositoryByID(handler.db, pendingRepoID); err != nil {
				if len(createdResourcePaths) > 0 {
					bestEffortDeleteAuthzResources(setupCtx, authorizationHeader, createdResourcePaths)
				}
				response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("resolve pending repository failed: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "pending_repo_id": pendingRepoID}, nil)
				response.WriteLog(handler.logger)
				return response.Write(ctx)
			}
		}
	}

	readiness := handler.deriveProjectReadiness(setupCtx, authorizationHeader, projectID, request.Config, nil, syfonBuckets, syfonBucketsErr)
	configured := readiness.Config.Pass && readiness.Git.Pass && readiness.Syfon.Pass
	response := git.CalyprProjectSetupResponse{
		ProjectID:    projectID,
		ResourcePath: git.ProgramProjectResourcePath(organization, project),
		Configured:   configured,
		Readiness:    readiness,
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}
