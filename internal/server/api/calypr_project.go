package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

const defaultReadinessTTL = 5 * time.Minute

type installationStatusCacheValue struct {
	expiresAt time.Time
	status    git.GitRepositoryInstallationStatus
}

var installationStatusCache = struct {
	mu   sync.RWMutex
	data map[string]installationStatusCacheValue
}{
	data: map[string]installationStatusCacheValue{},
}

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

func authHeaderHash(authorizationHeader string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(authorizationHeader)))
	return hex.EncodeToString(sum[:])
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

func installationStatusCacheKey(authorizationHeader string, identity git.GitRepositoryIdentity) string {
	return authHeaderHash(authorizationHeader) + ":" + strings.ToLower(strings.TrimSpace(identity.Owner)) + ":" + strings.ToLower(strings.TrimSpace(identity.Repo))
}

func getInstallationStatusCached(authorizationHeader string, identity git.GitRepositoryIdentity) (git.GitRepositoryInstallationStatus, bool) {
	key := installationStatusCacheKey(authorizationHeader, identity)
	now := time.Now()
	installationStatusCache.mu.RLock()
	entry, ok := installationStatusCache.data[key]
	installationStatusCache.mu.RUnlock()
	if !ok || now.After(entry.expiresAt) {
		return git.GitRepositoryInstallationStatus{}, false
	}
	return entry.status, true
}

func setInstallationStatusCached(authorizationHeader string, identity git.GitRepositoryIdentity, status git.GitRepositoryInstallationStatus) {
	key := installationStatusCacheKey(authorizationHeader, identity)
	installationStatusCache.mu.Lock()
	installationStatusCache.data[key] = installationStatusCacheValue{
		expiresAt: time.Now().Add(defaultReadinessTTL),
		status:    status,
	}
	installationStatusCache.mu.Unlock()
}

func upsertSyfonScope(ctx context.Context, authorizationHeader string, intent *git.CalyprProjectStorageIntent) error {
	if intent == nil {
		return nil
	}
	baseURL := syfonDataAPIBaseURL()
	if baseURL == "" {
		return fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}
	body, err := json.Marshal(intent)
	if err != nil {
		return fmt.Errorf("marshal syfon upsert request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(baseURL, "/")+"/buckets", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build syfon upsert request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request syfon upsert bucket scope: %w", err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("syfon upsert bucket scope failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
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
		installStatus, ok := getInstallationStatusCached(authorizationHeader, identity)
		var err error
		if !ok {
			installStatus, err = handler.gitService.RequestInstallationStatus(ctx, authorizationHeader, organization, identity)
			if err == nil {
				setInstallationStatusCached(authorizationHeader, identity, installStatus)
			}
		}
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

	if request.Storage != nil {
		request.Storage.Organization = organization
		request.Storage.ProjectID = project
		if err := upsertSyfonScope(setupCtx, authorizationHeader, request.Storage); err != nil {
			response := httputil.NewError("integration_error", fmt.Sprintf("failed to configure syfon scope: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	}
	syfonBuckets, syfonBucketsErr := fetchSyfonBuckets(setupCtx, authorizationHeader)

	if err := geckodb.ConfigPUTGeneric(handler.db, projectID, string(config.TypeProjects), &request.Config); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": string(config.TypeProjects), "config_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	pendingRepoID := strings.TrimSpace(request.PendingRepoID)
	if pendingRepoID != "" {
		pendingRepo, pendingErr := geckodb.GitPendingRepositoryByID(handler.db, pendingRepoID)
		if pendingErr != nil {
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
				response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("upsert git project state failed: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
				response.WriteLog(handler.logger)
				return response.Write(ctx)
			}
			if err := geckodb.ResolveGitPendingRepositoryByID(handler.db, pendingRepoID); err != nil {
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
