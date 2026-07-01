package git

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	gitcore "github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectStorageSummaryGET(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	gitSubpath := normalizeAnalyticsSubpath(strings.TrimSpace(ctx.Query("git_subpath")))
	response, err := handler.storageAnalytics.BuildStorageSummary(ctx.Context(), projectCtx.authorizationHeader, projectCtx.organization, projectCtx.project, projectCtx.refName, gitSubpath, projectCtx.mirrorPath, projectCtx.repo, projectCtx.hash)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, gitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageChildrenGET(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	gitSubpath := normalizeAnalyticsSubpath(strings.TrimSpace(ctx.Query("git_subpath")))
	limit := 1000
	if rawLimit := strings.TrimSpace(ctx.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			response := httputil.NewError("invalid_request", "limit must be a positive integer", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		limit = parsed
	}
	response, err := handler.storageAnalytics.BuildStorageChildren(
		ctx.Context(),
		projectCtx.authorizationHeader,
		projectCtx.organization,
		projectCtx.project,
		projectCtx.refName,
		gitSubpath,
		projectCtx.mirrorPath,
		projectCtx.repo,
		projectCtx.hash,
		limit,
		strings.TrimSpace(ctx.Query("sort_by")),
		strings.TrimSpace(ctx.Query("sort_order")),
	)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, gitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectDiffAuditPOST(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	requestBody := gitcore.GitProjectDiffAuditRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectCtx.applyRequestRef(requestBody.Ref)
	gitSubpath := normalizeAnalyticsSubpath(requestBody.GitSubpath)
	response, err := handler.storageAnalytics.BuildProjectDiffAudit(ctx.Context(), projectCtx.authorizationHeader, projectCtx.organization, projectCtx.project, projectCtx.refName, gitSubpath, projectCtx.mirrorPath, projectCtx.repo, projectCtx.hash)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, gitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageCleanupAuditPOST(ctx fiber.Ctx) error {
	projectCtx, requestBody, errResponse := handler.parseCleanupAnalyticsRequest(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	response, _, err := handler.storageAnalytics.BuildStorageCleanupAudit(
		ctx.Context(),
		projectCtx.authorizationHeader,
		projectCtx.organization,
		projectCtx.project,
		projectCtx.refName,
		requestBody.GitSubpath,
		requestBody.SelectedRepoPaths,
		projectCtx.mirrorPath,
		projectCtx.repo,
		projectCtx.hash,
		requestBody.CheckStorage,
	)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, requestBody.GitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageChainAuditPOST(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	requestBody := gitcore.GitStorageChainAuditRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectCtx.applyRequestRef(requestBody.Ref)
	gitSubpath := normalizeAnalyticsSubpath(requestBody.GitSubpath)
	response, err := handler.storageAnalytics.BuildStorageChainAudit(
		ctx.Context(),
		projectCtx.authorizationHeader,
		projectCtx.organization,
		projectCtx.project,
		projectCtx.refName,
		gitSubpath,
		projectCtx.mirrorPath,
		projectCtx.repo,
		projectCtx.hash,
	)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, gitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageCleanupApplyPOST(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	requestBody := gitcore.GitStorageCleanupApplyRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectCtx.applyRequestRef(requestBody.Ref)
	requestBody.GitSubpath = normalizeAnalyticsSubpath(requestBody.GitSubpath)
	requestBody.SelectedRepoPaths = normalizeAnalyticsPathList(requestBody.SelectedRepoPaths)
	response, err := handler.storageAnalytics.ApplyStorageCleanup(
		ctx.Context(),
		projectCtx.authorizationHeader,
		projectCtx.organization,
		projectCtx.project,
		projectCtx.refName,
		requestBody.GitSubpath,
		requestBody.SelectedRepoPaths,
		projectCtx.mirrorPath,
		projectCtx.repo,
		projectCtx.hash,
		true,
		requestBody.DeleteRepoOrphans,
		requestBody.DeleteStaleDuplicates,
		requestBody.DeleteBucketOnlyObjects,
		requestBody.RepairBrokenBucketMappings,
		requestBody.DryRun,
	)
	if err != nil {
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, requestBody.GitSubpath, err)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

type gitAnalyticsContext struct {
	organization        string
	project             string
	projectID           string
	authorizationHeader string
	defaultBranch       string
	refName             string
	mirrorPath          string
	repo                *gogit.Repository
	hash                plumbing.Hash
}

func (ctx *gitAnalyticsContext) applyRequestRef(ref string) {
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == ctx.refName {
		return
	}
	refName, hash, err := gitcore.ResolveGitReference(ctx.repo, ref, ctx.defaultBranch)
	if err != nil {
		return
	}
	ctx.refName = refName
	ctx.hash = hash
}

func (handler *Handler) parseCleanupAnalyticsRequest(ctx fiber.Ctx) (*gitAnalyticsContext, gitcore.GitStorageCleanupAuditRequest, *httputil.ErrorResponse) {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return nil, gitcore.GitStorageCleanupAuditRequest{}, errResponse
	}
	requestBody := gitcore.GitStorageCleanupAuditRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return nil, gitcore.GitStorageCleanupAuditRequest{}, errResponse
	}
	projectCtx.applyRequestRef(requestBody.Ref)
	requestBody.GitSubpath = normalizeAnalyticsSubpath(requestBody.GitSubpath)
	requestBody.SelectedRepoPaths = normalizeAnalyticsPathList(requestBody.SelectedRepoPaths)
	return projectCtx, requestBody, nil
}

func (handler *Handler) resolveGitAnalyticsContext(ctx fiber.Ctx) (*gitAnalyticsContext, *httputil.ErrorResponse) {
	if handler.storageAnalytics == nil {
		response := httputil.NewError("internal_error", "storage analytics service is not configured", http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	organization, project, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return nil, errResponse
	}
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	authorizationHeader := strings.TrimSpace(ctx.Get("Authorization"))
	if authorizationHeader != "" {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state)
		if err != nil {
			handler.logger.Warning("failed to warm git mirror for %s analytics: %v", projectID, err)
		}
	}
	repo, err := gitcore.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	if gitcore.RepositoryIsEmpty(repo) {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has no Git content yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	defaultBranch := state.DefaultBranch.String
	refName, hash, err := gitcore.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), defaultBranch)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	return &gitAnalyticsContext{
		organization:        organization,
		project:             project,
		projectID:           projectID,
		authorizationHeader: authorizationHeader,
		defaultBranch:       defaultBranch,
		refName:             refName,
		mirrorPath:          state.MirrorPath,
		repo:                repo,
		hash:                hash,
	}, nil
}

func parseOptionalAnalyticsBody(ctx fiber.Ctx, target any, details map[string]any) *httputil.ErrorResponse {
	body := ctx.Body()
	if len(body) == 0 {
		return nil
	}
	return httputil.ParseJSONBody(body, target, details)
}

func normalizeAnalyticsSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func normalizeAnalyticsPathList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := normalizeAnalyticsSubpath(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func (handler *Handler) writeGitAnalyticsError(ctx fiber.Ctx, projectID string, ref string, gitSubpath string, err error) error {
	statusCode := http.StatusBadGateway
	errorType := "integration_error"
	if strings.Contains(strings.ToLower(err.Error()), "git tree path") {
		statusCode = http.StatusNotFound
		errorType = "not_found"
	}
	response := httputil.NewError(apierror.Type(errorType), err.Error(), statusCode, map[string]any{
		"project_id":  projectID,
		"ref":         ref,
		"git_subpath": gitSubpath,
	}, nil)
	response.WriteLog(handler.logger)
	return response.Write(ctx)
}
