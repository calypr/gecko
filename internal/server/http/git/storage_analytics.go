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

const defaultStorageChainFindingLimit = 500

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
	routeOrg := strings.TrimSpace(ctx.Params("orgTitle"))
	routeProject := strings.TrimSpace(ctx.Params("projectTitle"))
	handler.logger.Info("storage_chain_audit_request_received org=%s project=%s path=%s query=%s", routeOrg, routeProject, ctx.Path(), string(ctx.Request().URI().QueryString()))
	timings := &gitcore.StorageChainAuditTimings{
		Logf:        handler.logger.Info,
		DebugPrefix: fmt.Sprintf("org=%s project=%s", routeOrg, routeProject),
	}
	resolveStart := time.Now()
	timings.StageStart("resolve_git_context")
	projectCtx, errResponse := handler.resolveGitAnalyticsContextWithTimings(ctx, timings)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	timings.Record("resolve_git_context", time.Since(resolveStart))
	timings.DebugPrefix = fmt.Sprintf("project_id=%s ref=%s git_subpath=%q", projectCtx.projectID, projectCtx.refName, "")
	requestBody := gitcore.GitStorageChainAuditRequest{}
	parseStart := time.Now()
	timings.StageStart("parse_request_body")
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	timings.Record("parse_request_body", time.Since(parseStart))
	probeMode, ok := gitcore.NormalizeStorageChainProbeMode(requestBody.ProbeMode)
	if !ok {
		response := httputil.NewError("invalid_request", "probe_mode must be either full or inventory_only", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	validationMode := gitcore.StorageChainValidationModeList
	if strings.TrimSpace(requestBody.ValidationMode) != "" {
		var valid bool
		validationMode, valid = gitcore.NormalizeStorageChainValidationMode(requestBody.ValidationMode)
		if !valid {
			response := httputil.NewError("invalid_request", "validation_mode must be either list, metadata, or inventory", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	} else if strings.TrimSpace(requestBody.ProbeMode) != "" {
		validationMode = gitcore.DefaultStorageChainValidationMode(probeMode, "")
	}
	bucketMode := gitcore.StorageChainBucketModeValidate
	if strings.TrimSpace(requestBody.BucketInventoryMode) != "" {
		var valid bool
		bucketMode, valid = gitcore.NormalizeStorageChainBucketInventoryMode(requestBody.BucketInventoryMode)
		if !valid {
			response := httputil.NewError("invalid_request", "bucket_inventory_mode must be either validate or items", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	}
	if validationMode == gitcore.StorageChainValidationModeInventory && strings.TrimSpace(requestBody.BucketInventoryMode) == "" {
		bucketMode = gitcore.StorageChainBucketModeItems
	}
	findingLimit := requestBody.FindingLimit
	if findingLimit == 0 {
		findingLimit = defaultStorageChainFindingLimit
	}
	if findingLimit < -1 {
		response := httputil.NewError("invalid_request", "finding_limit must be -1 for all findings or a positive integer", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	bucketPathPrefix := normalizeAnalyticsSubpath(requestBody.BucketPathPrefix)
	if bucketPathPrefix != "" && bucketMode != gitcore.StorageChainBucketModeItems {
		response := httputil.NewError("invalid_request", "bucket_path_prefix requires bucket_inventory_mode=items", http.StatusBadRequest, map[string]any{"project_id": projectCtx.projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectCtx.applyRequestRef(requestBody.Ref)
	gitSubpath := normalizeAnalyticsSubpath(requestBody.GitSubpath)
	timings.DebugPrefix = fmt.Sprintf("project_id=%s ref=%s git_subpath=%q validation_mode=%s probe_mode=%s bucket_inventory_mode=%s bucket_path_prefix=%q finding_limit=%d", projectCtx.projectID, projectCtx.refName, gitSubpath, validationMode, probeMode, bucketMode, bucketPathPrefix, findingLimit)
	handler.logger.Info("storage_chain_audit_request_start %s", timings.DebugPrefix)
	response, err := handler.storageAnalytics.BuildStorageChainAuditWithOptions(
		ctx.Context(),
		projectCtx.authorizationHeader,
		projectCtx.organization,
		projectCtx.project,
		projectCtx.refName,
		gitSubpath,
		projectCtx.mirrorPath,
		projectCtx.repo,
		projectCtx.hash,
		gitcore.StorageChainAuditOptions{
			ProbeMode:           probeMode,
			ValidationMode:      validationMode,
			BucketInventoryMode: bucketMode,
			BucketPathPrefix:    bucketPathPrefix,
			FindingLimit:        findingLimit,
			Timings:             timings,
		},
	)
	if err != nil {
		handler.logger.Info("storage_chain_audit_request_error %s error=%q timings=%s", timings.DebugPrefix, err.Error(), formatStorageChainTimingSnapshot(timings))
		return handler.writeGitAnalyticsError(ctx, projectCtx.projectID, projectCtx.refName, gitSubpath, err)
	}
	writeStart := time.Now()
	timings.StageStart("json_response")
	writeErr := httputil.JSON(response, http.StatusOK).Write(ctx)
	timings.Record("json_response", time.Since(writeStart))
	handler.logStorageChainAuditTimings(projectCtx, gitSubpath, probeMode, bucketMode, response, timings)
	return writeErr
}

func formatStorageChainTimingSnapshot(timings *gitcore.StorageChainAuditTimings) string {
	parts := make([]string, 0)
	for _, stage := range timings.Snapshot() {
		if stage.Stage == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s_ms=%d", stage.Stage, stage.Duration.Milliseconds()))
	}
	return strings.Join(parts, ",")
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
	return handler.resolveGitAnalyticsContextWithTimings(ctx, nil)
}

func (handler *Handler) resolveGitAnalyticsContextWithTimings(ctx fiber.Ctx, timings *gitcore.StorageChainAuditTimings) (*gitAnalyticsContext, *httputil.ErrorResponse) {
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
		start := time.Now()
		timings.StageStart("mirror_warmup")
		refreshCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		state, err = handler.ensureMirrorReadyForRead(refreshCtx, authorizationHeader, projectID, identity, state)
		timings.Record("mirror_warmup", time.Since(start))
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

func (handler *Handler) logStorageChainAuditTimings(projectCtx *gitAnalyticsContext, gitSubpath string, probeMode string, bucketMode string, response *gitcore.GitStorageChainAuditResponse, timings *gitcore.StorageChainAuditTimings) {
	if projectCtx == nil || response == nil {
		return
	}
	parts := make([]string, 0)
	for _, stage := range timings.Snapshot() {
		if stage.Stage == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s_ms=%d", stage.Stage, stage.Duration.Milliseconds()))
	}
	bucketPathExists := "unknown"
	if response.Summary.BucketPathExists != nil {
		bucketPathExists = strconv.FormatBool(*response.Summary.BucketPathExists)
	}
	handler.logger.Info(
		"storage_chain_audit project_id=%s ref=%s git_subpath=%q validation_mode=%s probe_mode=%s bucket_inventory_mode=%s bucket_path_exists=%s bucket_summary_mode=%s bucket_inventory_available=%t findings=%d returned_findings=%d findings_truncated=%t finding_limit=%d bucket_objects=%d syfon_records=%d git_files=%d %s",
		projectCtx.projectID,
		projectCtx.refName,
		gitSubpath,
		response.Summary.ValidationMode,
		probeMode,
		bucketMode,
		bucketPathExists,
		response.Summary.BucketSummaryMode,
		response.Summary.BucketInventoryAvailable,
		response.Summary.TotalFindings,
		response.Summary.ReturnedFindings,
		response.Summary.FindingsTruncated,
		response.Summary.FindingLimit,
		response.Summary.BucketObjectCount,
		response.Summary.SyfonRecordCount,
		response.Summary.GitTrackedFileCount,
		strings.Join(parts, " "),
	)
}
