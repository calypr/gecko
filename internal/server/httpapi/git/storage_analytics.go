package git

import (
	"fmt"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/gitanalytics"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/gofiber/fiber/v3"
)

const defaultStorageChildrenLimit = 100
const maxStorageChildrenLimit = 1000

type storageChildrenRequestOptions struct {
	limit       int
	cursor      string
	sortBy      string
	sortOrder   string
	summaryMode string
}

func (handler *Handler) handleGitProjectStorageSummaryGET(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	gitSubpath := normalizeAnalyticsSubpath(strings.TrimSpace(ctx.Query("git_subpath")))
	response, err := handler.Analytics.BuildSummary(ctx.Context(), projectCtx, gitSubpath)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, gitSubpath), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageChildrenGET(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	gitSubpath := normalizeAnalyticsSubpath(strings.TrimSpace(ctx.Query("git_subpath")))
	options, errResponse := parseStorageChildrenRequestOptions(ctx, projectCtx.ProjectID)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	response, err := handler.Analytics.BuildChildren(ctx.Context(), projectCtx, gitSubpath, options.limit, options.sortBy, options.sortOrder, options.cursor)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, gitSubpath), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageFolderGET(ctx fiber.Ctx) error {
	resolveStart := time.Now()
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	resolveDuration := time.Since(resolveStart)
	gitSubpath := normalizeAnalyticsSubpath(strings.TrimSpace(ctx.Query("git_subpath")))
	options, errResponse := parseStorageChildrenRequestOptions(ctx, projectCtx.ProjectID)
	if errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	summaryMode := strings.TrimSpace(options.summaryMode)
	if summaryMode == "" {
		summaryMode = storageaudit.StorageFolderSummarySourceGitIndex
	}
	timings := &storageaudit.StorageFolderTimings{
		DebugPrefix: fmt.Sprintf(
			"project_id=%s ref=%s git_subpath=%q limit=%d sort_by=%q sort_order=%q cursor=%t summary_mode=%s",
			projectCtx.ProjectID,
			projectCtx.RefName,
			gitSubpath,
			options.limit,
			options.sortBy,
			options.sortOrder,
			options.cursor != "",
			summaryMode,
		),
		Logf: handler.Logger.Info,
	}
	timings.Record("resolve_git_context", resolveDuration)
	forceRefresh := parseBoolQuery(ctx.Query("force_refresh")) || parseBoolQuery(ctx.Query("force_audit_refresh")) || parseBoolQuery(ctx.Query("force_bucket_inventory_refresh"))
	buildStart := time.Now()
	response, err := handler.Analytics.BuildFolder(ctx.Context(), projectCtx, gitSubpath, options.limit, options.sortBy, options.sortOrder, options.cursor, options.summaryMode, forceRefresh, timings)
	timings.Record("build_folder", time.Since(buildStart))
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, gitSubpath), handler.Logger)
	}
	writeStart := time.Now()
	writeErr := httputil.JSON(response, http.StatusOK).Write(ctx)
	timings.Record("write_response", time.Since(writeStart))
	handler.Logger.Info("storage_folder_request_complete %s", timings.DebugPrefix)
	return writeErr
}

func parseStorageChildrenRequestOptions(ctx fiber.Ctx, projectID string) (storageChildrenRequestOptions, *httputil.ErrorResponse) {
	limit, err := parseStorageChildrenLimit(strings.TrimSpace(ctx.Query("limit")))
	if err != nil {
		return storageChildrenRequestOptions{}, httputil.NewError("invalid_request", err.Error(), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
	}
	return storageChildrenRequestOptions{
		limit:       limit,
		cursor:      strings.TrimSpace(ctx.Query("cursor")),
		sortBy:      strings.TrimSpace(ctx.Query("sort_by")),
		sortOrder:   strings.TrimSpace(ctx.Query("sort_order")),
		summaryMode: strings.TrimSpace(ctx.Query("summary_mode")),
	}, nil
}

func parseBoolQuery(raw string) bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	return err == nil && parsed
}

func parseStorageChildrenLimit(rawLimit string) (int, error) {
	if rawLimit == "" {
		return defaultStorageChildrenLimit, nil
	}
	parsed, err := strconv.Atoi(rawLimit)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if parsed > maxStorageChildrenLimit {
		return maxStorageChildrenLimit, nil
	}
	return parsed, nil
}

func (handler *Handler) handleGitProjectDiffAuditPOST(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	requestBody := storageaudit.ProjectDiffAuditRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.ProjectID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	projectCtx.ApplyRef(requestBody.Ref)
	gitSubpath := normalizeAnalyticsSubpath(requestBody.GitSubpath)
	response, err := handler.Analytics.BuildDiffAudit(ctx.Context(), projectCtx, requestBody, gitSubpath)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, gitSubpath), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageCleanupAuditPOST(ctx fiber.Ctx) error {
	projectCtx, requestBody, errResponse := handler.parseCleanupAnalyticsRequest(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	response, err := handler.Analytics.BuildCleanupAudit(ctx.Context(), projectCtx, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, requestBody.GitSubpath), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectStorageChainAuditPOST(ctx fiber.Ctx) error {
	routeOrg := strings.TrimSpace(ctx.Params("orgTitle"))
	routeProject := strings.TrimSpace(ctx.Params("projectTitle"))
	handler.Logger.Info("storage_chain_audit_request_received org=%s project=%s path=%s query=%s", routeOrg, routeProject, ctx.Path(), string(ctx.Request().URI().QueryString()))
	timings := &storageaudit.StorageChainAuditTimings{
		Logf:        handler.Logger.Info,
		DebugPrefix: fmt.Sprintf("org=%s project=%s", routeOrg, routeProject),
	}
	resolveStart := time.Now()
	timings.StageStart("resolve_git_context")
	projectCtx, errResponse := handler.resolveGitAnalyticsContextWithTimings(ctx, timings)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	timings.Record("resolve_git_context", time.Since(resolveStart))
	timings.DebugPrefix = fmt.Sprintf("project_id=%s ref=%s git_subpath=%q", projectCtx.ProjectID, projectCtx.RefName, "")
	requestBody := storageaudit.ChainAuditRequest{}
	parseStart := time.Now()
	timings.StageStart("parse_request_body")
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.ProjectID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	timings.Record("parse_request_body", time.Since(parseStart))
	projectCtx.ApplyRef(requestBody.Ref)
	normalized, normalizeErr := storageaudit.NormalizeStorageChainAuditRequest(requestBody)
	if normalizeErr != nil {
		response := httputil.NewError("invalid_request", normalizeErr.Error(), http.StatusBadRequest, map[string]any{"project_id": projectCtx.ProjectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	gitSubpath := normalized.GitSubpath
	timings.DebugPrefix = fmt.Sprintf("project_id=%s ref=%s git_subpath=%q validation_mode=%s probe_mode=%s bucket_inventory_mode=%s bucket_path_prefix=%q finding_kind=%q finding_limit=%d", projectCtx.ProjectID, projectCtx.RefName, gitSubpath, normalized.Options.ValidationMode, normalized.Options.ProbeMode, normalized.Options.BucketInventoryMode, normalized.BucketPathPrefix, normalized.FindingKind, normalized.Options.FindingLimit)
	handler.Logger.Info("storage_chain_audit_request_start %s", timings.DebugPrefix)
	normalized.Options.Timings = timings
	response, err := handler.Analytics.BuildChainAudit(ctx.Context(), projectCtx, gitSubpath, normalized.Options)
	if err != nil {
		handler.Logger.Info("storage_chain_audit_request_error %s error=%q timings=%s", timings.DebugPrefix, err.Error(), formatStorageChainTimingSnapshot(timings))
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, gitSubpath), handler.Logger)
	}
	writeStart := time.Now()
	timings.StageStart("json_response")
	writeErr := httputil.JSON(response, http.StatusOK).Write(ctx)
	timings.Record("json_response", time.Since(writeStart))
	timings.RecordMemory(
		"json_response",
		"total_findings", response.Summary.TotalFindings,
		"returned_findings", response.Summary.ReturnedFindings,
		"bucket_objects", response.Summary.BucketObjectCount,
		"syfon_records", response.Summary.SyfonRecordCount,
		"git_files", response.Summary.GitTrackedFileCount,
	)
	handler.logStorageChainAuditTimings(projectCtx, gitSubpath, normalized.Options.ProbeMode, normalized.Options.BucketInventoryMode, response, timings)
	return writeErr
}

func (handler *Handler) handleGitProjectStorageChainRegisterGitOnlyPOST(ctx fiber.Ctx) error {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	requestBody := storageaudit.GitOnlySyfonRegistrationRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.ProjectID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	projectCtx.ApplyRef(requestBody.Ref)
	requestBody.Ref = projectCtx.RefName
	storageaudit.NormalizeGitOnlySyfonRegistrationRequest(&requestBody)
	response, err := handler.Analytics.RegisterGitOnly(ctx.Context(), projectCtx, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, ""), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func formatStorageChainTimingSnapshot(timings *storageaudit.StorageChainAuditTimings) string {
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
	requestBody := storageaudit.CleanupApplyRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.ProjectID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return errResponse.Write(ctx)
	}
	projectCtx.ApplyRef(requestBody.Ref)
	storageaudit.NormalizeCleanupApplyRequest(&requestBody)
	response, err := handler.Analytics.ApplyCleanup(ctx.Context(), projectCtx, requestBody)
	if err != nil {
		return core.WriteAppError(ctx, gitanalytics.WrapError(err, projectCtx, requestBody.GitSubpath), handler.Logger)
	}
	return httputil.JSON(response, http.StatusOK).Write(ctx)
}

func (handler *Handler) parseCleanupAnalyticsRequest(ctx fiber.Ctx) (*gitanalytics.Target, storageaudit.CleanupAuditRequest, *httputil.ErrorResponse) {
	projectCtx, errResponse := handler.resolveGitAnalyticsContext(ctx)
	if errResponse != nil {
		return nil, storageaudit.CleanupAuditRequest{}, errResponse
	}
	requestBody := storageaudit.CleanupAuditRequest{}
	if errResponse := parseOptionalAnalyticsBody(ctx, &requestBody, map[string]any{"project_id": projectCtx.ProjectID}); errResponse != nil {
		errResponse.WriteLog(handler.Logger)
		return nil, storageaudit.CleanupAuditRequest{}, errResponse
	}
	projectCtx.ApplyRef(requestBody.Ref)
	storageaudit.NormalizeCleanupAuditRequest(&requestBody)
	return projectCtx, requestBody, nil
}

func (handler *Handler) resolveGitAnalyticsContext(ctx fiber.Ctx) (*gitanalytics.Target, *httputil.ErrorResponse) {
	return handler.resolveGitAnalyticsContextWithMirrorPolicy(ctx, nil, false)
}

func (handler *Handler) resolveGitAnalyticsContextWithTimings(ctx fiber.Ctx, timings *storageaudit.StorageChainAuditTimings) (*gitanalytics.Target, *httputil.ErrorResponse) {
	return handler.resolveGitAnalyticsContextWithMirrorPolicy(ctx, timings, true)
}

func (handler *Handler) resolveGitAnalyticsContextWithMirrorPolicy(ctx fiber.Ctx, timings *storageaudit.StorageChainAuditTimings, requireCurrentMirror bool) (*gitanalytics.Target, *httputil.ErrorResponse) {
	if handler.StorageAnalytics == nil {
		response := httputil.NewError("internal_error", "storage analytics service is not configured", http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.Logger)
		return nil, response
	}
	target, err := handler.Analytics.Resolve(
		ctx.Context(),
		ctx.Params("orgTitle"),
		ctx.Params("projectTitle"),
		ctx.Get("Authorization"),
		ctx.Query("ref"),
		requireCurrentMirror,
		timings,
	)
	if err != nil {
		return nil, core.ErrorResponse(err)
	}
	return target, nil
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

func (handler *Handler) logStorageChainAuditTimings(projectCtx *gitanalytics.Target, gitSubpath string, probeMode string, bucketMode string, response *storageaudit.ChainAuditResponse, timings *storageaudit.StorageChainAuditTimings) {
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
	handler.Logger.Info(
		"storage_chain_audit project_id=%s ref=%s git_subpath=%q validation_mode=%s probe_mode=%s bucket_inventory_mode=%s bucket_path_exists=%s bucket_summary_mode=%s bucket_inventory_available=%t audit_cache_hit=%t audit_cache_source=%s audit_cache_age_seconds=%d audit_refresh_duration_ms=%d findings=%d returned_findings=%d findings_truncated=%t finding_limit=%d bucket_objects=%d syfon_records=%d git_files=%d %s",
		projectCtx.ProjectID,
		projectCtx.RefName,
		gitSubpath,
		response.Summary.ValidationMode,
		probeMode,
		bucketMode,
		bucketPathExists,
		response.Summary.BucketSummaryMode,
		response.Summary.BucketInventoryAvailable,
		response.Summary.AuditCacheHit,
		response.Summary.AuditCacheSource,
		response.Summary.AuditCacheAgeSeconds,
		response.Summary.AuditRefreshDurationMs,
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
