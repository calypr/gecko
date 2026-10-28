package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/gitanalytics"
	"github.com/calypr/gecko/internal/storageaudit"
)

const (
	defaultStorageChildrenLimit = 100
	maxStorageChildrenLimit     = 1000
)

func (s *Server) resolveAnalyticsContext(ctx context.Context, organization, project, ref string, requireCurrentMirror bool) (*gitanalytics.Target, *opError) {
	if s.StorageAnalytics == nil {
		return nil, newOpError(http.StatusInternalServerError, "internal_error", "storage analytics service is not configured", nil)
	}
	target, err := s.Analytics.Resolve(ctx, organization, project, AuthorizationFromContext(ctx), ref, requireCurrentMirror, nil)
	if err != nil {
		return nil, fromAppError(err)
	}
	return target, nil
}

// GetGitProjectStorageSummary implements GET /git/projects/{orgTitle}/{projectTitle}/storage/summary.
func (s *Server) GetGitProjectStorageSummary(ctx context.Context, request gecko.GetGitProjectStorageSummaryRequestObject) (gecko.GetGitProjectStorageSummaryResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	gitSubpath := normalizeAnalyticsSubpath(queryValue(request.Params.GitSubpath))
	response, err := s.Analytics.BuildSummary(ctx, target, gitSubpath)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, gitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectStorageSummary200JSONResponse(m), nil
}

// GetGitProjectStorageChildren implements GET /git/projects/{orgTitle}/{projectTitle}/storage/children.
func (s *Server) GetGitProjectStorageChildren(ctx context.Context, request gecko.GetGitProjectStorageChildrenRequestObject) (gecko.GetGitProjectStorageChildrenResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	gitSubpath := normalizeAnalyticsSubpath(queryValue(request.Params.GitSubpath))
	limit, opErr := parseStorageChildrenLimit(queryInt(request.Params.Limit))
	if opErr != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", opErr.Error(), map[string]any{"project_id": target.ProjectID})
	}
	response, err := s.Analytics.BuildChildren(ctx, target, gitSubpath, limit, queryValue(request.Params.SortBy), queryValue(request.Params.SortOrder), queryValue(request.Params.Cursor))
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, gitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectStorageChildren200JSONResponse(m), nil
}

// GetGitProjectStorageFolder implements GET /git/projects/{orgTitle}/{projectTitle}/storage/folder.
func (s *Server) GetGitProjectStorageFolder(ctx context.Context, request gecko.GetGitProjectStorageFolderRequestObject) (gecko.GetGitProjectStorageFolderResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	resolveStart := time.Now()
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	gitSubpath := normalizeAnalyticsSubpath(queryValue(request.Params.GitSubpath))
	limit, opErr := parseStorageChildrenLimit(queryInt(request.Params.Limit))
	if opErr != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", opErr.Error(), map[string]any{"project_id": target.ProjectID})
	}
	sortBy, sortOrder, cursor := queryValue(request.Params.SortBy), queryValue(request.Params.SortOrder), queryValue(request.Params.Cursor)
	summaryMode := strings.TrimSpace(queryValue(request.Params.SummaryMode))
	if summaryMode == "" {
		summaryMode = storageaudit.StorageFolderSummarySourceGitIndex
	}
	timings := &storageaudit.StorageFolderTimings{
		DebugPrefix: fmt.Sprintf(
			"project_id=%s ref=%s git_subpath=%q limit=%d sort_by=%q sort_order=%q cursor=%t summary_mode=%s",
			target.ProjectID, target.RefName, gitSubpath, limit, sortBy, sortOrder, cursor != "", summaryMode,
		),
		Logf: s.Logger.Info,
	}
	timings.Record("resolve_git_context", time.Since(resolveStart))
	forceRefresh := parseBoolQuery(queryValue(request.Params.ForceRefresh)) || parseBoolQuery(queryValue(request.Params.ForceAuditRefresh)) || parseBoolQuery(queryValue(request.Params.ForceBucketInventoryRefresh))
	buildStart := time.Now()
	// The legacy handler passes the raw summary mode to BuildFolder and only
	// applies the git-index default for its debug log line; keep both.
	response, err := s.Analytics.BuildFolder(ctx, target, gitSubpath, limit, sortBy, sortOrder, cursor, strings.TrimSpace(queryValue(request.Params.SummaryMode)), forceRefresh, timings)
	timings.Record("build_folder", time.Since(buildStart))
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, gitSubpath))
	}
	s.Logger.Info("storage_folder_request_complete %s", timings.DebugPrefix)
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.GetGitProjectStorageFolder200JSONResponse(m), nil
}

// PostGitProjectDiffAudit implements POST /git/projects/{orgTitle}/{projectTitle}/repair/project-diff/audit.
func (s *Server) PostGitProjectDiffAudit(ctx context.Context, request gecko.PostGitProjectDiffAuditRequestObject) (gecko.PostGitProjectDiffAuditResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	var body storageaudit.ProjectDiffAuditRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": target.ProjectID}); err != nil {
			return nil, err
		}
	}
	target.ApplyRef(body.Ref)
	gitSubpath := normalizeAnalyticsSubpath(body.GitSubpath)
	response, err := s.Analytics.BuildDiffAudit(ctx, target, body, gitSubpath)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, gitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectDiffAudit200JSONResponse(m), nil
}

// PostGitProjectStorageChainAudit implements POST /git/projects/{orgTitle}/{projectTitle}/repair/storage-chain/audit.
func (s *Server) PostGitProjectStorageChainAudit(ctx context.Context, request gecko.PostGitProjectStorageChainAuditRequestObject) (gecko.PostGitProjectStorageChainAuditResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	s.Logger.Info("storage_chain_audit_request_received org=%s project=%s", organization, project)
	timings := &storageaudit.StorageChainAuditTimings{
		Logf:        s.Logger.Info,
		DebugPrefix: fmt.Sprintf("org=%s project=%s", organization, project),
	}
	var body storageaudit.ChainAuditRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": project}); err != nil {
			return nil, err
		}
	}
	normalized, err := storageaudit.NormalizeStorageChainAuditRequest(body)
	if err != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", err.Error(), map[string]any{"project_id": project})
	}
	requestedRef := queryValue(request.Params.Ref)
	if bodyRef := strings.TrimSpace(body.Ref); bodyRef != "" {
		requestedRef = bodyRef
	}
	// Every audit resolves the selected ref against a current mirror before it
	// can build or serve a snapshot. A saved report keeps its own revision metadata.
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, requestedRef, true)
	if opErr != nil {
		return nil, opErr
	}
	gitSubpath := normalized.GitSubpath
	normalized.Options.Timings = timings
	response, err := s.Analytics.BuildChainAudit(ctx, target, gitSubpath, normalized.Options)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, gitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectStorageChainAudit200JSONResponse(m), nil
}

// PostGitProjectStorageChainRegisterGitOnly implements POST /git/projects/{orgTitle}/{projectTitle}/repair/storage-chain/register-git-only.
func (s *Server) PostGitProjectStorageChainRegisterGitOnly(ctx context.Context, request gecko.PostGitProjectStorageChainRegisterGitOnlyRequestObject) (gecko.PostGitProjectStorageChainRegisterGitOnlyResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectConfigAuthz(ctx, organization, project, "update"); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	var body storageaudit.GitOnlySyfonRegistrationRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": target.ProjectID}); err != nil {
			return nil, err
		}
	}
	target.ApplyRef(body.Ref)
	body.Ref = target.RefName
	storageaudit.NormalizeGitOnlySyfonRegistrationRequest(&body)
	response, err := s.Analytics.RegisterGitOnly(ctx, target, body)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, ""))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectStorageChainRegisterGitOnly200JSONResponse(m), nil
}

// PostGitProjectStorageCleanupAudit implements POST /git/projects/{orgTitle}/{projectTitle}/repair/storage-cleanup/audit.
func (s *Server) PostGitProjectStorageCleanupAudit(ctx context.Context, request gecko.PostGitProjectStorageCleanupAuditRequestObject) (gecko.PostGitProjectStorageCleanupAuditResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	var body storageaudit.CleanupAuditRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": target.ProjectID}); err != nil {
			return nil, err
		}
	}
	target.ApplyRef(body.Ref)
	storageaudit.NormalizeCleanupAuditRequest(&body)
	response, err := s.Analytics.BuildCleanupAudit(ctx, target, body)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, body.GitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectStorageCleanupAudit200JSONResponse(m), nil
}

// PostGitProjectStorageCleanupApply implements POST /git/projects/{orgTitle}/{projectTitle}/repair/storage-cleanup/apply.
func (s *Server) PostGitProjectStorageCleanupApply(ctx context.Context, request gecko.PostGitProjectStorageCleanupApplyRequestObject) (gecko.PostGitProjectStorageCleanupApplyResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	target, opErr := s.resolveAnalyticsContext(ctx, organization, project, queryValue(request.Params.Ref), false)
	if opErr != nil {
		return nil, opErr
	}
	var body storageaudit.CleanupApplyRequest
	if request.Body != nil {
		if err := decodeBody(*request.Body, &body, map[string]any{"project_id": target.ProjectID}); err != nil {
			return nil, err
		}
	}
	target.ApplyRef(body.Ref)
	storageaudit.NormalizeCleanupApplyRequest(&body)
	response, err := s.Analytics.ApplyCleanup(ctx, target, body)
	if err != nil {
		return nil, fromAppError(gitanalytics.WrapError(err, target, body.GitSubpath))
	}
	m, err := toMap(response)
	if err != nil {
		return nil, err
	}
	return gecko.PostGitProjectStorageCleanupApply200JSONResponse(m), nil
}

func parseStorageChildrenLimit(rawLimit string) (int, *opError) {
	if rawLimit == "" {
		return defaultStorageChildrenLimit, nil
	}
	parsed, err := strconv.Atoi(rawLimit)
	if err != nil || parsed <= 0 {
		return 0, newOpError(http.StatusBadRequest, "invalid_request", "limit must be a positive integer", nil)
	}
	if parsed > maxStorageChildrenLimit {
		return maxStorageChildrenLimit, nil
	}
	return parsed, nil
}

func parseBoolQuery(raw string) bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	return err == nil && parsed
}

func normalizeAnalyticsSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}
