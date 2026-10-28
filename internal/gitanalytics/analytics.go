package gitanalytics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/domain"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

type TimingRecorder interface {
	StageStart(stage string)
	Record(stage string, duration time.Duration)
}

type Target struct {
	Organization        string
	Project             string
	ProjectID           string
	AuthorizationHeader string
	DefaultBranch       string
	RefName             string
	MirrorPath          string
	Repository          *gogit.Repository
	Hash                plumbing.Hash
}

func (target *Target) ApplyRef(ref string) {
	if target == nil {
		return
	}
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == target.RefName {
		return
	}
	refName, hash, err := repo.ResolveGitReference(target.Repository, ref, target.DefaultBranch)
	if err != nil {
		return
	}
	target.RefName = refName
	target.Hash = hash
}

type Service struct {
	reader    *projectpkg.ReadService
	analytics *storageaudit.StorageAnalyticsService
}

func NewService(reader *projectpkg.ReadService, analytics *storageaudit.StorageAnalyticsService) *Service {
	return &Service{reader: reader, analytics: analytics}
}

func (service *Service) Resolve(ctx context.Context, organization string, project string, authorizationHeader string, requestedRef string, requireCurrentMirror bool, timings TimingRecorder) (*Target, error) {
	resolved, err := service.reader.Resolve(ctx, organization, project)
	if err != nil {
		return nil, err
	}
	authorizationHeader = strings.TrimSpace(authorizationHeader)
	if authorizationHeader != "" && timings != nil {
		timings.StageStart("mirror_warmup")
	}
	mirrorStart := time.Now()
	mirror, err := service.reader.OpenMirror(ctx, authorizationHeader, resolved, requireCurrentMirror)
	if authorizationHeader != "" && timings != nil {
		timings.Record("mirror_warmup", time.Since(mirrorStart))
	}
	if err != nil {
		var appErr *domain.Error
		if requireCurrentMirror && !errors.As(err, &appErr) {
			return nil, domain.NewTypedError(
				domain.ErrorKindIntegration,
				apierror.Type("integration_error"),
				http.StatusBadGateway,
				fmt.Sprintf("storage audit requires a current Git mirror: %s", err),
				map[string]any{"project_id": resolved.ID},
			)
		}
		return nil, err
	}
	if mirror.Empty {
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.Type("conflict"),
			http.StatusConflict,
			fmt.Sprintf("project %s has no Git content yet", resolved.ID),
			map[string]any{"project_id": resolved.ID},
		)
	}
	refName, hash, err := repo.ResolveGitReference(mirror.Repository, strings.TrimSpace(requestedRef), mirror.DefaultBranch)
	if err != nil {
		return nil, domain.NewTypedError(
			domain.ErrorKindNotFound,
			apierror.TypeNotFound,
			http.StatusNotFound,
			fmt.Sprintf("failed to resolve git ref: %s", err),
			map[string]any{"project_id": resolved.ID, "ref": requestedRef},
		)
	}
	return &Target{
		Organization:        resolved.Organization,
		Project:             resolved.Name,
		ProjectID:           resolved.ID,
		AuthorizationHeader: authorizationHeader,
		DefaultBranch:       mirror.DefaultBranch,
		RefName:             refName,
		MirrorPath:          mirror.State.MirrorPath,
		Repository:          mirror.Repository,
		Hash:                hash,
	}, nil
}

func (service *Service) BuildSummary(ctx context.Context, target *Target, gitSubpath string) (*storageaudit.StorageSummaryResponse, error) {
	return service.analytics.BuildStorageSummary(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, gitSubpath, target.MirrorPath, target.Repository, target.Hash)
}

func (service *Service) BuildChildren(ctx context.Context, target *Target, gitSubpath string, limit int, sortBy string, sortOrder string, cursor string) (*storageaudit.StorageChildrenResponse, error) {
	return service.analytics.BuildStorageChildren(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, gitSubpath, target.MirrorPath, target.Repository, target.Hash, limit, sortBy, sortOrder, cursor)
}

func (service *Service) BuildFolder(ctx context.Context, target *Target, gitSubpath string, limit int, sortBy string, sortOrder string, cursor string, summaryMode string, forceRefresh bool, timings *storageaudit.StorageFolderTimings) (*storageaudit.StorageFolderResponse, error) {
	return service.analytics.BuildStorageFolder(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, gitSubpath, target.MirrorPath, target.Repository, target.Hash, limit, sortBy, sortOrder, cursor, summaryMode, forceRefresh, timings)
}

func (service *Service) BuildDiffAudit(ctx context.Context, target *Target, request storageaudit.ProjectDiffAuditRequest, gitSubpath string) (*storageaudit.ProjectDiffAuditResponse, error) {
	return service.analytics.BuildProjectDiffAudit(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, gitSubpath, target.MirrorPath, target.Repository, target.Hash)
}

func (service *Service) BuildCleanupAudit(ctx context.Context, target *Target, request storageaudit.CleanupAuditRequest) (*storageaudit.CleanupAuditResponse, error) {
	response, _, err := service.analytics.BuildStorageCleanupAudit(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, request.GitSubpath, request.SelectedRepoPaths, target.MirrorPath, target.Repository, target.Hash, request.CheckStorage)
	return response, err
}

func (service *Service) BuildChainAudit(ctx context.Context, target *Target, gitSubpath string, options storageaudit.StorageChainAuditOptions) (*storageaudit.ChainAuditResponse, error) {
	return service.analytics.BuildStorageChainAuditWithOptions(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, gitSubpath, target.MirrorPath, target.Repository, target.Hash, options)
}

func (service *Service) RegisterGitOnly(ctx context.Context, target *Target, request storageaudit.GitOnlySyfonRegistrationRequest) (*storageaudit.GitOnlySyfonRegistrationResponse, error) {
	return service.analytics.RegisterGitOnlySyfonRecords(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.RefName, target.MirrorPath, target.Repository, target.Hash, request)
}

func (service *Service) ApplyCleanup(ctx context.Context, target *Target, request storageaudit.CleanupApplyRequest) (*storageaudit.CleanupApplyResponse, error) {
	return service.analytics.ApplyStorageCleanupWithRevisions(ctx, target.AuthorizationHeader, target.Organization, target.Project, target.Hash.String(), request)
}

func WrapError(err error, target *Target, gitSubpath string) error {
	if err == nil {
		return nil
	}
	var appErr *domain.Error
	if errors.As(err, &appErr) {
		return err
	}
	statusCode := http.StatusBadGateway
	errorType := apierror.Type("integration_error")
	errorKind := domain.ErrorKindIntegration
	errorMessage := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errorMessage, "cleanup audit is stale"):
		statusCode = http.StatusConflict
		errorType = apierror.Type("conflict")
		errorKind = domain.ErrorKindValidation
	case strings.Contains(errorMessage, "storage children cursor"):
		statusCode = http.StatusBadRequest
		errorType = apierror.Type("invalid_request")
		errorKind = domain.ErrorKindValidation
	case strings.Contains(errorMessage, "cleanup apply"),
		strings.Contains(errorMessage, "cleanup finding"),
		strings.Contains(errorMessage, "cleanup action"),
		strings.Contains(errorMessage, "unsupported cleanup"),
		strings.Contains(errorMessage, "selected cleanup paths"),
		strings.Contains(errorMessage, "git-only syfon registration"):
		statusCode = http.StatusBadRequest
		errorType = apierror.Type("invalid_request")
		errorKind = domain.ErrorKindValidation
	case strings.Contains(errorMessage, "git tree path"):
		statusCode = http.StatusNotFound
		errorType = apierror.TypeNotFound
		errorKind = domain.ErrorKindNotFound
	}
	details := map[string]any{"git_subpath": gitSubpath}
	if target != nil {
		details["project_id"] = target.ProjectID
		details["ref"] = target.RefName
	}
	return domain.NewTypedError(errorKind, errorType, statusCode, err.Error(), details)
}
