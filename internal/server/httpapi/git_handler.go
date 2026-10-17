package httpapi

import (
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/calypr/gecko/internal/upload"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type GitHandler struct {
	*Handler
	db                  *sqlx.DB
	logger              arborist.Logger
	repoService         *repo.Service
	projectSetup        *project.SetupService
	projectSync         *project.ReconcileService
	projectInstallation *project.InstallationService
	storageAnalytics    *storageaudit.StorageAnalyticsService
	uploadService       *upload.Service
	thumbnailStore      thumbnail.Manager
	presentationStore   presentation.Manager
}

func (handler *GitHandler) upload() *upload.Service {
	if handler.uploadService == nil && handler.db != nil && handler.repoService != nil {
		handler.uploadService = upload.NewService(handler.db, handler.repoService)
	}
	return handler.uploadService
}

func (handler *GitHandler) installation() *project.InstallationService {
	if handler.projectInstallation == nil && handler.db != nil && handler.repoService != nil {
		handler.projectInstallation = project.NewInstallationService(handler.db, handler.repoService)
	}
	return handler.projectInstallation
}

func newGitHandler(sharedHandler *Handler) *GitHandler {
	var storageAnalytics *storageaudit.StorageAnalyticsService
	if sharedHandler.RepoService != nil && sharedHandler.SyfonManager != nil {
		storageAnalytics = storageaudit.NewStorageAnalyticsService(sharedHandler.SyfonManager)
		storageAnalytics.EnableStorageChainAuditResponseCacheFromEnv()
	}
	handler := &GitHandler{
		Handler:           sharedHandler,
		db:                sharedHandler.DB,
		logger:            sharedHandler.Logger,
		repoService:       sharedHandler.RepoService,
		projectSetup:      sharedHandler.ProjectSetup,
		projectSync:       sharedHandler.ProjectSync,
		storageAnalytics:  storageAnalytics,
		thumbnailStore:    sharedHandler.ThumbnailStore,
		presentationStore: sharedHandler.PresentationStore,
	}
	if sharedHandler.DB != nil && sharedHandler.RepoService != nil {
		handler.projectInstallation = project.NewInstallationService(sharedHandler.DB, sharedHandler.RepoService)
	}
	return handler
}
