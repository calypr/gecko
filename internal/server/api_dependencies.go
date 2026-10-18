package server

import (
	"os"
	"strings"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/gitanalytics"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/project"
	httpapi "github.com/calypr/gecko/internal/server/httpapi"
	configapi "github.com/calypr/gecko/internal/server/httpapi/config"
	gitapi "github.com/calypr/gecko/internal/server/httpapi/git"
	healthapi "github.com/calypr/gecko/internal/server/httpapi/health"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/upload"
)

func (server *Server) newAPIDependencies() httpapi.Dependencies {
	deps := httpapi.Dependencies{
		Config: configapi.Dependencies{
			DB:             server.db,
			Logger:         server.Logger,
			ThumbnailStore: server.thumbnailStore,
		},
		Git: gitapi.Dependencies{
			DB:                server.db,
			Logger:            server.Logger,
			RepoService:       server.repoService,
			ThumbnailStore:    server.thumbnailStore,
			PresentationStore: server.presentationStore,
		},
		Health: healthapi.Dependencies{DB: server.db, Logger: server.Logger},
	}
	if server.repoService == nil {
		return deps
	}

	storageManager := gintegrationsyfon.NewManager(strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL")), nil)
	deps.Git.ProjectSetup = project.NewSetupService(server.db, storageManager, access.NewFenceUserAccessHandler(nil))
	deps.Git.ProjectSync = project.NewReconcileService(server.db, storageManager, server.repoService)
	if server.db != nil {
		deps.Git.ProjectRead = project.NewReadService(server.db, server.repoService)
		deps.Git.ProjectInstallation = project.NewInstallationService(server.db, server.repoService)
		deps.Git.UploadService = upload.NewService(server.db, server.repoService)
		deps.Config.ConfigService = configuration.NewService(server.db, server.repoService)
		deps.Config.DeleteService = project.NewDeleteService(server.db, deps.Git.ProjectSetup, server.thumbnailStore)
	}
	if storageManager != nil {
		deps.Git.StorageAnalytics = storageaudit.NewStorageAnalyticsService(storageManager)
		deps.Git.StorageAnalytics.EnableStorageChainAuditResponseCacheFromEnv()
		deps.Git.Analytics = gitanalytics.NewService(deps.Git.ProjectRead, deps.Git.StorageAnalytics)
	}
	return deps
}
