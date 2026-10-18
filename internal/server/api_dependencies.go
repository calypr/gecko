package server

import (
	"os"
	"strings"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/gitanalytics"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/server/httpapi/apiserver"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/upload"
)

// newAPIServer builds the strict OpenAPI server implementation from the
// application services, so the generated router enforces the same behavior and
// authorization the HTTP surface always had.
func (server *Server) newAPIServer() *apiserver.Server {
	s := &apiserver.Server{
		Logger:            server.Logger,
		DB:                server.db,
		RepoService:       server.repoService,
		ThumbnailStore:    server.thumbnailStore,
		PresentationStore: server.presentationStore,
		Authz:             access.NewFenceUserAccessHandler(nil),
	}
	if server.repoService == nil {
		return s
	}

	storageManager := gintegrationsyfon.NewManager(strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL")), nil)
	s.ProjectSetup = project.NewSetupService(server.db, storageManager, access.NewFenceUserAccessHandler(nil))
	s.ProjectSync = project.NewReconcileService(server.db, storageManager, server.repoService)
	if server.db != nil {
		s.ProjectRead = project.NewReadService(server.db, server.repoService)
		s.ProjectInstallation = project.NewInstallationService(server.db, server.repoService)
		s.UploadService = upload.NewService(server.db, server.repoService)
		s.ConfigService = configuration.NewService(server.db, server.repoService)
		s.DeleteService = project.NewDeleteService(server.db, s.ProjectSetup, server.thumbnailStore)
	}
	if storageManager != nil {
		s.StorageAnalytics = storageaudit.NewStorageAnalyticsService(storageManager)
		s.StorageAnalytics.EnableStorageChainAuditResponseCacheFromEnv()
		s.Analytics = gitanalytics.NewService(s.ProjectRead, s.StorageAnalytics)
	}
	return s
}
