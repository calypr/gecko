package httpapi

import (
	"os"
	"strings"

	"github.com/calypr/gecko/internal/access"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB                *sqlx.DB
	Logger            arborist.Logger
	RepoService       *repo.Service
	ThumbnailStore    thumbnail.Manager
	PresentationStore presentation.Manager
}

type Handler struct {
	DB                *sqlx.DB
	Logger            arborist.Logger
	RepoService       *repo.Service
	ProjectSetup      *project.SetupService
	ProjectSync       *project.ReconcileService
	SyfonManager      *gintegrationsyfon.Manager
	ThumbnailStore    thumbnail.Manager
	PresentationStore presentation.Manager
}

func NewHandler(deps Dependencies) *Handler {
	var projectSetup *project.SetupService
	var projectSync *project.ReconcileService
	var storageManager *gintegrationsyfon.Manager
	if deps.RepoService != nil {
		storageManager = gintegrationsyfon.NewManager(strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL")), nil)
		projectSetup = project.NewSetupService(deps.DB, storageManager, access.NewFenceUserAccessHandler(nil))
		projectSync = project.NewReconcileService(
			deps.DB,
			storageManager,
			deps.RepoService,
		)
	}
	return &Handler{
		DB:                deps.DB,
		Logger:            deps.Logger,
		RepoService:       deps.RepoService,
		ProjectSetup:      projectSetup,
		ProjectSync:       projectSync,
		SyfonManager:      storageManager,
		ThumbnailStore:    deps.ThumbnailStore,
		PresentationStore: deps.PresentationStore,
	}
}
