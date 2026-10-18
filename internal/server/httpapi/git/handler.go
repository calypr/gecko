package git

import (
	"github.com/calypr/gecko/internal/gitanalytics"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/calypr/gecko/internal/upload"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB                  *sqlx.DB
	Logger              arborist.Logger
	RepoService         *repo.Service
	ProjectSetup        *project.SetupService
	ProjectSync         *project.ReconcileService
	ThumbnailStore      thumbnail.Manager
	PresentationStore   presentation.Manager
	ProjectRead         *project.ReadService
	ProjectInstallation *project.InstallationService
	UploadService       *upload.Service
	StorageAnalytics    *storageaudit.StorageAnalyticsService
	Analytics           *gitanalytics.Service
}

type Handler struct {
	Dependencies
}

func NewHandler(deps Dependencies) *Handler {
	return &Handler{Dependencies: deps}
}
