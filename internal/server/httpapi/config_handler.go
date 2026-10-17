package httpapi

import (
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type ConfigHandler struct {
	*Handler
	db             *sqlx.DB
	logger         arborist.Logger
	repoService    *repo.Service
	projectSetup   *project.SetupService
	thumbnailStore thumbnail.Manager
}

func newConfigHandler(sharedHandler *Handler) *ConfigHandler {
	return &ConfigHandler{
		Handler:        sharedHandler,
		db:             sharedHandler.DB,
		logger:         sharedHandler.Logger,
		repoService:    sharedHandler.RepoService,
		projectSetup:   sharedHandler.ProjectSetup,
		thumbnailStore: sharedHandler.ThumbnailStore,
	}
}
