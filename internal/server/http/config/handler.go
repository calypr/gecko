package config

import (
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	db             *sqlx.DB
	logger         arborist.Logger
	gitService     *git.GitService
	projectSetup   *git.SetupService
	thumbnailStore thumbnail.Manager
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	return &Handler{
		Handler:        sharedHandler,
		db:             sharedHandler.DB,
		logger:         sharedHandler.Logger,
		gitService:     sharedHandler.GitService,
		projectSetup:   sharedHandler.ProjectSetup,
		thumbnailStore: sharedHandler.ThumbnailStore,
	}
}
