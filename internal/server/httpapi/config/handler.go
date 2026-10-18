package config

import (
	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB             *sqlx.DB
	Logger         arborist.Logger
	ThumbnailStore thumbnail.Manager
	ConfigService  *configuration.Service
	DeleteService  *project.DeleteService
}

type Handler struct {
	Dependencies
}

func NewHandler(deps Dependencies) *Handler {
	return &Handler{Dependencies: deps}
}
