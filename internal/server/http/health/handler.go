package health

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	db     *sqlx.DB
	logger arborist.Logger
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	return &Handler{
		Handler: sharedHandler,
		db:      sharedHandler.DB,
		logger:  sharedHandler.Logger,
	}
}
