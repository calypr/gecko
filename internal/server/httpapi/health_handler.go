package httpapi

import (
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type HealthHandler struct {
	*Handler
	db     *sqlx.DB
	logger arborist.Logger
}

func newHealthHandler(sharedHandler *Handler) *HealthHandler {
	return &HealthHandler{
		Handler: sharedHandler,
		db:      sharedHandler.DB,
		logger:  sharedHandler.Logger,
	}
}
