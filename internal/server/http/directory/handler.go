package directory

import (
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	logger        arborist.Logger
	gripqlClient  *gripql.Client
	gripGraphName string
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	return &Handler{
		Handler:       sharedHandler,
		logger:        sharedHandler.Logger,
		gripqlClient:  sharedHandler.GripqlClient,
		gripGraphName: sharedHandler.GripGraphName,
	}
}
