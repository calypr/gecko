package vector

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	logger       arborist.Logger
	qdrantClient *qdrant.Client
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	return &Handler{
		Handler:      sharedHandler,
		logger:       sharedHandler.Logger,
		qdrantClient: sharedHandler.QdrantClient,
	}
}
