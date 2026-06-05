package git

import (
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	gitappreconcile "github.com/calypr/gecko/internal/git/app/reconcile"
	gitappsetup "github.com/calypr/gecko/internal/git/app/setup"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	db            *sqlx.DB
	logger        arborist.Logger
	jwtApp        arborist.JWTDecoder
	qdrantClient  *qdrant.Client
	gripqlClient  *gripql.Client
	gripGraphName string
	gitService    *git.GitService
	projectSetup  *gitappsetup.Service
	projectSync   *gitappreconcile.Service
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	return &Handler{
		Handler:       sharedHandler,
		db:            sharedHandler.DB,
		logger:        sharedHandler.Logger,
		jwtApp:        sharedHandler.JWTApp,
		qdrantClient:  sharedHandler.QdrantClient,
		gripqlClient:  sharedHandler.GripqlClient,
		gripGraphName: sharedHandler.GripGraphName,
		gitService:    sharedHandler.GitService,
		projectSetup:  sharedHandler.ProjectSetup,
		projectSync:   sharedHandler.ProjectSync,
	}
}
