package api

import (
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB            *sqlx.DB
	Logger        arborist.Logger
	QdrantClient  *qdrant.Client
	GripqlClient  *gripql.Client
	GripGraphName string
	GitService    *git.GitService
}

type Handler struct {
	db            *sqlx.DB
	logger        arborist.Logger
	qdrantClient  *qdrant.Client
	gripqlClient  *gripql.Client
	gripGraphName string
	gitService    *git.GitService
}

func Register(app *fiber.App, deps Dependencies) {
	handler := &Handler{
		db:            deps.DB,
		logger:        deps.Logger,
		qdrantClient:  deps.QdrantClient,
		gripqlClient:  deps.GripqlClient,
		gripGraphName: deps.GripGraphName,
		gitService:    deps.GitService,
	}
	handler.registerRoutes(app)
}
