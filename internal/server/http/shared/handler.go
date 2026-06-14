package shared

import (
	"net/http"
	"os"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/presentation"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB                *sqlx.DB
	Logger            arborist.Logger
	JWTApp            arborist.JWTDecoder
	QdrantClient      *qdrant.Client
	GripqlClient      *gripql.Client
	GripGraphName     string
	GitService        *git.GitService
	ThumbnailStore    thumbnail.Manager
	PresentationStore presentation.Manager
}

type Handler struct {
	DB                *sqlx.DB
	Logger            arborist.Logger
	JWTApp            arborist.JWTDecoder
	QdrantClient      *qdrant.Client
	GripqlClient      *gripql.Client
	GripGraphName     string
	GitService        *git.GitService
	ProjectSetup      *git.SetupService
	ProjectSync       *git.ReconcileService
	ThumbnailStore    thumbnail.Manager
	PresentationStore presentation.Manager
}

func NewHandler(deps Dependencies) *Handler {
	var projectSetup *git.SetupService
	var projectSync *git.ReconcileService
	if deps.GitService != nil {
		storageManager := gintegrationsyfon.NewManager(strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL")), http.DefaultClient)
		projectSetup = git.NewSetupService(deps.DB, deps.GitService, storageManager, servermw.NewFenceUserAccessHandler(nil))
		projectSync = git.NewReconcileService(
			deps.DB,
			storageManager,
			deps.GitService,
		)
	}
	return &Handler{
		DB:                deps.DB,
		Logger:            deps.Logger,
		JWTApp:            deps.JWTApp,
		QdrantClient:      deps.QdrantClient,
		GripqlClient:      deps.GripqlClient,
		GripGraphName:     deps.GripGraphName,
		GitService:        deps.GitService,
		ProjectSetup:      projectSetup,
		ProjectSync:       projectSync,
		ThumbnailStore:    deps.ThumbnailStore,
		PresentationStore: deps.PresentationStore,
	}
}
