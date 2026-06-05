package shared

import (
	"net/http"
	"os"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	gitappreconcile "github.com/calypr/gecko/internal/git/app/reconcile"
	gitappsetup "github.com/calypr/gecko/internal/git/app/setup"
	gitintegrationfence "github.com/calypr/gecko/internal/git/integrations/fence"
	gitintegrationgithub "github.com/calypr/gecko/internal/git/integrations/github"
	gintegrationsyfon "github.com/calypr/gecko/internal/git/integrations/syfon"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB            *sqlx.DB
	Logger        arborist.Logger
	JWTApp        arborist.JWTDecoder
	QdrantClient  *qdrant.Client
	GripqlClient  *gripql.Client
	GripGraphName string
	GitService    *git.GitService
}

type Handler struct {
	DB            *sqlx.DB
	Logger        arborist.Logger
	JWTApp        arborist.JWTDecoder
	QdrantClient  *qdrant.Client
	GripqlClient  *gripql.Client
	GripGraphName string
	GitService    *git.GitService
	ProjectSetup  *gitappsetup.Service
	ProjectSync   *gitappreconcile.Service
}

func NewHandler(deps Dependencies) *Handler {
	var projectSetup *gitappsetup.Service
	var projectSync *gitappreconcile.Service
	if deps.GitService != nil {
		storageManager := gintegrationsyfon.NewManager(strings.TrimSpace(os.Getenv("SYFON_DATA_API_BASE_URL")), http.DefaultClient)
		projectSetup = gitappsetup.NewService(deps.DB, deps.GitService, storageManager)
		projectSync = gitappreconcile.NewService(
			deps.DB,
			storageManager,
			gitintegrationfence.NewBroker(deps.GitService),
			gitintegrationgithub.NewInspector(deps.GitService),
		)
	}
	return &Handler{
		DB:            deps.DB,
		Logger:        deps.Logger,
		JWTApp:        deps.JWTApp,
		QdrantClient:  deps.QdrantClient,
		GripqlClient:  deps.GripqlClient,
		GripGraphName: deps.GripGraphName,
		GitService:    deps.GitService,
		ProjectSetup:  projectSetup,
		ProjectSync:   projectSync,
	}
}
