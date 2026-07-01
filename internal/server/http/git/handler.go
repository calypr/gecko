package git

import (
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Handler struct {
	*shared.Handler
	db                *sqlx.DB
	logger            arborist.Logger
	jwtApp            arborist.JWTDecoder
	qdrantClient      *qdrant.Client
	gripqlClient      *gripql.Client
	gripGraphName     string
	gitService        *git.GitService
	projectSetup      *git.SetupService
	projectSync       *git.ReconcileService
	storageAnalytics  *git.StorageAnalyticsService
	thumbnailStore    thumbnail.Manager
	presentationStore presentation.Manager
}

func NewHandler(sharedHandler *shared.Handler) *Handler {
	var storageAnalytics *git.StorageAnalyticsService
	if sharedHandler.GitService != nil && sharedHandler.SyfonManager != nil {
		storageAnalytics = git.NewStorageAnalyticsService(sharedHandler.SyfonManager)
	}
	return &Handler{
		Handler:           sharedHandler,
		db:                sharedHandler.DB,
		logger:            sharedHandler.Logger,
		jwtApp:            sharedHandler.JWTApp,
		qdrantClient:      sharedHandler.QdrantClient,
		gripqlClient:      sharedHandler.GripqlClient,
		gripGraphName:     sharedHandler.GripGraphName,
		gitService:        sharedHandler.GitService,
		projectSetup:      sharedHandler.ProjectSetup,
		projectSync:       sharedHandler.ProjectSync,
		storageAnalytics:  storageAnalytics,
		thumbnailStore:    sharedHandler.ThumbnailStore,
		presentationStore: sharedHandler.PresentationStore,
	}
}
