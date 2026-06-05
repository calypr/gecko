package server

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/internal/git"
	geckologging "github.com/calypr/gecko/internal/logging"
	httpapi "github.com/calypr/gecko/internal/server/http"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type Server struct {
	db             *sqlx.DB
	jwtApp         arborist.JWTDecoder
	Logger         *geckologging.Handler
	stmts          *arborist.CachedStmts
	qdrantClient   *qdrant.Client
	gripqlClient   *gripql.Client
	gripGraphName  string
	gitService     *git.GitService
	thumbnailStore thumbnail.Manager
}

func NewServer() *Server { return &Server{} }

func (server *Server) WithLogger(logger *log.Logger) *Server {
	server.Logger = &geckologging.Handler{Logger: logger}
	return server
}

func (server *Server) WithJWTApp(jwtApp arborist.JWTDecoder) *Server {
	server.jwtApp = jwtApp
	return server
}

func (server *Server) WithDB(db *sqlx.DB) *Server {
	server.db = db
	server.stmts = arborist.NewCachedStmts(db)
	return server
}

func (server *Server) WithQdrantClient(client *qdrant.Client) *Server {
	server.qdrantClient = client
	return server
}

func (server *Server) WithGripqlClient(client *gripql.Client, gripGraphName string) *Server {
	server.gripqlClient = client
	server.gripGraphName = gripGraphName
	return server
}

func (server *Server) WithGitService(service *git.GitService) *Server {
	server.gitService = service
	return server
}

func (server *Server) WithThumbnailStore(store thumbnail.Manager) *Server {
	server.thumbnailStore = store
	return server
}

func (server *Server) Init() (*Server, error) {
	if server.jwtApp == nil {
		return nil, errors.New("gecko server initialized without JWT app")
	}
	if server.Logger == nil {
		return nil, errors.New("gecko server initialized without logger")
	}
	if server.db == nil {
		server.Logger.Warning("Database endpoints will be disabled.")
	}
	if server.qdrantClient == nil {
		server.Logger.Warning("Qdrant endpoints will be disabled.")
	}
	if server.gripqlClient == nil || server.gripGraphName == "" {
		server.Logger.Warning("Grip endpoints will be disabled.")
	}
	if server.gitService != nil {
		if err := server.gitService.Init(server.db); err != nil {
			return nil, err
		}
	} else {
		server.Logger.Warning("Git endpoints will be disabled.")
	}
	server.Logger.Info("Gecko server initialized successfully.")
	return server, nil
}

func (server *Server) MakeRouter() *fiber.App {
	app := fiber.New(fiber.Config{
		ReadBufferSize: 32 * 1024,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
	})

	app.Use(func(ctx fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				server.Logger.Error("panic recovered: %v", r)
				ctx.Status(http.StatusInternalServerError)
				_ = ctx.SendString("Internal Server Error")
			}
		}()
		return ctx.Next()
	})
	app.Use(servermw.RequestLogger(server.Logger))

	httpapi.Register(app, httpapi.Dependencies{
		DB:             server.db,
		Logger:         server.Logger,
		JWTApp:         server.jwtApp,
		QdrantClient:   server.qdrantClient,
		GripqlClient:   server.gripqlClient,
		GripGraphName:  server.gripGraphName,
		GitService:     server.gitService,
		ThumbnailStore: server.thumbnailStore,
	})
	return app
}
