package server

import (
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/apierror"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type LogHandler struct {
	Logger *log.Logger
}

type Server struct {
	db            *sqlx.DB
	jwtApp        arborist.JWTDecoder
	Logger        *LogHandler
	stmts         *arborist.CachedStmts
	qdrantClient  *qdrant.Client
	gripqlClient  *gripql.Client
	gripGraphName string
}

func NewServer() *Server { return &Server{} }

func (server *Server) WithLogger(logger *log.Logger) *Server {
	server.Logger = &LogHandler{Logger: logger}
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
	server.Logger.Info("Gecko server initialized successfully.")
	return server, nil
}

func withConfigType(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
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
	app.Use(server.logRequestMiddleware)

	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		return ctx.SendFile("./docs/swagger.json")
	})
	app.Get("/health", server.handleHealth)

	if server.gripqlClient != nil {
		app.Get("/dir", server.handleListProjects)
		app.Get("/dir/:projectId", server.GeneralAuthMware(&middleware.ProdJWTHandler{}, "read", "*"), server.handleDirGet)
	} else {
		server.Logger.Warning("Skipping gripql Directory endpoints — no database configured")
	}

	if server.db != nil {
		configGroup := app.Group("/config")
		configGroup.Get("/types", server.handleConfigTypesGET)
		configGroup.Get("/list", server.handleConfigListGET)

		explorer := configGroup.Group("/explorer", withConfigType("explorer"))
		explorer.Get("/list", server.handleConfigListGET)
		explorer.Get("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		explorer.Put("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		explorer.Delete("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)

		appsPage := configGroup.Group("/apps_page", withConfigType("apps_page"))
		appsPage.Get("/list", server.handleConfigListGET)
		appsPage.Get("/", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		appsPage.Get("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		appsPage.Put("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		appsPage.Delete("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)
		appCard := appsPage.Group("/appcard")
		appCard.Get("/:projectId", server.AppCardAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleAppCardGET)
		appCard.Post("/:projectId", server.AppCardAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleAppCardPOST)
		appCard.Delete("/:projectId", server.AppCardAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleAppCardDELETE)

		nav := configGroup.Group("/nav", withConfigType("nav"))
		nav.Get("/list", server.handleConfigListGET)
		nav.Get("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		nav.Put("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		nav.Delete("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)

		fileSummary := configGroup.Group("/file_summary", withConfigType("file_summary"))
		fileSummary.Get("/list", server.handleConfigListGET)
		fileSummary.Get("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		fileSummary.Put("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		fileSummary.Delete("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)

		project := configGroup.Group("/project", withConfigType("project"))
		project.Get("/list", server.handleConfigListGET)
		project.Get("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		project.Put("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		project.Delete("/:configId", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)

		projects := configGroup.Group("/projects", withConfigType("projects"))
		projects.Get("", server.handleConfigListGET)
		projects.Get("/list", server.handleConfigListGET)
		projects.Get("/:orgTitle/:projectTitle", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleProjectConfigGET)
		projects.Put("/:orgTitle/:projectTitle", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleProjectConfigPUT)
		projects.Delete("/:orgTitle/:projectTitle", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleProjectConfigDELETE)
	} else {
		server.Logger.Warning("Skipping DB endpoints — no database configured")
	}

	if server.qdrantClient != nil {
		vector := app.Group("/vector")
		vector.Get("/swagger/doc.json", func(ctx fiber.Ctx) error { return ctx.SendFile("./docs/swagger.json") })
		vector.Get("/swagger", func(ctx fiber.Ctx) error {
			return ctx.Redirect().Status(fiber.StatusTemporaryRedirect).To("/vector/swagger/doc.json")
		})
		vector.Get("/swagger/*", func(ctx fiber.Ctx) error {
			return ctx.Redirect().Status(fiber.StatusTemporaryRedirect).To("/vector/swagger/doc.json")
		})

		collections := vector.Group("/collections")
		collections.Get("", server.handleListCollections)
		collections.Put("/:collection", server.handleCreateCollection)
		collections.Get("/:collection", server.handleGetCollection)
		collections.Patch("/:collection", server.handleUpdateCollection)
		collections.Delete("/:collection", server.handleDeleteCollection)

		points := collections.Group("/:collection/points")
		points.Put("", server.handleUpsertPoints)
		points.Get("/:id", server.handleGetPoint)
		points.Post("/search", server.handleQueryPoints)
		points.Post("/delete", server.handleDeletePoints)
	} else {
		server.Logger.Warning("Skipping Qdrant endpoints — no vector store configured")
	}

	if server.gripqlClient == nil || server.gripGraphName == "" {
		server.Logger.Warning("Skipping Grip endpoints — no graph configured")
	}

	app.Use(func(ctx fiber.Ctx) error {
		ctx.Path(strings.TrimSuffix(ctx.Path(), "/"))
		return handleNotFound(ctx)
	})

	return app
}

func (server *Server) handleHealth(ctx fiber.Ctx) error {
	if server.db != nil {
		if err := server.db.Ping(); err != nil {
			server.Logger.Error("Database ping failed: %v", err)
			return newTypedErrorResponse(apierror.TypeDatabaseUnavailable, "database unavailable", http.StatusInternalServerError, nil, nil).write(ctx)
		}
	} else {
		server.Logger.Warning("Health check: Database connection not configured.")
	}
	return jsonResponseFrom("Healthy", http.StatusOK).write(ctx)
}
