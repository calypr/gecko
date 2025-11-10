package gecko

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/bmeg/grip/gripql"
	"github.com/iris-contrib/swagger"
	"github.com/iris-contrib/swagger/swaggerFiles"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/iris/v12"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type LogHandler struct {
	Logger *log.Logger
}

type Server struct {
	iris          *iris.Application
	db            *sqlx.DB
	jwtApp        arborist.JWTDecoder
	Logger        *LogHandler
	stmts         *arborist.CachedStmts
	qdrantClient  *qdrant.Client
	gripqlClient  *gripql.Client
	gripGraphName string
}

func NewServer() *Server {
	return &Server{}
}

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

func (server *Server) MakeRouter() *iris.Application {
	router := iris.New()
	router.Get("/swagger/doc.json", func(ctx iris.Context) {
		ctx.ServeFile("./docs/swagger.json")
	})
	router.Use(recoveryMiddleware)
	router.Use(server.logRequestMiddleware)
	router.OnErrorCode(iris.StatusNotFound, handleNotFound)
	router.Get("/health", server.handleHealth)

	if server.gripqlClient != nil {
		router.Get("/dir", server.handleListProjects)
		router.Get("/dir/{projectId}", server.GeneralAuthMware(&middleware.ProdJWTHandler{}, "read", "*"), server.handleDirGet)
	} else {
		server.Logger.Warning("Skipping gripql Directory endpoints — no database configured")
	}

	// project id must be in the form [program-project] if not permissions checking will not work and you won't be able to view the project
	if server.db != nil {
		router.Get("/config/list", server.handleConfigListGET)
		router.Get("/config/{configType}/{configId}", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigGET)
		router.Put("/config/{configType}/{configId}", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigPUT)
		router.Delete("/config/{configType}/{configId}", server.ConfigAuthMiddleware(&middleware.ProdJWTHandler{}), server.handleConfigDELETE)
	} else {
		server.Logger.Warning("Skipping DB endpoints — no database configured")
	}

	if server.qdrantClient != nil {
		vectorRouter := router.Party("/vector")
		{
			swaggerUI := swagger.Handler(swaggerFiles.Handler,
				swagger.URL("/vector/swagger/doc.json"),
				swagger.DeepLinking(true),
				swagger.Prefix("/vector/swagger"),
			)

			vectorRouter.Get("/swagger/doc.json", func(ctx iris.Context) {
				ctx.ServeFile("./docs/swagger.json")
			})
			vectorRouter.Get("/swagger", swaggerUI)
			vectorRouter.Get("/swagger/{any:path}", swaggerUI)

			collections := vectorRouter.Party("/collections")
			{
				collections.Get("", server.handleListCollections)
				collections.Put("/{collection}", server.handleCreateCollection)
				collections.Get("/{collection}", server.handleGetCollection)
				collections.Patch("/{collection}", server.handleUpdateCollection)
				collections.Delete("/{collection}", server.handleDeleteCollection)

				points := collections.Party("/{collection}/points")
				{
					points.Put("", server.handleUpsertPoints)
					points.Get("/{id}", server.handleGetPoint)
					points.Post("/search", server.handleQueryPoints)
					points.Post("/delete", server.handleDeletePoints)
				}
			}
		}
	} else {
		server.Logger.Warning("Skipping Qdrant endpoints — no vector store configured")
	}

	if server.gripqlClient != nil && server.gripGraphName != "" {
		// register your Grip routes here
	} else {
		server.Logger.Warning("Skipping Grip endpoints — no graph configured")
	}

	// Final trim/slash middleware and build
	router.UseRouter(func(ctx iris.Context) {
		req := ctx.Request()
		if req == nil || req.URL == nil {
			server.Logger.Warning("Request or URL is nil")
			ctx.StatusCode(http.StatusInternalServerError)
			ctx.WriteString("Internal Server Error")
			return
		}
		req.URL.Path = strings.TrimSuffix(req.URL.Path, "/")
		ctx.Next()
	})

	if err := router.Build(); err != nil {
		server.Logger.Error("Failed to build Iris router: %v", err)
	}

	return router
}

func recoveryMiddleware(ctx iris.Context) {
	defer func() {
		if r := recover(); r != nil {
			ctx.Application().Logger().Errorf("panic recovered: %v", r)
			ctx.StatusCode(iris.StatusInternalServerError)
			ctx.WriteString("Internal Server Error")
		}
	}()
	ctx.Next()
}

// handleHealth godoc
// @Summary Health check endpoint
// @Description Checks the database connection and returns the server status
// @Tags Health
// @Produce json
// @Success 200 {string} string "Healthy"
// @Failure 500 {object} ErrorResponse "Database unavailable"
// @Router /health [get]
func (server *Server) handleHealth(ctx iris.Context) {
	err := server.db.Ping()
	if err != nil {
		server.Logger.Error("Database ping failed: %v", err)
		response := newErrorResponse("database unavailable", 500, nil)
		_ = response.write(ctx)
		return
	}
	server.Logger.Info("Health check passed")
	_ = jsonResponseFrom("Healthy", http.StatusOK).write(ctx)
}

func handleNotFound(ctx iris.Context) {
	response := struct {
		Error struct {
			Message string `json:"message"`
			Code    int    `json:"code"`
		} `json:"error"`
	}{
		Error: struct {
			Message string `json:"message"`
			Code    int    `json:"code"`
		}{
			Message: "not found",
			Code:    404,
		},
	}
	_ = jsonResponseFrom(response, 404).write(ctx)
}

func unmarshal(body []byte, x any) *ErrorResponse {
	if len(body) == 0 {
		return newErrorResponse("empty request body", http.StatusBadRequest, nil)
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	err := dec.Decode(x)
	if err != nil {
		structType := reflect.TypeOf(x)
		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}

		msg := fmt.Sprintf(
			"could not parse %s from JSON; make sure input has correct types",
			structType,
		)
		response := newErrorResponse(msg, http.StatusBadRequest, &err)
		response.log.Info(
			"tried to create %s but input was invalid; offending JSON: %s",
			structType,
			loggableJSON(body),
		)
		return response
	}

	return nil
}

func loggableJSON(bytes []byte) []byte {
	return regWhitespace.ReplaceAll(bytes, []byte(""))
}

var regWhitespace *regexp.Regexp = regexp.MustCompile(`\s`)
