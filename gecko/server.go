package gecko

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/kataras/iris/v12"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
)

type LogHandler struct {
	logger *log.Logger
}

type Server struct {
	iris         *iris.Application
	db           *sqlx.DB
	jwtApp       arborist.JWTDecoder
	logger       *LogHandler
	stmts        *arborist.CachedStmts
	qdrantClient *qdrant.Client
}

func NewServer() *Server {
	return &Server{}
}

func (server *Server) WithLogger(logger *log.Logger) *Server {
	server.logger = &LogHandler{logger: logger}
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

func (server *Server) Init() (*Server, error) {
	if server.db == nil {
		return nil, errors.New("gecko server initialized without database")
	}
	if server.jwtApp == nil {
		return nil, errors.New("gecko server initialized without JWT app")
	}
	if server.logger == nil {
		return nil, errors.New("gecko server initialized without logger")
	}
	if server.qdrantClient == nil {
		return nil, errors.New("gecko server initialized without Qdrant client")
	}
	server.logger.Info("Gecko server initialized successfully.")
	return server, nil
}

func (server *Server) MakeRouter() *iris.Application {
	router := iris.New()
	if router == nil {
		server.logger.Error("Failed to initialize router")
	}
	router.Use(recoveryMiddleware)
	router.Use(server.logRequestMiddleware)
	router.OnErrorCode(iris.StatusNotFound, handleNotFound)
	router.Get("/health", server.handleHealth)
	router.Get("/config/{configId}", server.handleConfigGET)
	router.Put("/config/{configId}", server.handleConfigPUT)
	router.Get("/config/list", server.handleConfigListGET)
	router.Delete("/config/{configId}", server.handleConfigDELETE)

	// Add Qdrant vector endpoints under /vector
	vectorRouter := router.Party("/vector")
	{
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
				points.Post("/search", server.handleQueryPoints) // Using Query as per client
				points.Post("/delete", server.handleDeletePoints)
			}
		}
	}

	router.UseRouter(func(ctx iris.Context) {
		req := ctx.Request()
		if req == nil || req.URL == nil {
			server.logger.Warning("Request or URL is nil")
			ctx.StatusCode(http.StatusInternalServerError)
			ctx.WriteString("Internal Server Error")
			return
		}
		req.URL.Path = strings.TrimSuffix(req.URL.Path, "/")
		ctx.Next()
	})

	if err := router.Build(); err != nil {
		server.logger.Error("Failed to build Iris router: %v", err)
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

func (server *Server) handleHealth(ctx iris.Context) {
	err := server.db.Ping()
	if err != nil {
		server.logger.Error("Database ping failed: %v", err)
		response := newErrorResponse("database unavailable", 500, nil)
		_ = response.write(ctx)
		return
	}
	server.logger.Info("Health check passed")
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
	err := json.Unmarshal(body, x)
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
