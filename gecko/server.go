package gecko

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/calypr/gecko/gecko/adapter"
	"github.com/calypr/gecko/gecko/config"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/iris/v12"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/arborist/arborist"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	server.logger.Info("DB: %#v, JWTApp: %#v, Logger: %#v", server.db, server.jwtApp, server.logger)
	if server.qdrantClient == nil {
		qdrantHost := os.Getenv("QDRANT_HOST")
		if qdrantHost == "" {
			qdrantHost = "qdrant"
		}
		qdrantPort := 6334
		qdrantAPIKey := os.Getenv("QDRANT_API_KEY")
		qdrantConfig := &qdrant.Config{
			Host:   qdrantHost,
			Port:   qdrantPort,
			APIKey: qdrantAPIKey,
			UseTLS: false,
		}
		var err error
		server.qdrantClient, err = qdrant.NewClient(qdrantConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Qdrant client: %w", err)
		}
		server.logger.Info("Initialized Qdrant client with host: %s, port: %d (from environment/default)", qdrantHost, qdrantPort)
	} else {
		server.logger.Info("Qdrant client provided via WithQdrantClient, skipping internal initialization.")
	}
	if server.qdrantClient == nil {
		return nil, errors.New("Qdrant client is nil after all initialization attempts")
	}
	return server, nil
}

func (server *Server) MakeRouter() *iris.Application {
	router := iris.New()
	if router == nil {
		server.logger.Error("Failed to initialize router")
	}
	router.Use(recoveryMiddleware)
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
				points.Post("/scroll", server.handleScrollPoints)
			}
		}
	}

	// Optionally keep UseRouter if needed, with safety checks
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

	// Build the router to ensure it's ready for net/http
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

func (server *Server) handleListCollections(ctx iris.Context) {
	resp, err := server.qdrantClient.ListCollections(ctx.Request().Context())
	if err != nil {
		msg := fmt.Sprintf("failed to list collections: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	successResponse := map[string]any{
		"result": resp,
		"status": "ok",
	}

	_ = jsonResponseFrom(successResponse, http.StatusOK).write(ctx)
}

func (server *Server) handleCreateCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")

	var reqBody adapter.CreateCollectionRequest
	if err := ctx.ReadJSON(&reqBody); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		_ = errResponse.write(ctx)
		return
	}

	namedVectorsMap := map[string]*qdrant.VectorParams{}
	for name, params := range reqBody.Vectors {
		distanceVal, ok := qdrant.Distance_value[params.Distance]
		if !ok {
			msg := fmt.Sprintf("invalid distance: %s", params.Distance)
			errResponse := newErrorResponse(msg, http.StatusBadRequest, nil)
			_ = errResponse.write(ctx)
			return
		}
		namedVectorsMap[name] = &qdrant.VectorParams{
			Size:     params.Size,
			Distance: qdrant.Distance(distanceVal),
		}
	}

	// 3. Construct the gRPC Vector Configuration using the available helper.
	var vectorsConfig *qdrant.VectorsConfig
	if len(namedVectorsMap) > 0 {
		vectorsConfig = qdrant.NewVectorsConfigMap(namedVectorsMap)
	}

	qdrantReq := &qdrant.CreateCollection{
		CollectionName: collection,
		VectorsConfig:  vectorsConfig,
	}

	err := server.qdrantClient.CreateCollection(ctx.Request().Context(), qdrantReq)
	if err != nil {
		msg := fmt.Sprintf("failed to create collection: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	_ = jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

func (server *Server) handleGetCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	resp, err := server.qdrantClient.GetCollectionInfo(ctx.Request().Context(), collection)
	if err != nil {
		msg := fmt.Sprintf("failed to get collection info: %s", err.Error())
		statusCode := mapQdrantErrorToHTTPStatus(err)
		errResponse := newErrorResponse(msg, statusCode, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

func (server *Server) handleUpdateCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req qdrant.UpdateCollection
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	req.CollectionName = collection
	err := server.qdrantClient.UpdateCollection(ctx.Request().Context(), &req)
	if err != nil {
		msg := fmt.Sprintf("failed to update collection: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

func (server *Server) handleDeleteCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	err := server.qdrantClient.DeleteCollection(ctx.Request().Context(), collection)
	if err != nil {
		msg := fmt.Sprintf("failed to delete collection: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

func (server *Server) handleGetPoint(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	idStr := ctx.Params().Get("id")
	if idStr == "" || collection == "" {
		err := fmt.Errorf("collection or id not provide")
		errResponse := newErrorResponse("collection or id is not provided", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	_, err := uuid.Parse(idStr)
	if err != nil {
		errResponse := newErrorResponse("invalid UUID", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	req := &qdrant.GetPoints{
		CollectionName: collection,
		Ids:            []*qdrant.PointId{qdrant.NewIDUUID(idStr)},
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	}

	resp, err := server.qdrantClient.Get(ctx.Request().Context(), req)
	if err != nil {
		msg := fmt.Sprintf("failed to get point: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if len(resp) == 0 {
		errResponse := newErrorResponse("point not found", http.StatusNotFound, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

func (server *Server) handleUpsertPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")

	var reqBody adapter.UpsertRequest
	if err := ctx.ReadJSON(&reqBody); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		_ = errResponse.write(ctx)
		return
	}

	upsertReq, err := adapter.ToQdrantUpsert(reqBody, collection)
	if err != nil {
		errResponse := newErrorResponse(err.Error(), http.StatusBadRequest, &err)
		_ = errResponse.write(ctx)
		return
	}

	resp, err := server.qdrantClient.Upsert(ctx.Request().Context(), upsertReq)
	if err != nil {
		msg := fmt.Sprintf("failed to upsert points: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		_ = errResponse.write(ctx)
		return
	}

	_ = jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

func (server *Server) handleQueryPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req qdrant.QueryPoints
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	req.CollectionName = collection
	resp, err := server.qdrantClient.Query(ctx.Request().Context(), &req)
	if err != nil {
		msg := fmt.Sprintf("failed to query points: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

func (server *Server) handleDeletePoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req adapter.DeletePoints
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	Deletereq, err := adapter.ToQdrantDelete(req, collection)
	if err != nil {
		msg := fmt.Sprintf("failed to delete points: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	Deletereq.CollectionName = collection
	_, err = server.qdrantClient.Delete(ctx.Request().Context(), Deletereq)
	if err != nil {
		msg := fmt.Sprintf("failed to delete points: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

func (server *Server) handleScrollPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req qdrant.ScrollPoints
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse("invalid request body", http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	req.CollectionName = collection
	resp, err := server.qdrantClient.Scroll(ctx.Request().Context(), &req)
	if err != nil {
		msg := fmt.Sprintf("failed to scroll points: %s", err.Error())
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	_ = jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

func (server *Server) handleConfigListGET(ctx iris.Context) {
	configList, err := configList(server.db)
	if configList == nil && err == nil {
		errResponse := newErrorResponse("No configs found", 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		errResponse := newErrorResponse(fmt.Sprintf("%s", err), 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	server.logger.Info("Configs: %#v", configList)
	_ = jsonResponseFrom(configList, http.StatusOK).write(ctx)
}

func (server *Server) handleConfigGET(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	doc, err := configGET(server.db, configId)
	if doc == nil && err == nil {
		msg := fmt.Sprintf("no configId found with configId: %s", configId)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		msg := fmt.Sprintf("config query failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	server.logger.Info("%#v", doc)
	_ = jsonResponseFrom(doc, http.StatusOK).write(ctx)
}

func (server *Server) handleConfigDELETE(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	doc, err := configDELETE(server.db, configId)
	if doc == false && err == nil {
		msg := fmt.Sprintf("no configId found with configId: %s", configId)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		msg := fmt.Sprintf("config query failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	okmsg := map[string]any{"code": 200, "message": fmt.Sprintf("DELETED: %s", configId)}
	server.logger.Info("%#v", okmsg)
	_ = jsonResponseFrom(okmsg, http.StatusOK).write(ctx)
}

func (server *Server) handleConfigPUT(ctx iris.Context) {
	configId := ctx.Params().Get("configId")
	data := []config.ConfigItem{}
	body, err := ctx.GetBody()
	if err != nil {
		msg := fmt.Sprintf("GetBody() failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	if !json.Valid(body) {
		msg := "Invalid JSON format"
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	errResponse := unmarshal(body, &data)
	if errResponse != nil {
		msg := fmt.Sprintf("body data unmarshal failed: %s", errResponse.err)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	err = configPUT(server.db, configId, data)
	if err != nil {
		msg := fmt.Sprintf("configPut failed: %s", err.Error())
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	okmsg := map[string]any{"code": 200, "message": fmt.Sprintf("ACCEPTED: %s", configId)}
	server.logger.Info("%#v", okmsg)
	_ = jsonResponseFrom(okmsg, http.StatusOK).write(ctx)
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

func mapQdrantErrorToHTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	st, ok := status.FromError(err)
	if !ok {
		// Not a standard gRPC error, treat as a server issue
		return http.StatusInternalServerError
	}

	switch st.Code() {
	case codes.NotFound:
		return http.StatusNotFound // 404
	case codes.InvalidArgument:
		return http.StatusBadRequest // 400
	case codes.Unauthenticated:
		return http.StatusUnauthorized // 401
	case codes.AlreadyExists:
		return http.StatusConflict // 409
	case codes.Unavailable:
		return http.StatusServiceUnavailable // 503
	default:
		// Default for unhandled gRPC errors
		return http.StatusInternalServerError // 500
	}
}
