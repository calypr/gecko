package vector

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/vectoradapter"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

func (handler *Handler) registerVectorHandlers(app fiber.Router) {
	vector := app.Group("/vector")
	vector.Get("/swagger/doc.json", func(ctx fiber.Ctx) error { return ctx.SendFile("./docs/swagger.json") })
	vector.Get("/swagger", func(ctx fiber.Ctx) error {
		return ctx.Redirect().Status(fiber.StatusTemporaryRedirect).To("/vector/swagger/doc.json")
	})
	vector.Get("/swagger/*", func(ctx fiber.Ctx) error {
		return ctx.Redirect().Status(fiber.StatusTemporaryRedirect).To("/vector/swagger/doc.json")
	})

	collections := vector.Group("/collections")
	collections.Get("", handler.handleListCollections)
	collections.Put("/:collection", handler.handleCreateCollection)
	collections.Get("/:collection", handler.handleGetCollection)
	collections.Patch("/:collection", handler.handleUpdateCollection)
	collections.Delete("/:collection", handler.handleDeleteCollection)

	points := collections.Group("/:collection/points")
	points.Put("", handler.handleUpsertPoints)
	points.Get("/:id", handler.handleGetPoint)
	points.Post("/search", handler.handleQueryPoints)
	points.Post("/delete", handler.handleDeletePoints)
}

// handleListCollections godoc
// @Summary List all collections
// @Description Retrieve all vector collections.
// @Tags Vector Collections
// @Produce json
// @Success 200 {object} map[string]interface{} "Collections listed"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections [get]
func (handler *Handler) handleListCollections(ctx fiber.Ctx) error {
	resp, err := handler.qdrantClient.ListCollections(ctx)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("list collections", "", err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"result": resp, "status": "ok"}, http.StatusOK).Write(ctx)
}

// handleCreateCollection godoc
// @Summary Create a new collection
// @Description Create a collection with vector configuration.
// @Tags Vector Collections
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body vectoradapter.CreateCollectionRequest true "Collection configuration"
// @Success 200 {object} map[string]bool "Collection created"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections/{collection} [put]
func (handler *Handler) handleCreateCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var reqBody vectoradapter.CreateCollectionRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &reqBody, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	namedVectorsMap := map[string]*qdrant.VectorParams{}
	for name, params := range reqBody.Vectors {
		distanceVal, ok := qdrant.Distance_value[params.Distance]
		if !ok {
			errResponse := httputil.NewError(apierror.TypeInvalidDistance, fmt.Sprintf("invalid distance: %s", params.Distance), http.StatusBadRequest, map[string]any{"collection": collection}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		namedVectorsMap[name] = &qdrant.VectorParams{Size: params.Size, Distance: qdrant.Distance(distanceVal)}
	}

	var vectorsConfig *qdrant.VectorsConfig
	if len(namedVectorsMap) > 0 {
		vectorsConfig = qdrant.NewVectorsConfigMap(namedVectorsMap)
	}
	if err := handler.qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{CollectionName: collection, VectorsConfig: vectorsConfig}); err != nil {
		errResponse := newVectorBackendErrorResponse("create collection", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]bool{"result": true}, http.StatusOK).Write(ctx)
}

// handleGetCollection godoc
// @Summary Get collection info
// @Description Returns information about a collection by name.
// @Tags Vector Collections
// @Produce json
// @Param collection path string true "Collection name"
// @Success 200 {object} map[string]interface{} "Collection info"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [get]
func (handler *Handler) handleGetCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	resp, err := handler.qdrantClient.GetCollectionInfo(ctx, collection)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("get collection info", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(resp, http.StatusOK).Write(ctx)
}

// handleUpdateCollection godoc
// @Summary Update collection
// @Description Updates an existing collection by name.
// @Tags Vector Collections
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body qdrant.UpdateCollection true "Update collection request"
// @Success 200 {object} map[string]bool "Update successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [patch]
func (handler *Handler) handleUpdateCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req qdrant.UpdateCollection
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	req.CollectionName = collection
	if err := handler.qdrantClient.UpdateCollection(ctx, &req); err != nil {
		errResponse := newVectorBackendErrorResponse("update collection", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]bool{"result": true}, http.StatusOK).Write(ctx)
}

// handleDeleteCollection godoc
// @Summary Delete collection
// @Description Deletes a collection and all its points.
// @Tags Vector Collections
// @Produce json
// @Param collection path string true "Collection name"
// @Success 200 {object} map[string]bool "Delete successful"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [delete]
func (handler *Handler) handleDeleteCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	if err := handler.qdrantClient.DeleteCollection(ctx, collection); err != nil {
		errResponse := newVectorBackendErrorResponse("delete collection", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]bool{"result": true}, http.StatusOK).Write(ctx)
}

// handleGetPoint godoc
// @Summary Get point
// @Description Returns a single point, including vectors and payload, by ID.
// @Tags Vector
// @Produce json
// @Param collection path string true "Collection name"
// @Param id path string true "Point UUID"
// @Success 200 {object} map[string]interface{} "Point found"
// @Failure 400 {object} ErrorResponse "Invalid request or ID"
// @Failure 404 {object} ErrorResponse "Point not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points/{id} [get]
func (handler *Handler) handleGetPoint(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	idStr := ctx.Params("id")
	if idStr == "" || collection == "" {
		err := fmt.Errorf("collection or id not provide")
		errResponse := httputil.NewError(apierror.TypeMissingIdentifier, "collection or id is not provided", http.StatusBadRequest, map[string]any{"collection": collection, "id": idStr}, &err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if _, err := uuid.Parse(idStr); err != nil {
		errResponse := httputil.NewError(apierror.TypeInvalidUUID, "invalid UUID", http.StatusBadRequest, map[string]any{"collection": collection, "id": idStr}, &err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	resp, err := handler.qdrantClient.Get(ctx, &qdrant.GetPoints{CollectionName: collection, Ids: []*qdrant.PointId{qdrant.NewIDUUID(idStr)}, WithPayload: qdrant.NewWithPayload(true), WithVectors: qdrant.NewWithVectors(true)})
	if err != nil {
		errResponse := newVectorBackendErrorResponse("get point", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if len(resp) == 0 {
		errResponse := httputil.NewError(apierror.TypePointNotFound, "point not found", http.StatusNotFound, map[string]any{"collection": collection, "id": idStr}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(vectoradapter.ConvertQdrantRetrievedPointsResponse(resp), http.StatusOK).Write(ctx)
}

// handleQueryPoints godoc
// @Summary Query points in a collection
// @Description Executes a kNN or recommendation query against a collection.
// @Tags Vector Search
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param request body vectoradapter.QueryPointsRequest true "Query request body"
// @Success 200 {array} vectoradapter.QueryPointsResponseItem
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /vector/collections/{collection}/points/search [post]
func (handler *Handler) handleQueryPoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req vectoradapter.QueryPointsRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	qdrantReq, err := vectoradapter.ToQdrantQuery(req, collection)
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeInvalidQueryParameter, fmt.Sprintf("invalid query parameter: %s", err), http.StatusBadRequest, map[string]any{"collection": collection}, &err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	resp, err := handler.qdrantClient.Query(ctx, qdrantReq)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("query points", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(vectoradapter.ConvertQdrantPointsResponse(resp), http.StatusOK).Write(ctx)
}

// handleUpsertPoints godoc
// @Summary Upsert points
// @Description Inserts new points or updates existing ones.
// @Tags Vector
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body vectoradapter.UpsertRequest true "Upsert request"
// @Success 200 {object} map[string]interface{} "Upsert successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points [put]
func (handler *Handler) handleUpsertPoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var reqBody vectoradapter.UpsertRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &reqBody, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	upsertReq, err := vectoradapter.ToQdrantUpsert(reqBody, collection)
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeInvalidPointData, err.Error(), http.StatusBadRequest, map[string]any{"collection": collection}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	resp, err := handler.qdrantClient.Upsert(ctx, upsertReq)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("upsert points", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(resp, http.StatusOK).Write(ctx)
}

// handleDeletePoints godoc
// @Summary Delete points
// @Description Deletes points from a collection based on the provided selector.
// @Tags Vector
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body vectoradapter.DeletePoints true "Delete request"
// @Success 200 {object} map[string]bool "Delete successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points/delete [post]
func (handler *Handler) handleDeletePoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req vectoradapter.DeletePoints
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	deleteReq, err := vectoradapter.ToQdrantDelete(req, collection)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("delete points", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	deleteReq.CollectionName = collection
	if _, err := handler.qdrantClient.Delete(ctx, deleteReq); err != nil {
		errResponse := newVectorBackendErrorResponse("delete points", collection, err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]bool{"result": true}, http.StatusOK).Write(ctx)
}

func vectorBackendErrorType(err error) apierror.Type {
	statusCode := vectoradapter.MapQdrantErrorToHTTPStatus(err)
	switch statusCode {
	case http.StatusNotFound:
		return apierror.TypeVectorCollectionNotFound
	case http.StatusConflict:
		return apierror.TypeVectorCollectionAlreadyExists
	case http.StatusServiceUnavailable:
		return apierror.TypeVectorStoreUnavailable
	case http.StatusBadRequest:
		return apierror.TypeInvalidVectorRequest
	default:
		return apierror.TypeVectorOperationFailed
	}
}

func newVectorBackendErrorResponse(action, collection string, err error) *httputil.ErrorResponse {
	details := map[string]any{}
	if collection != "" {
		details["collection"] = collection
	}
	if len(details) == 0 {
		details = nil
	}
	return httputil.NewError(
		vectorBackendErrorType(err),
		fmt.Sprintf("failed to %s: %s", action, err),
		vectoradapter.MapQdrantErrorToHTTPStatus(err),
		details,
		&err,
	)
}
