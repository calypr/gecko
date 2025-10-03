package gecko

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/adapter"
	"github.com/google/uuid"
	"github.com/kataras/iris/v12"
	"github.com/qdrant/go-client/qdrant"
)

// handleListCollections godoc
// @Summary List all collections
// @Description Retrieve all collections
// @Tags Vector Collections
// @Produce json
// @Success 200 {object} map[string]interface{} "Collections listed"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections [get]
func (server *Server) handleListCollections(ctx iris.Context) {
	resp, err := server.qdrantClient.ListCollections(ctx.Request().Context())
	if err != nil {
		msg := fmt.Sprintf("failed to list collections: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	successResponse := map[string]any{
		"result": resp,
		"status": "ok",
	}

	jsonResponseFrom(successResponse, http.StatusOK).write(ctx)
}

// handleCreateCollection godoc
// @Summary Create a new collection
// @Description Create a collection with vector configuration
// @Tags Vector Collections
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body adapter.CreateCollectionRequest true "Collection configuration"
// @Success 200 {object} map[string]bool "Collection created"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections/{collection} [put]
func (server *Server) handleCreateCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")

	var reqBody adapter.CreateCollectionRequest
	if err := ctx.ReadJSON(&reqBody); err != nil {
		msg := fmt.Sprintf("invalid request body: JSON parsing failed: %s", err.Error())
		errResponse := newErrorResponse(msg, http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	namedVectorsMap := map[string]*qdrant.VectorParams{}
	for name, params := range reqBody.Vectors {
		distanceVal, ok := qdrant.Distance_value[params.Distance]
		if !ok {
			msg := fmt.Sprintf("invalid distance: %s", params.Distance)
			errResponse := newErrorResponse(msg, http.StatusBadRequest, nil)
			errResponse.log.write(server.logger)
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
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

// handleGetCollection retrieves metadata/info for a specific collection.
// @Summary Get collection info
// @Description Returns information about a collection by name
// @Tags Vector Collections
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Success 200 {object} jsonResponse "Collection info"
// @Failure 400 {object} ErrorResponse "Invalid collection name"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [get]
func (server *Server) handleGetCollection(ctx iris.Context) {

	collection := ctx.Params().Get("collection")
	resp, err := server.qdrantClient.GetCollectionInfo(ctx.Request().Context(), collection)
	if err != nil {
		msg := fmt.Sprintf("failed to get collection info: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponse := jsonResponseFrom(resp, http.StatusOK)
	server.logger.Info("%#v", jsonResponse)
	jsonResponse.write(ctx)
}

// handleUpdateCollection updates metadata/settings for a specific collection.
// @Summary Update collection
// @Description Updates an existing collection by name
// @Tags Vector Collections
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Param body body qdrant.UpdateCollection true "Update collection request"
// @Success 200 {object} jsonResponse "Update successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [patch]
func (server *Server) handleUpdateCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req qdrant.UpdateCollection
	if err := ctx.ReadJSON(&req); err != nil {
		msg := fmt.Sprintf("invalid request body: JSON parsing failed: %s", err.Error())
		errResponse := newErrorResponse(msg, http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	req.CollectionName = collection
	err := server.qdrantClient.UpdateCollection(ctx.Request().Context(), &req)
	if err != nil {
		msg := fmt.Sprintf("failed to update collection: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

// handleDeleteCollection deletes a collection by name.
// @Summary Delete collection
// @Description Deletes a collection and all its points
// @Tags Vector Collections
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Success 200 {object} jsonResponse "Delete successful"
// @Failure 404 {object} ErrorResponse "Collection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection} [delete]
func (server *Server) handleDeleteCollection(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	err := server.qdrantClient.DeleteCollection(ctx.Request().Context(), collection)
	if err != nil {
		msg := fmt.Sprintf("failed to delete collection: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

// handleGetPoint retrieves a single point from a collection.
// @Summary Get point
// @Description Returns a single point (with vectors and payload) by ID
// @Tags Vector
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Param id path string true "Point UUID"
// @Success 200 {object} jsonResponse "Point found"
// @Failure 400 {object} ErrorResponse "Invalid request or ID"
// @Failure 404 {object} ErrorResponse "Point not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points/{id} [get]
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
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
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
	jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

// handleQueryPoints godoc
// @Summary Query points in a collection
// @Description Executes a kNN or recommendation query against a collection
// @Tags Vector Search
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param request body adapter.QueryPointsRequest true "Query request body"
// @Success 200 {object} adapter.QueryPointsResponseItem
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /vector/collections/{collection}/points/search [post]
func (server *Server) handleQueryPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req adapter.QueryPointsRequest
	if err := ctx.ReadJSON(&req); err != nil {
		msg := fmt.Sprintf("invalid request body: JSON parsing failed: %s", err.Error())
		errResponse := newErrorResponse(msg, http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	qdrantReq, err := adapter.ToQdrantQuery(req, collection)
	if err != nil {
		errResponse := newErrorResponse(fmt.Sprintf("invalid query parameter: %s", err.Error()), http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	resp, err := server.qdrantClient.Query(ctx.Request().Context(), qdrantReq)
	if err != nil {
		msg := fmt.Sprintf("failed to query points: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponseFrom(adapter.ConvertQdrantPointsResponse(resp), http.StatusOK).write(ctx)
}

// handleUpsertPoints inserts or updates points in a collection.
// @Summary Upsert points
// @Description Inserts new points or updates existing ones
// @Tags Vector
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Param body body adapter.UpsertRequest true "Upsert request"
// @Success 200 {object} jsonResponse "Upsert successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points [put]
func (server *Server) handleUpsertPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")

	var reqBody adapter.UpsertRequest
	if err := ctx.ReadJSON(&reqBody); err != nil {
		msg := fmt.Sprintf("invalid request body: JSON parsing failed: %s", err.Error())
		errResponse := newErrorResponse(msg, http.StatusBadRequest, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	upsertReq, err := adapter.ToQdrantUpsert(reqBody, collection)
	if err != nil {
		errResponse := newErrorResponse(err.Error(), http.StatusBadRequest, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	resp, err := server.qdrantClient.Upsert(ctx.Request().Context(), upsertReq)
	if err != nil {
		msg := fmt.Sprintf("failed to upsert points: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

// handleDeletePoints deletes one or more points from a collection.
// @Summary Delete points
// @Description Deletes points in a collection based on filter or IDs
// @Tags Vector
// @Accept  json
// @Produce  json
// @Param collection path string true "Collection name"
// @Param body body adapter.DeletePoints true "Delete points request"
// @Success 200 {object} jsonResponse "Delete successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points/delete [post]
func (server *Server) handleDeletePoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req adapter.DeletePoints
	if err := ctx.ReadJSON(&req); err != nil {
		msg := fmt.Sprintf("invalid request body: JSON parsing failed: %s", err.Error())
		errResponse := newErrorResponse(msg, http.StatusBadRequest, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	Deletereq, err := adapter.ToQdrantDelete(req, collection)
	if err != nil {
		msg := fmt.Sprintf("failed to delete points: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	Deletereq.CollectionName = collection
	_, err = server.qdrantClient.Delete(ctx.Request().Context(), Deletereq)
	if err != nil {
		msg := fmt.Sprintf("failed to delete points: %s", err.Error())
		errResponse := newErrorResponse(msg, adapter.MapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}
	jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}
