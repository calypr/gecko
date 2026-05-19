package server

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/adapter"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

// handleListCollections godoc
// @Summary List all collections
// @Description Retrieve all vector collections.
// @Tags Vector Collections
// @Produce json
// @Success 200 {object} map[string]interface{} "Collections listed"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections [get]
func (server *Server) handleListCollections(ctx fiber.Ctx) error {
	resp, err := server.qdrantClient.ListCollections(ctx)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("list collections", "", err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]any{"result": resp, "status": "ok"}, http.StatusOK).write(ctx)
}

// handleCreateCollection godoc
// @Summary Create a new collection
// @Description Create a collection with vector configuration.
// @Tags Vector Collections
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body adapter.CreateCollectionRequest true "Collection configuration"
// @Success 200 {object} map[string]bool "Collection created"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /vector/collections/{collection} [put]
func (server *Server) handleCreateCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var reqBody adapter.CreateCollectionRequest
	if errResponse := parseJSONBody(ctx.Body(), &reqBody, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}

	namedVectorsMap := map[string]*qdrant.VectorParams{}
	for name, params := range reqBody.Vectors {
		distanceVal, ok := qdrant.Distance_value[params.Distance]
		if !ok {
			errResponse := newTypedErrorResponse(apierror.TypeInvalidDistance, fmt.Sprintf("invalid distance: %s", params.Distance), http.StatusBadRequest, map[string]any{"collection": collection}, nil)
			errResponse.log.write(server.Logger)
			return errResponse.write(ctx)
		}
		namedVectorsMap[name] = &qdrant.VectorParams{Size: params.Size, Distance: qdrant.Distance(distanceVal)}
	}

	var vectorsConfig *qdrant.VectorsConfig
	if len(namedVectorsMap) > 0 {
		vectorsConfig = qdrant.NewVectorsConfigMap(namedVectorsMap)
	}
	if err := server.qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{CollectionName: collection, VectorsConfig: vectorsConfig}); err != nil {
		errResponse := newVectorBackendErrorResponse("create collection", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
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
func (server *Server) handleGetCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	resp, err := server.qdrantClient.GetCollectionInfo(ctx, collection)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("get collection info", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(resp, http.StatusOK).write(ctx)
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
func (server *Server) handleUpdateCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req qdrant.UpdateCollection
	if errResponse := parseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	req.CollectionName = collection
	if err := server.qdrantClient.UpdateCollection(ctx, &req); err != nil {
		errResponse := newVectorBackendErrorResponse("update collection", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
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
func (server *Server) handleDeleteCollection(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	if err := server.qdrantClient.DeleteCollection(ctx, collection); err != nil {
		errResponse := newVectorBackendErrorResponse("delete collection", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
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
func (server *Server) handleGetPoint(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	idStr := ctx.Params("id")
	if idStr == "" || collection == "" {
		err := fmt.Errorf("collection or id not provide")
		errResponse := newTypedErrorResponse(apierror.TypeMissingIdentifier, "collection or id is not provided", http.StatusBadRequest, map[string]any{"collection": collection, "id": idStr}, &err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	if _, err := uuid.Parse(idStr); err != nil {
		errResponse := newTypedErrorResponse(apierror.TypeInvalidUUID, "invalid UUID", http.StatusBadRequest, map[string]any{"collection": collection, "id": idStr}, &err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	resp, err := server.qdrantClient.Get(ctx, &qdrant.GetPoints{CollectionName: collection, Ids: []*qdrant.PointId{qdrant.NewIDUUID(idStr)}, WithPayload: qdrant.NewWithPayload(true), WithVectors: qdrant.NewWithVectors(true)})
	if err != nil {
		errResponse := newVectorBackendErrorResponse("get point", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	if len(resp) == 0 {
		errResponse := newTypedErrorResponse(apierror.TypePointNotFound, "point not found", http.StatusNotFound, map[string]any{"collection": collection, "id": idStr}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(adapter.ConvertQdrantRetrievedPointsResponse(resp), http.StatusOK).write(ctx)
}

// handleQueryPoints godoc
// @Summary Query points in a collection
// @Description Executes a kNN or recommendation query against a collection.
// @Tags Vector Search
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param request body adapter.QueryPointsRequest true "Query request body"
// @Success 200 {array} adapter.QueryPointsResponseItem
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /vector/collections/{collection}/points/search [post]
func (server *Server) handleQueryPoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req adapter.QueryPointsRequest
	if errResponse := parseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	qdrantReq, err := adapter.ToQdrantQuery(req, collection)
	if err != nil {
		errResponse := newTypedErrorResponse(apierror.TypeInvalidQueryParameter, fmt.Sprintf("invalid query parameter: %s", err), http.StatusBadRequest, map[string]any{"collection": collection}, &err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	resp, err := server.qdrantClient.Query(ctx, qdrantReq)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("query points", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(adapter.ConvertQdrantPointsResponse(resp), http.StatusOK).write(ctx)
}

// handleUpsertPoints godoc
// @Summary Upsert points
// @Description Inserts new points or updates existing ones.
// @Tags Vector
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body adapter.UpsertRequest true "Upsert request"
// @Success 200 {object} map[string]interface{} "Upsert successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points [put]
func (server *Server) handleUpsertPoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var reqBody adapter.UpsertRequest
	if errResponse := parseJSONBody(ctx.Body(), &reqBody, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	upsertReq, err := adapter.ToQdrantUpsert(reqBody, collection)
	if err != nil {
		errResponse := newTypedErrorResponse(apierror.TypeInvalidPointData, err.Error(), http.StatusBadRequest, map[string]any{"collection": collection}, nil)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	resp, err := server.qdrantClient.Upsert(ctx, upsertReq)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("upsert points", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(resp, http.StatusOK).write(ctx)
}

// handleDeletePoints godoc
// @Summary Delete points
// @Description Deletes points from a collection based on the provided selector.
// @Tags Vector
// @Accept json
// @Produce json
// @Param collection path string true "Collection name"
// @Param body body adapter.DeletePoints true "Delete request"
// @Success 200 {object} map[string]bool "Delete successful"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /vector/collections/{collection}/points/delete [post]
func (server *Server) handleDeletePoints(ctx fiber.Ctx) error {
	collection := ctx.Params("collection")
	var req adapter.DeletePoints
	if errResponse := parseJSONBody(ctx.Body(), &req, map[string]any{"collection": collection}); errResponse != nil {
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	deleteReq, err := adapter.ToQdrantDelete(req, collection)
	if err != nil {
		errResponse := newVectorBackendErrorResponse("delete points", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	deleteReq.CollectionName = collection
	if _, err := server.qdrantClient.Delete(ctx, deleteReq); err != nil {
		errResponse := newVectorBackendErrorResponse("delete points", collection, err)
		errResponse.log.write(server.Logger)
		return errResponse.write(ctx)
	}
	return jsonResponseFrom(map[string]bool{"result": true}, http.StatusOK).write(ctx)
}

func vectorBackendErrorType(err error) apierror.Type {
	statusCode := adapter.MapQdrantErrorToHTTPStatus(err)
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

func newVectorBackendErrorResponse(action, collection string, err error) *ErrorResponse {
	details := map[string]any{}
	if collection != "" {
		details["collection"] = collection
	}
	if len(details) == 0 {
		details = nil
	}
	return newTypedErrorResponse(
		vectorBackendErrorType(err),
		fmt.Sprintf("failed to %s: %s", action, err),
		adapter.MapQdrantErrorToHTTPStatus(err),
		details,
		&err,
	)
}
