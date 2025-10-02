package gecko

import (
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/adapter"
	"github.com/google/uuid"
	"github.com/kataras/iris/v12"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

func (server *Server) handleQueryPoints(ctx iris.Context) {
	collection := ctx.Params().Get("collection")
	var req adapter.QueryPointsRequest
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse("invalid request body format", http.StatusBadRequest, &err)
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
		errResponse := newErrorResponse(msg, mapQdrantErrorToHTTPStatus(err), nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	simplifiedResp := adapter.ConvertQdrantPointsResponse(resp)

	_ = jsonResponseFrom(simplifiedResp, http.StatusOK).write(ctx)
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

func mapQdrantErrorToHTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	st, ok := status.FromError(err)
	if !ok {
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
