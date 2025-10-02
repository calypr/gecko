package adapter

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Upsert Structs
type UpsertRequest struct {
	Points []Point `json:"points"`
}

type Point struct {
	ID         string         `json:"id"`
	VectorName string         `json:"vector_name"` // This comes from the collection when it is created
	Vector     []float32      `json:"vector"`
	Payload    map[string]any `json:"payload,omitempty"`
}

type UpsertResponse struct {
	Result bool `json:"result"`
}

// Collection Structs
type VectorParams struct {
	Size     uint64 `json:"size"`
	Distance string `json:"distance"`
}

type CreateCollectionRequest struct {
	Vectors map[string]VectorParams `json:"vectors"`
}

// Delete Points
type DeletePoints struct {
	Points []string   `json:"points"`
	Filter HeadFilter `json:"filter,omitempty"`
}

// Query Structs
type QueryPointsRequest struct {
	// Query is for standard kNN search (vector input)
	Query []float32 `json:"query,omitempty"`
	// LookupID is for single-ID recommendation (backward compatible)
	LookupID *string `json:"lookup_id,omitempty"`
	// Positives and Negatives for multi-ID recommendation
	Positives []string `json:"positives,omitempty"`
	Negatives []string `json:"negatives,omitempty"`

	VectorName     string               `json:"vector_name"`
	Limit          uint64               `json:"limit"`
	Offset         *uint64              `json:"offset,omitempty"`
	ScoreThreshold *float32             `json:"score_threshold,omitempty"`
	Filter         *HeadFilter          `json:"filter,omitempty"`
	Params         *SearchParamsRequest `json:"params,omitempty"`
	WithPayload    *bool                `json:"with_payload,omitempty"`
	WithVector     *bool                `json:"with_vector,omitempty"`
}

type SearchParamsRequest struct {
	HnswEf        *uint64 `json:"hnsw_ef,omitempty"`
	Exact         *bool   `json:"exact,omitempty"`
	Quantization  *bool   `json:"quantization,omitempty"`
	SearchWithout *bool   `json:"search_without_vectors,omitempty"`
}

type HeadFilter struct {
	Must []IndFilter `json:"must"`
}

type IndFilter struct {
	Key   string      `json:"key"`
	Match MatchFilter `json:"match"`
}

type MatchFilter struct {
	Value any `json:"value"`
}

func MapQdrantErrorToHTTPStatus(err error) int {
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
