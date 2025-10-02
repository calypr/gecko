package adapter

import (
	"fmt"

	"github.com/qdrant/go-client/qdrant"
)

func toPointID(id any) (*qdrant.PointId, error) {
	switch v := id.(type) {
	case string:
		return &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: v}}, nil
	case float32:
		return &qdrant.PointId{PointIdOptions: &qdrant.PointId_Num{Num: uint64(v)}}, nil
	default:
		return nil, fmt.Errorf("unsupported point id type: %T", id)
	}
}

func ptr[T any](v T) *T {
	return &v
}
func ToQdrantUpsert(req UpsertRequest, collection string) (*qdrant.UpsertPoints, error) {
	upsert := &qdrant.UpsertPoints{
		CollectionName: collection,
		Points:         make([]*qdrant.PointStruct, 0, len(req.Points)),
		Wait:           ptr(true),
	}

	for _, p := range req.Points {
		// Validation check: Ensure the required vector name is provided
		if p.VectorName == "" {
			// Return an error if the client failed to provide the name
			return nil, fmt.Errorf("vector_name is required for point ID %v", p.ID)
		}

		pointID, err := toPointID(p.ID)
		if err != nil {
			return nil, err
		}

		var qPayload map[string]*qdrant.Value
		if p.Payload != nil {
			qPayload = qdrant.NewValueMap(p.Payload)
		}

		qp := &qdrant.PointStruct{
			Id: pointID,
			Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{
				p.VectorName: qdrant.NewVector(p.Vector...),
			}),
			Payload: qPayload,
		}
		upsert.Points = append(upsert.Points, qp)
	}

	return upsert, nil
}

func ToQdrantDelete(req DeletePoints, collection string) (*qdrant.DeletePoints, error) {
	pointIDs := make([]*qdrant.PointId, 0, len(req.Points))
	for _, id := range req.Points {
		pointIDs = append(pointIDs, qdrant.NewIDUUID(id))
	}
	delete := &qdrant.DeletePoints{
		CollectionName: collection,
		Points:         qdrant.NewPointsSelectorIDs(pointIDs),
		Wait:           ptr(true),
	}
	return delete, nil
}

// ToQdrantSearchParams converts the custom SearchParamsRequest to the Qdrant gRPC SearchParams struct.
func ToQdrantSearchParams(req *SearchParamsRequest) *qdrant.SearchParams {
	if req == nil {
		return nil
	}

	params := &qdrant.SearchParams{}

	// HnswEf is a oneof field, but the Qdrant Go client allows setting it directly
	// in the SearchParams struct. We'll set it here to allow configuration.
	if req.HnswEf != nil {
		params.HnswEf = req.HnswEf
	}

	// Exact is also a separate field for convenience in the Go client.
	if req.Exact != nil {
		params.Exact = req.Exact
	}

	// If no fields were set, return nil
	if params.Exact == nil {
		return nil
	}

	return params
}

// ToQdrantQuery converts the custom HTTP request struct to the Qdrant gRPC QueryPoints struct.
func ToQdrantQuery(req QueryPointsRequest, collection string) (*qdrant.QueryPoints, error) {
	var queryVariant *qdrant.Query

	var positives []*qdrant.VectorInput
	var negatives []*qdrant.VectorInput

	if req.LookupID != nil {
		pointID, err := toPointID(*req.LookupID)
		if err != nil {
			return nil, fmt.Errorf("invalid lookup_id: %w", err)
		}
		positives = append(positives, qdrant.NewVectorInputID(pointID))
	}

	for _, p := range req.Positives {
		pointID, err := toPointID(p)
		if err != nil {
			return nil, fmt.Errorf("invalid positive ID: %w", err)
		}
		positives = append(positives, qdrant.NewVectorInputID(pointID))
	}

	for _, n := range req.Negatives {
		pointID, err := toPointID(n)
		if err != nil {
			return nil, fmt.Errorf("invalid negative ID: %w", err)
		}
		negatives = append(negatives, qdrant.NewVectorInputID(pointID))
	}

	hasQueryVector := len(req.Query) > 0
	hasRecommend := len(positives) > 0 || len(negatives) > 0

	if hasQueryVector && hasRecommend {
		return nil, fmt.Errorf("cannot use both 'query' vector and recommend inputs (positives/negatives/lookup_id) simultaneously")
	}

	if hasRecommend {
		if len(positives) == 0 {
			return nil, fmt.Errorf("must provide at least one positive for recommend query")
		}
		queryVariant = qdrant.NewQueryRecommend(
			&qdrant.RecommendInput{
				Positive: positives,
				Negative: negatives,
			},
		)
	} else if hasQueryVector {
		queryVariant = qdrant.NewQueryNearest(qdrant.NewVectorInput(req.Query...))
	} else {
		return nil, fmt.Errorf("must specify either 'query' vector or recommend inputs (positives/negatives/lookup_id)")
	}

	var using *string
	if req.VectorName != "" {
		using = ptr(req.VectorName)
	}

	qdrantQuery := &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          queryVariant,
		Limit:          ptr(req.Limit),
		Offset:         req.Offset,
		ScoreThreshold: req.ScoreThreshold,
		Using:          using,
		Filter:         ToQdrantFilter(req.Filter),
		Params:         ToQdrantSearchParams(req.Params),
		WithPayload:    ToQdrantWithPayload(req.WithPayload),
		WithVectors:    ToQdrantWithVectors(req.WithVector),
	}

	return qdrantQuery, nil
}

// ToQdrantFilter converts custom filter structs to *qdrant.Filter
func ToQdrantFilter(filter *HeadFilter) *qdrant.Filter {
	if filter == nil || len(filter.Must) == 0 {
		return nil
	}

	mustConditions := make([]*qdrant.Condition, len(filter.Must), len(filter.Must))

	for i, cond := range filter.Must {
		switch v := cond.Match.Value.(type) {
		case string:
			mustConditions[i] = qdrant.NewMatch(cond.Key, v)
		case int64:
			mustConditions[i] = qdrant.NewMatchInt(cond.Key, v)
		case bool:
			mustConditions[i] = qdrant.NewMatchBool(cond.Key, v)
		default:
			continue
		}
	}
	return &qdrant.Filter{
		Must: mustConditions,
	}
}

// ToQdrantWithPayload converts *bool to *qdrant.WithPayloadSelector
func ToQdrantWithPayload(b *bool) *qdrant.WithPayloadSelector {
	if b == nil || *b == false {
		// If not specified or explicitly false, return the default to skip payload
		return qdrant.NewWithPayload(false)
	}
	// If true, request all payload fields
	return qdrant.NewWithPayload(true)
}

// ToQdrantWithVectors converts *bool to *qdrant.WithVectorsSelector
func ToQdrantWithVectors(b *bool) *qdrant.WithVectorsSelector {
	if b == nil || *b == false {
		// If not specified or explicitly false, return the default to skip vectors
		return qdrant.NewWithVectors(false)
	}
	// If true, request all vector fields
	return qdrant.NewWithVectors(true)
}
