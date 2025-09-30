package adapter

import (
	"fmt"

	"github.com/qdrant/go-client/qdrant"
)

func toPointID(id any) (*qdrant.PointId, error) {
	switch v := id.(type) {
	case string:
		return &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: v}}, nil
	case float64:
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
