package adapter

import "github.com/qdrant/go-client/qdrant"

func convertVectors(v *qdrant.VectorsOutput) map[string]any {
	if v == nil || v.VectorsOptions == nil {
		return nil
	}
	vectors := make(map[string]any)
	switch vo := v.VectorsOptions.(type) {
	case *qdrant.VectorsOutput_Vector:
		if vo.Vector != nil {
			vectors["default"] = vo.Vector.Data
		}
	case *qdrant.VectorsOutput_Vectors:
		if vo.Vectors != nil && vo.Vectors.Vectors != nil {
			for name, vec := range vo.Vectors.Vectors {
				if vec != nil {
					vectors[name] = vec.Data
				}
			}
		}
	}
	return vectors
}

func convertPayload(p map[string]*qdrant.Value) map[string]any {
	if p == nil {
		return nil
	}
	payload := make(map[string]any)
	for key, value := range p {
		payload[key] = convertQdrantValueToJSON(value)
	}
	return payload
}

// ConvertQdrantPointsResponse transforms a Qdrant QueryResponse to simplify payloads.
func ConvertQdrantPointsResponse(resp []*qdrant.ScoredPoint) []map[string]any {
	result := make([]map[string]any, len(resp))
	for i, point := range resp {
		result[i] = map[string]any{
			"id":      point.Id.GetUuid(),
			"score":   point.Score,
			"vectors": convertVectors(point.Vectors),
			"payload": convertPayload(point.Payload),
		}
	}
	return result
}

// ConvertQdrantRetrievedPointsResponse transforms a Qdrant GetResponse to simplify payloads.
func ConvertQdrantRetrievedPointsResponse(resp []*qdrant.RetrievedPoint) []map[string]any {
	result := make([]map[string]any, len(resp))
	for i, point := range resp {
		result[i] = map[string]any{
			"id":      point.Id.GetUuid(),
			"vectors": convertVectors(point.Vectors),
			"payload": convertPayload(point.Payload),
		}
	}
	return result
}

func ConvertQdrantCollectionInfo(info *qdrant.CollectionInfo) map[string]any {
	if info == nil {
		return nil
	}
	status := "Unknown"
	if s := info.GetStatus(); s != qdrant.CollectionStatus_UnknownCollectionStatus {
		status = s.String()
	}

	return map[string]any{
		"status":                status,
		"optimizer_status":      info.GetOptimizerStatus().String(),
		"vectors_count":         info.GetVectorsCount(),
		"segments_count":        info.GetSegmentsCount(),
		"config":                info.GetConfig(),
		"payloadSchema":         info.GetPayloadSchema(),
		"points_count":          info.GetPointsCount(),
		"indexed_vectors_count": info.GetIndexedVectorsCount(),
	}
}

func convertQdrantValueToJSON(value *qdrant.Value) any {
	if value == nil || value.Kind == nil {
		return nil
	}
	switch kind := value.Kind.(type) {
	case *qdrant.Value_NullValue:
		return nil
	case *qdrant.Value_BoolValue:
		return kind.BoolValue
	case *qdrant.Value_IntegerValue:
		return kind.IntegerValue
	case *qdrant.Value_DoubleValue:
		return kind.DoubleValue
	case *qdrant.Value_StringValue:
		return kind.StringValue
	case *qdrant.Value_ListValue:
		list := make([]any, len(kind.ListValue.Values))
		for i, v := range kind.ListValue.Values {
			list[i] = convertQdrantValueToJSON(v)
		}
		return list
	case *qdrant.Value_StructValue:
		m := make(map[string]any)
		for k, v := range kind.StructValue.Fields {
			m[k] = convertQdrantValueToJSON(v)
		}
		return m
	default:
		return nil
	}
}
