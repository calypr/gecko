package adapter

import "github.com/qdrant/go-client/qdrant"

// convertQdrantPointsResponse transforms a Qdrant QueryResponse to simplify payloads.
func ConvertQdrantPointsResponse(resp []*qdrant.ScoredPoint) []map[string]any {
	result := make([]map[string]any, len(resp))
	for i, point := range resp {
		simplifiedPoint := map[string]any{
			"id":    point.Id.GetUuid(),
			"score": point.Score,
		}

		// Convert vectors if present
		if point.Vectors != nil {
			vectors := make(map[string]any)
			if vectorsMap := point.Vectors.GetVectors(); vectorsMap != nil {
				for name, vec := range vectorsMap.Vectors {
					vectors[name] = vec.Data
				}
			} else if vector := point.Vectors.GetVector(); vector != nil {
				vectors["default"] = vector.Data
			}
			simplifiedPoint["vectors"] = vectors
		}

		// Convert payload if present
		if point.Payload != nil {
			payload := make(map[string]any)
			for key, value := range point.Payload {
				payload[key] = convertQdrantValueToJSON(value)
			}
			simplifiedPoint["payload"] = payload
		}

		result[i] = simplifiedPoint
	}
	return result
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
