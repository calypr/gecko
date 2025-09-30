package adapter

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
