package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

func assertJSONRoundTrip(t *testing.T, input string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(input), target); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	marshaled, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var originalMap any
	if err := json.Unmarshal([]byte(input), &originalMap); err != nil {
		t.Fatalf("unmarshal original map: %v", err)
	}
	var marshaledMap any
	if err := json.Unmarshal(marshaled, &marshaledMap); err != nil {
		t.Fatalf("unmarshal marshaled map: %v", err)
	}
	if reflect.DeepEqual(originalMap, marshaledMap) {
		return
	}
	wantJSON, _ := json.MarshalIndent(originalMap, "", "  ")
	gotJSON, _ := json.MarshalIndent(marshaledMap, "", "  ")
	t.Fatalf("round-trip mismatch\nwant: %s\ngot: %s", wantJSON, gotJSON)
}
