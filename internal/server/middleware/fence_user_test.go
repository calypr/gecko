package middleware

import "testing"

func TestSnapshotAllows(t *testing.T) {
	raw := []any{
		map[string]any{"service": "*", "method": "read"},
		map[string]any{"service": "arborist", "method": "create-descendant"},
	}
	if !snapshotAllows(raw, "read", "*") {
		t.Fatalf("expected wildcard read to match")
	}
	if !snapshotAllows(raw, "create-descendant", "arborist") {
		t.Fatalf("expected arborist create-descendant to match")
	}
	if snapshotAllows(raw, "update", "*") {
		t.Fatalf("did not expect update to match")
	}
}

func TestFenceUserEndpoint(t *testing.T) {
	token := "Bearer eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2V4YW1wbGUub3JnL3VzZXIifQ."
	endpoint, err := fenceUserEndpoint(token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint != "https://example.org/user/user" {
		t.Fatalf("unexpected endpoint %q", endpoint)
	}
}
