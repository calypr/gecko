package server

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMakeRouterConfiguresLargeRequestBodyLimit(t *testing.T) {
	app := NewServer().WithLogger(log.New(io.Discard, "", 0)).MakeRouter()

	const wantBodyLimit = 50 * 1024 * 1024
	if got := app.Config().BodyLimit; got != wantBodyLimit {
		t.Fatalf("BodyLimit = %d, want %d", got, wantBodyLimit)
	}
}

// TestMakeRouterRoutesThroughGeneratedOpenAPI proves the full fiber app
// (adaptor -> generated mux -> strict handler) serves a spec operation end to
// end after the cutover away from the legacy fiber handlers.
func TestMakeRouterRoutesThroughGeneratedOpenAPI(t *testing.T) {
	app := NewServer().WithLogger(log.New(io.Discard, "", 0)).MakeRouter()

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/config/types", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for /config/types, got %d", resp.StatusCode)
	}
	var types []string
	if err := json.NewDecoder(resp.Body).Decode(&types); err != nil {
		t.Fatalf("decode config types: %v", err)
	}
	if len(types) == 0 {
		t.Fatalf("expected a non-empty list of config types")
	}
}
