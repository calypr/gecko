package apiserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/gecko/internal/access"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
)

func newTestApp(t *testing.T) *fiber.App {
	t.Helper()
	impl := &Server{Logger: &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}}
	app := fiber.New()
	app.Use(adaptor.HTTPHandler(NewRouter(impl)))
	return app
}

func TestOpenAPIRouterServesConfigTypes(t *testing.T) {
	app := newTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/config/types", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var types []string
	if err := json.NewDecoder(resp.Body).Decode(&types); err != nil {
		t.Fatalf("decode config types: %v", err)
	}
	found := false
	for _, item := range types {
		if item == "projects" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected config types to include \"projects\", got %v", types)
	}
}

func TestOpenAPIRouterRejectsUnknownConfigType(t *testing.T) {
	app := newTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/config/list?type=bogus", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var body gecko.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Type == nil || *body.Error.Type != "invalid_config_type" {
		t.Fatalf("expected error type invalid_config_type, got %v", body.Error.Type)
	}
	if body.Error.Code == nil || *body.Error.Code != http.StatusBadRequest {
		t.Fatalf("expected error code 400 mirroring the status, got %v", body.Error.Code)
	}
}

func TestOpenAPIRouterRequiresTokenForGitProjects(t *testing.T) {
	app := newTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/git/projects", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a token, got %d", resp.StatusCode)
	}
}

func TestUpgradeWildcardPattern(t *testing.T) {
	cases := map[string]string{
		"GET /git/projects/{orgTitle}/{projectTitle}/tree/{path}":     "GET /git/projects/{orgTitle}/{projectTitle}/tree/{path...}",
		"GET /git/projects/{orgTitle}/{projectTitle}/file/{path}":     "GET /git/projects/{orgTitle}/{projectTitle}/file/{path...}",
		"GET /health":                                                 "GET /health",
		"GET /git/projects/{orgTitle}/{projectTitle}/tree":            "GET /git/projects/{orgTitle}/{projectTitle}/tree",
	}
	for in, want := range cases {
		if got := upgradeWildcardPattern(in); got != want {
			t.Fatalf("upgradeWildcardPattern(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNestedSubpathRoutesMatch confirms the multi-segment {path...} upgrade lets
// nested file/tree paths reach a handler instead of 404ing at the mux. A non-nil
// authz makes an unauthenticated request 401 in the handler (proving the route
// matched) rather than panicking on nil services; an unmatched route would 404.
func TestNestedSubpathRoutesMatch(t *testing.T) {
	impl := &Server{
		Logger: &geckologging.Handler{Logger: log.New(io.Discard, "", 0)},
		Authz:  access.NewFenceUserAccessHandler(nil),
	}
	app := fiber.New()
	app.Use(adaptor.HTTPHandler(NewRouter(impl)))

	for _, path := range []string{
		"/git/projects/org/proj/file/src/pkg/main.go",
		"/git/projects/org/proj/tree/a/b/c",
		"/git/projects/org/proj/manifest/x/y",
		"/git/projects/org/proj/download/d/e.bin",
	} {
		resp, err := app.Test(httptest.NewRequest("GET", path, nil))
		if err != nil {
			t.Fatalf("unexpected app.Test error: %v", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			t.Fatalf("nested subpath %q was not routed (got 404); wildcard upgrade failed", path)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 (route matched, no token) for %q, got %d", path, resp.StatusCode)
		}
	}
}

// TestBindingErrorReturnsJSONEnvelope confirms a query-binding failure (a
// non-integer limit) is reported through the JSON error envelope rather than the
// std-http default text/plain 400, matching the legacy error format.
func TestBindingErrorReturnsJSONEnvelope(t *testing.T) {
	app := newTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/git/projects/org/proj/storage/children?limit=abc", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-integer limit, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json error body, got %q", ct)
	}
	var body gecko.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Type == nil || *body.Error.Type != "invalid_request" {
		t.Fatalf("expected error type invalid_request, got %v", body.Error.Type)
	}
}

// TestDocEndpointServesFullSpec guards the original documentation gap: the
// served contract must be the full generated spec, not a stale 4-path stub.
func TestDocEndpointServesFullSpec(t *testing.T) {
	app := newTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/swagger/doc.json", nil))
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for doc endpoint, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json spec, got %q", ct)
	}
	var spec map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		t.Fatalf("decode spec: %v", err)
	}
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		t.Fatalf("spec has no paths object")
	}
	if len(paths) < 40 {
		t.Fatalf("expected the full spec (40+ paths), got %d", len(paths))
	}
	for _, p := range []string{
		"/git/projects/{orgTitle}/{projectTitle}/tree/{path}",
		"/config/projects",
		"/health",
	} {
		if _, ok := paths[p]; !ok {
			t.Fatalf("served spec missing path %q", p)
		}
	}
}

func TestInjectAuthorizationCapturesHeader(t *testing.T) {
	var captured string
	next := func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
		captured = AuthorizationFromContext(ctx)
		return nil, nil
	}
	wrapped := InjectAuthorization(next, "test")
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if _, err := wrapped(context.Background(), nil, req, nil); err != nil {
		t.Fatalf("unexpected middleware error: %v", err)
	}
	if captured != "Bearer abc" {
		t.Fatalf("expected captured token \"Bearer abc\", got %q", captured)
	}
}
