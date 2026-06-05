package middleware

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/gofiber/fiber/v3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type fakeJWTAllowedResourceHandler struct {
	resources []any
}

func (handler fakeJWTAllowedResourceHandler) GetAllowedResources(_ string, _ string, _ string) ([]any, error) {
	return handler.resources, nil
}

func (handler fakeJWTAllowedResourceHandler) CheckResourceServiceAccess(_ string, _ string, _ string, resourcePath string) (bool, error) {
	for _, resource := range handler.resources {
		if value, ok := resource.(string); ok && value == resourcePath {
			return true, nil
		}
	}
	return false, nil
}

func TestGitProjectAuthAllowsProgramProjectResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Get("/git/projects/:orgTitle/:projectTitle", GitProjectAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a/projects/proj-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("GET", "/git/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func buildUnverifiedJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	headerBody, err := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(headerBody) + "." + base64.RawURLEncoding.EncodeToString(body) + ".sig"
}

func TestGitProjectAuthRejectsLegacyOrganizationResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Get("/git/projects/:orgTitle/:projectTitle", GitProjectAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/organization/org-a/project/proj-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("GET", "/git/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestGitOrganizationAuthAllowsProjectResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", GitOrganizationAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/init-connect", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestGitOrganizationAuthRejectsLegacyOrganizationResourcePath(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", GitOrganizationAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/organization/org-a/project/proj-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/init-connect", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestRequireAuthorizationRejectsMissingHeader(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", RequireAuthorization(logger), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/init-connect", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestRequireAuthorizationAllowsBearerHeader(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/connect", RequireAuthorization(logger), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/projects/org-a/proj-a/connect", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestParseResourceAccessSnapshotPrefersAuthzBlock(t *testing.T) {
	snapshot, err := parseResourceAccessSnapshot(map[string]any{
		"authz": map[string]any{
			"/programs/org-a": []any{
				map[string]any{"service": "arborist", "method": "manage-owners"},
			},
			"/programs/org-a/projects/proj-a": []any{
				map[string]any{"service": "*", "method": "read"},
				map[string]any{"service": "*", "method": "update"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a", "manage-owners", "arborist") {
		t.Fatal("expected org manage-owners access to be present")
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "update", "*") {
		t.Fatal("expected project update access to be present")
	}
	if ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-b", "update", "*") {
		t.Fatal("did not expect unrelated project access")
	}
}

func TestFenceUserAccessHandlerGetResourceAccessUsesProjectAccessFallback(t *testing.T) {
	responseBody, err := json.Marshal(map[string]any{
		"project_access": map[string]any{
			"/programs/org-a/projects/proj-a": []any{
				map[string]any{"service": "*", "method": "read"},
				map[string]any{"service": "*", "method": "update"},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal response body: %v", err)
	}
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(responseBody)),
			}, nil
		}),
	}

	token := "Bearer " + buildUnverifiedJWT(t, map[string]any{"iss": "https://example.test"})
	handler := NewFenceUserAccessHandler(client)
	snapshot, err := handler.GetResourceAccess(token)
	if err != nil {
		t.Fatalf("unexpected access lookup error: %v", err)
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "read", "*") {
		t.Fatal("expected read access from project_access fallback")
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "update", "*") {
		t.Fatal("expected update access from project_access fallback")
	}
}

func TestParseResourceAccessSnapshotRejectsMissingBlocks(t *testing.T) {
	_, err := parseResourceAccessSnapshot(map[string]any{})
	var accessErr *AccessError
	if !errors.As(err, &accessErr) {
		t.Fatalf("expected AccessError, got %T", err)
	}
	if accessErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502-style access error, got %d", accessErr.StatusCode)
	}
}

func TestProjectConfigAuthAllowsExactProjectResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Delete("/config/projects/:orgTitle/:projectTitle", ProjectConfigAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a/projects/proj-a"}}, "delete"), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("DELETE", "/config/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestProjectConfigAuthRejectsDifferentProjectResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Delete("/config/projects/:orgTitle/:projectTitle", ProjectConfigAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a/projects/other"}}, "delete"), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("DELETE", "/config/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestProjectConfigAuthAllowsAdminWildcardResource(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Delete("/config/projects/:orgTitle/:projectTitle", ProjectConfigAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"*"}}, "delete"), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("DELETE", "/config/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestSnapshotAllowsAcceptsWildcardMethod(t *testing.T) {
	raw := []any{
		map[string]any{
			"method":  "*",
			"service": "*",
		},
	}

	if !snapshotAllows(raw, "delete", "*") {
		t.Fatalf("expected wildcard method/service snapshot entry to allow delete")
	}
}

func TestGitProjectAuthForbiddenIncludesRequestAccessDetails(t *testing.T) {
	logger := &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
	app := fiber.New()
	app.Get("/git/projects/:orgTitle/:projectTitle", GitProjectAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a/projects/other"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("GET", "/git/projects/org-a/proj-a", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app.Test error: %v", err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	var payload struct {
		Error struct {
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload.Error.Details["request_access"]; got != true {
		t.Fatalf("expected request_access=true, got %#v", got)
	}
	if got := payload.Error.Details["request_access_resource_path"]; got != "/programs/org-a/projects/proj-a" {
		t.Fatalf("unexpected request_access_resource_path: %#v", got)
	}
}
