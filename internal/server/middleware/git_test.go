package middleware

import (
	"encoding/json"
	"io"
	"log"
	"net/http/httptest"
	"testing"

	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/gofiber/fiber/v3"
)

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
	app.Post("/git/organizations/:orgTitle/connect", GitOrganizationAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/programs/org-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/connect", nil)
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
	app.Post("/git/organizations/:orgTitle/connect", GitOrganizationAuth(logger, fakeJWTAllowedResourceHandler{resources: []any{"/organization/org-a/project/proj-a"}}), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/connect", nil)
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
	app.Post("/git/organizations/:orgTitle/connect", RequireAuthorization(logger), func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/git/organizations/org-a/connect", nil)
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
