package middleware

import (
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

func TestGitProjectAuthAllowsOrganizationResource(t *testing.T) {
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
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestGitOrganizationAuthAllowsProjectResource(t *testing.T) {
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
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
