package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bmeg/grip-graphql/middleware"
	geckologging "github.com/calypr/gecko/internal/logging"
	server "github.com/calypr/gecko/internal/server"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
)

type MockJWTHandler struct {
	AllowedResources []string
	Err              error
}

func (m *MockJWTHandler) GetAllowedResources(token string, method, service string) ([]any, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	out := make([]any, len(m.AllowedResources))
	for i, s := range m.AllowedResources {
		out[i] = s
	}
	return out, nil
}

func (m *MockJWTHandler) CheckResourceServiceAccess(token, resource, service, method string) (bool, error) {
	if m.Err != nil {
		return false, m.Err
	}
	for _, r := range m.AllowedResources {
		if r == resource {
			return true, nil
		}
	}
	return false, nil
}

func setupServer() *server.Server {
	return &server.Server{Logger: &geckologging.Handler{Logger: log.New(os.Stdout, "", log.Ldate|log.Ltime)}}
}

func runFiber(app *fiber.App, req *http.Request, t *testing.T) (*http.Response, string) {
	t.Helper()
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0, FailOnTimeout: false})
	if err != nil {
		t.Fatalf("fiber test request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(body)
}

func TestGeneralAuthMware_NoAuthorization(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandler{}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, body := runFiber(app, httptest.NewRequest(http.MethodGet, "/ohsu-test", nil), t)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, body, "Authorization token not provided")
}

func TestGeneralAuthMware_BadProjectID(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/ohsu/projects/test"}}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/ohsu", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, body, "Failed to parse request body")
}

func TestGeneralAuthMware_GetAllowedResourcesNonServerError(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandler{Err: fmt.Errorf("generic error")}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/ohsu-test", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, body, "expecting error to be serverError type")
}

func TestGeneralAuthMware_GetAllowedResourcesServerError(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandler{Err: &middleware.ServerError{Message: "token expired", StatusCode: http.StatusUnauthorized}}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/ohsu-test", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, body, "token expired")
}

type MockJWTHandlerBadAny struct{}

func (m *MockJWTHandlerBadAny) GetAllowedResources(token string, method, service string) ([]any, error) {
	return []any{123}, nil
}
func (m *MockJWTHandlerBadAny) CheckResourceServiceAccess(token, resource, service, method string) (bool, error) {
	return true, nil
}

func TestGeneralAuthMware_ConvertAnyToStringSliceError(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandlerBadAny{}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/ohsu-test", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, body, "Element 123 is not a string")
}

func TestGeneralAuthMware_ParseAccessDenied(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/:projectId", servermw.GeneralAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/other/projects/test"}}, "read", "*"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/ohsu-test", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Contains(t, body, "User is not allowed to read on resource path")
}

func TestConfigAuthMiddleware_MethodNotAllowed(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Use("/config/explorer/:configId", func(c fiber.Ctx) error { c.Locals("configType", "explorer"); return c.Next() })
	app.Patch("/config/explorer/:configId", servermw.ConfigAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPatch, "/config/explorer/ohsu-test", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Contains(t, body, "Unsupported HTTP method")
}

func TestConfigAuthMiddleware_AppsPage_PublicGET(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Use("/config/apps_page/:configId", func(c fiber.Ctx) error { c.Locals("configType", "apps_page"); return c.Next() })
	app.Get("/config/apps_page/:configId", servermw.ConfigAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, _ := runFiber(app, httptest.NewRequest(http.MethodGet, "/config/apps_page/default", nil), t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestConfigAuthMiddleware_Nav_PublicGET(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Use("/config/nav/:configId", func(c fiber.Ctx) error { c.Locals("configType", "nav"); return c.Next() })
	app.Get("/config/nav/:configId", servermw.ConfigAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, _ := runFiber(app, httptest.NewRequest(http.MethodGet, "/config/nav/default", nil), t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestBaseConfigsAuthMiddleware_NoAuthorization(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/", servermw.BaseConfigsAuth(srv.Logger, &MockJWTHandler{}, "read", "*", "/programs"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, body := runFiber(app, httptest.NewRequest(http.MethodGet, "/", nil), t)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, body, "Authorization token not provided")
}

func TestBaseConfigsAuthMiddleware_InvalidJWTHandler(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/", servermw.BaseConfigsAuth(srv.Logger, &MockJWTHandler{}, "read", "*", "/programs"), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, body, "Invalid JWT handler configuration")
}

func TestAppCardAuthMiddleware_NoAuthorization(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, body := runFiber(app, httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/TEST-PROJECT", nil), t)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, body, "Authorization token not provided")
}

func TestAppCardAuthMiddleware_GET_Success(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/TEST/projects/PROJECT"}}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/TEST-PROJECT", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, _ := runFiber(app, req, t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAppCardAuthMiddleware_GET_Denied(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Get("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/other/projects/wrong"}}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/TEST-PROJECT", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Contains(t, body, "User is not allowed to read on resource path")
}

func TestAppCardAuthMiddleware_POST_MissingPermsInBody(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Post("/config/apps_page/appcard", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard", bytes.NewBufferString(`{"title":"Test"}`))
	req.Header.Set("Authorization", "Bearer dummy")
	req.Header.Set("Content-Type", "application/json")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, body, "Missing or empty projectId")
}

func TestAppCardAuthMiddleware_POST_Success(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Post("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/TEST/projects/PROJECT"}}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/TEST-PROJECT", bytes.NewBufferString(`{"title":"Explore TEST","perms":"TEST-PROJECT"}`))
	req.Header.Set("Authorization", "Bearer dummy")
	req.Header.Set("Content-Type", "application/json")
	resp, _ := runFiber(app, req, t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAppCardAuthMiddleware_POST_Denied(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Post("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/other/projects/wrong"}}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/TEST-PROJECT", bytes.NewBufferString(`{"title":"Explore TEST","perms":"TEST-PROJECT"}`))
	req.Header.Set("Authorization", "Bearer dummy")
	req.Header.Set("Content-Type", "application/json")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Contains(t, body, "User is not allowed to create on resource path")
}

func TestAppCardAuthMiddleware_DELETE_Success(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Delete("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{AllowedResources: []string{"/programs/TEST/projects/PROJECT"}}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodDelete, "/config/apps_page/appcard/TEST-PROJECT", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, _ := runFiber(app, req, t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAppCardAuthMiddleware_UnsupportedMethod(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Patch("/config/apps_page/appcard/:projectId", servermw.AppCardAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPatch, "/config/apps_page/appcard/something", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	resp, body := runFiber(app, req, t)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Contains(t, body, "Unsupported HTTP method")
}

func TestConfigAuthMiddleware_Project_PublicGET(t *testing.T) {
	srv := setupServer()
	app := fiber.New()
	app.Use("/config/project/:configId", func(c fiber.Ctx) error { c.Locals("configType", "project"); return c.Next() })
	app.Get("/config/project/:configId", servermw.ConfigAuth(srv.Logger, &MockJWTHandler{}), func(c fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	resp, _ := runFiber(app, httptest.NewRequest(http.MethodGet, "/config/project/default", nil), t)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
