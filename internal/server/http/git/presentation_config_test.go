package git

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/calypr/gecko/config"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/presentation"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

type fakePresentationAccessHandler struct {
	resources []any
}

func (handler fakePresentationAccessHandler) GetAllowedResources(_ string, _ string, _ string) ([]any, error) {
	return handler.resources, nil
}

func (handler fakePresentationAccessHandler) CheckResourceServiceAccess(_ string, _ string, _ string, resourcePath string) (bool, error) {
	for _, resource := range handler.resources {
		if value, ok := resource.(string); ok && value == resourcePath {
			return true, nil
		}
	}
	return false, nil
}

func newPresentationConfigTestServer(t *testing.T) (*Handler, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	handler := &Handler{
		db:                sqlx.NewDb(db, "sqlmock"),
		logger:            &geckologging.Handler{Logger: log.New(os.Stdout, "", 0)},
		presentationStore: presentation.NewFilesystemStore(t.TempDir()),
	}
	return handler, mock, func() { _ = db.Close() }
}

func newPresentationConfigApp(handler *Handler, authz servermw.ResourceAccessHandler) *fiber.App {
	app := fiber.New()
	app.Get("/git/projects/:orgTitle/:projectTitle/presentationConfig", servermw.ProjectConfigAuth(handler.logger, authz, "read"), handler.handleGitProjectPresentationConfigGET)
	app.Put("/git/projects/:orgTitle/:projectTitle/presentationConfig", servermw.ProjectConfigAuth(handler.logger, authz, "update"), handler.handleGitProjectPresentationConfigPUT)
	app.Post("/git/projects/:orgTitle/:projectTitle/presentationConfig", servermw.ProjectConfigAuth(handler.logger, authz, "update"), handler.handleGitProjectPresentationConfigPUT)
	return app
}

func runPresentationConfigRequest(t *testing.T, app *fiber.App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0, FailOnTimeout: false})
	if err != nil {
		t.Fatalf("fiber test request failed: %v", err)
	}
	return resp
}

func expectProjectLookup(mock sqlmock.Sqlmock, organization string, project string) {
	projectCfg := appconfig.ProjectConfig{
		Title:        project,
		ContactEmail: "owner@example.org",
		SrcRepo:      "github.com/example/" + project,
		OrgTitle:     organization,
		Description:  "project",
		ProjectTitle: project,
	}
	content, _ := json.Marshal(projectCfg)
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs(organization + "/" + project).
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow(organization+"/"+project, content))
}

func decodePresentationResponse(t *testing.T, body io.Reader) appconfig.PresentationConfig {
	t.Helper()
	var payload appconfig.PresentationConfig
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return payload
}

func TestGitProjectPresentationConfigGETNoConfigFallsBackToEmpty(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()
	expectProjectLookup(mock, "org-a", "proj-a")

	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/proj-a"}})
	req := httptest.NewRequest(http.MethodGet, "/git/projects/org-a/proj-a/presentationConfig", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	payload := decodePresentationResponse(t, resp.Body)
	if payload.PresentationConfig != "" {
		t.Fatalf("expected empty presentationConfig, got %q", payload.PresentationConfig)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectPresentationConfigPUTSanitizesAndPersistsHTML(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()
	expectProjectLookup(mock, "org-a", "proj-a")

	requestPayload := appconfig.PresentationConfig{
		PresentationConfig: `<div onclick="bad()"><script>alert(1)</script><p>Hello</p></div>`,
	}
	requestBody, _ := json.Marshal(requestPayload)
	expectedStored := `<div><p>Hello</p></div>`

	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/proj-a"}})
	req := httptest.NewRequest(http.MethodPut, "/git/projects/org-a/proj-a/presentationConfig", bytes.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	payload := decodePresentationResponse(t, resp.Body)
	if payload.PresentationConfig != expectedStored {
		t.Fatalf("unexpected sanitized response: %q", payload.PresentationConfig)
	}
	data, err := os.ReadFile(handler.presentationStore.ProjectPresentationPath("org-a", "proj-a"))
	if err != nil {
		t.Fatalf("read stored presentation: %v", err)
	}
	if string(data) != expectedStored {
		t.Fatalf("unexpected stored HTML: %q", string(data))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectPresentationConfigPOSTUsesSameWritePath(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()
	expectProjectLookup(mock, "org-a", "proj-a")

	requestPayload := appconfig.PresentationConfig{PresentationConfig: `<p>Hello</p>`}
	requestBody, _ := json.Marshal(requestPayload)

	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/proj-a"}})
	req := httptest.NewRequest(http.MethodPost, "/git/projects/org-a/proj-a/presentationConfig", bytes.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if filepath.Base(handler.presentationStore.ProjectPresentationPath("org-a", "proj-a")) != "proj-a_presentation.html" {
		t.Fatalf("unexpected presentation filename: %q", filepath.Base(handler.presentationStore.ProjectPresentationPath("org-a", "proj-a")))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectPresentationConfigPUTRejectsMalformedHTML(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()
	expectProjectLookup(mock, "org-a", "proj-a")

	requestBody := []byte(`{"presentationConfig":"<div><p>broken</div>"}`)
	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/proj-a"}})
	req := httptest.NewRequest(http.MethodPut, "/git/projects/org-a/proj-a/presentationConfig", bytes.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectPresentationConfigPUTRejectsUnknownFields(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()
	expectProjectLookup(mock, "org-a", "proj-a")

	requestBody := []byte(`{"presentationConfig":"<p>Hello</p>","extra":"nope"}`)
	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/proj-a"}})
	req := httptest.NewRequest(http.MethodPut, "/git/projects/org-a/proj-a/presentationConfig", bytes.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectPresentationConfigRouteRequiresProjectAccess(t *testing.T) {
	handler, mock, cleanup := newPresentationConfigTestServer(t)
	defer cleanup()

	app := newPresentationConfigApp(handler, fakePresentationAccessHandler{resources: []any{"/programs/org-a/projects/other"}})
	req := httptest.NewRequest(http.MethodGet, "/git/projects/org-a/proj-a/presentationConfig", nil)
	req.Header.Set("Authorization", "Bearer test")
	resp := runPresentationConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
