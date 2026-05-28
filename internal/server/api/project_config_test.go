package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/config"
	geckologging "github.com/calypr/gecko/internal/logging"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

func newProjectConfigTestServer(t *testing.T) (*Handler, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	srv := &Handler{
		db:     sqlx.NewDb(db, "sqlmock"),
		logger: &geckologging.Handler{Logger: log.New(os.Stdout, "", 0)},
	}
	return srv, mock, func() { _ = db.Close() }
}

func runProjectConfigRequest(t *testing.T, app *fiber.App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0, FailOnTimeout: false})
	if err != nil {
		t.Fatalf("fiber test request failed: %v", err)
	}
	return resp
}

func TestProjectConfigListGET_PluralProjects(t *testing.T) {
	srv, mock, cleanup := newProjectConfigTestServer(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"name"}).AddRow("HTAN_INT/BForePC")
	mock.ExpectQuery(`SELECT name FROM config_schema\.projects`).WillReturnRows(rows)

	app := fiber.New()
	projects := app.Group("/config/projects", withConfigType(string(config.TypeProjects)))
	projects.Get("/list", srv.handleConfigListGET)

	resp := runProjectConfigRequest(t, app, httptest.NewRequest(http.MethodGet, "/config/projects/list", nil))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestProjectConfigGET_ByOrganizationAndProject(t *testing.T) {
	srv, mock, cleanup := newProjectConfigTestServer(t)
	defer cleanup()

	project := config.ProjectConfig{
		Title:        "BForePC",
		ContactEmail: "sanati@ohsu.edu",
		SrcRepo:      "https://source.ohsu.edu/CBDS/BForePC.git",
		OrgTitle:     "HTAN_INT",
		Description:  "BForePC collaboration",
		ProjectTitle: "BForePC",
		IconName:     "binoculars",
	}
	content, err := json.Marshal(project)
	if err != nil {
		t.Fatalf("failed to marshal project fixture: %v", err)
	}

	rows := sqlmock.NewRows([]string{"name", "content"}).AddRow("HTAN_INT/BForePC", content)
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("HTAN_INT/BForePC").
		WillReturnRows(rows)

	app := fiber.New()
	projects := app.Group("/config/projects", withConfigType(string(config.TypeProjects)))
	projects.Get("/:orgTitle/:projectTitle", servermw.ConfigAuth(srv.logger, nil), srv.handleProjectConfigGET)

	resp := runProjectConfigRequest(t, app, httptest.NewRequest(http.MethodGet, "/config/projects/HTAN_INT/BForePC", nil))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestProjectConfigPUT_ByOrganizationAndProject(t *testing.T) {
	srv, mock, cleanup := newProjectConfigTestServer(t)
	defer cleanup()
	originalValidator := config.ValidateProjectRepository
	config.ValidateProjectRepository = func(_ context.Context, raw string) (string, error) {
		return raw, nil
	}
	defer func() {
		config.ValidateProjectRepository = originalValidator
	}()

	project := config.ProjectConfig{
		Title:        "BForePC",
		ContactEmail: "sanati@ohsu.edu",
		SrcRepo:      "github.com/example/BForePC",
		OrgTitle:     "HTAN_INT",
		Description:  "BForePC collaboration",
		ProjectTitle: "BForePC",
		IconName:     "binoculars",
	}

	content, err := json.Marshal(project)
	if err != nil {
		t.Fatalf("failed to marshal project fixture: %v", err)
	}

	mock.ExpectExec(`INSERT INTO config_schema\.projects`).
		WithArgs("HTAN_INT/BForePC", content).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE config_schema\.git_pending_repository`).
		WithArgs("github.com", "example", "BForePC").
		WillReturnResult(sqlmock.NewResult(0, 0))

	app := fiber.New()
	projects := app.Group("/config/projects", withConfigType(string(config.TypeProjects)))
	projects.Put("/:orgTitle/:projectTitle", srv.handleProjectConfigPUT)

	req := httptest.NewRequest(http.MethodPut, "/config/projects/HTAN_INT/BForePC", bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/json")
	resp := runProjectConfigRequest(t, app, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
