package server

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func runFiberTest(t *testing.T, app *fiber.App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0, FailOnTimeout: false})
	if err != nil {
		t.Fatalf("fiber test request failed: %v", err)
	}
	return resp
}

func newTestServer(t *testing.T) (*Server, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening mock db: %s", err)
	}
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	srv := &Server{db: sqlxDB, Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)}}
	cleanup := func() { _ = db.Close() }
	return srv, mock, cleanup
}

func TestHandleAppCardPOST_Update(t *testing.T) {
	srv, mock, cleanup := newTestServer(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"name", "content"}).AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Old Title"}]}`))
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").WithArgs("1").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO config_schema.apps_page").WithArgs("1", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	app := fiber.New()
	app.Post("/config/apps_page/appcard/:projectId", srv.handleAppCardPOST)
	resp := runFiberTest(t, app, httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/PROG-PROJ", bytes.NewBufferString(`{"perms": "PROG-PROJ", "title": "New Title"}`)))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleAppCardDELETE_Success(t *testing.T) {
	srv, mock, cleanup := newTestServer(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"name", "content"}).AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Title"}]}`))
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").WithArgs("1").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO config_schema.apps_page").WithArgs("1", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	app := fiber.New()
	app.Delete("/config/apps_page/appcard/:projectId", srv.handleAppCardDELETE)
	resp := runFiberTest(t, app, httptest.NewRequest(http.MethodDelete, "/config/apps_page/appcard/PROG-PROJ", nil))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleAppCardPOST_Integration(t *testing.T) {
	srv, mock, cleanup := newTestServer(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"name", "content"}).AddRow("1", []byte(`{"appCards": []}`))
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").WithArgs("1").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO config_schema.apps_page").WithArgs("1", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	app := fiber.New()
	app.Post("/config/apps_page/appcard/:projectId", func(c fiber.Ctx) error {
		if c.Params("projectId") == "" {
			return c.SendStatus(http.StatusBadRequest)
		}
		return c.Next()
	}, srv.handleAppCardPOST)

	resp := runFiberTest(t, app, httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/PROG-PROJ", bytes.NewBufferString(`{"perms": "PROG-PROJ", "title": "New Title"}`)))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleAppCardGET_Success(t *testing.T) {
	srv, mock, cleanup := newTestServer(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"name", "content"}).AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Title"}]}`))
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").WithArgs("1").WillReturnRows(rows)

	app := fiber.New()
	app.Get("/config/apps_page/appcard/:projectId", srv.handleAppCardGET)
	resp := runFiberTest(t, app, httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/PROG-PROJ", nil))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, mock.ExpectationsWereMet())
}
