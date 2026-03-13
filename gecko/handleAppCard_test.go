package gecko

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/iris/v12"
	"github.com/stretchr/testify/assert"
)

func TestHandleAppCardPOST_Update(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	srv := &Server{
		db:     sqlxDB,
		Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)},
	}

	// Initial state: one card in config ID '1'
	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Old Title"}]}`))

	// The handler now uses ID '1'
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("1").
		WillReturnRows(rows)

	// Expect UPDATE (Upsert) back to ID '1'
	mock.ExpectExec("INSERT INTO config_schema.apps_page").
		WithArgs("1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	body := `{"perms": "PROG-PROJ", "title": "New Title"}`
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/PROG-PROJ", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "PROG-PROJ")

	srv.handleAppCardPOST(ctx)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "added or updated")
	assert.Contains(t, rec.Body.String(), "perms PROG-PROJ")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestHandleAppCardDELETE_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening mock db: %s", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	srv := &Server{
		db:     sqlxDB,
		Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)},
	}

	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Title"}]}`))

	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("1").
		WillReturnRows(rows)

	mock.ExpectExec("INSERT INTO config_schema.apps_page").
		WithArgs("1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	req := httptest.NewRequest(http.MethodDelete, "/config/apps_page/appcard/PROG-PROJ", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "PROG-PROJ")

	srv.handleAppCardDELETE(ctx)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "deleted")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestHandleAppCardPOST_Integration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	srv := &Server{
		db:     sqlxDB,
		Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)},
	}

	// Mock for initial config read
	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("1", []byte(`{"appCards": []}`))

	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("1").
		WillReturnRows(rows)

	// Mock for final config save
	mock.ExpectExec("INSERT INTO config_schema.apps_page").
		WithArgs("1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	app := iris.New()

	// Using the actual middleware pattern (simple projectId from Param)
	app.Post("/config/apps_page/appcard/{projectId}", func(ctx iris.Context) {
		projectId := ctx.Params().Get("projectId")
		if projectId == "" {
			ctx.StatusCode(400)
			return
		}
		ctx.Next()
	}, srv.handleAppCardPOST)

	if err := app.Build(); err != nil {
		t.Fatalf("Failed to build iris app: %v", err)
	}

	body := `{"perms": "PROG-PROJ", "title": "New Title"}`
	// New URL format: /config/apps_page/appcard/{projectId}
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard/PROG-PROJ", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	app.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "added or updated")
	assert.Contains(t, rec.Body.String(), "perms PROG-PROJ")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestHandleAppCardGET_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening mock db: %s", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	srv := &Server{
		db:     sqlxDB,
		Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)},
	}

	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("1", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Title"}]}`))

	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("1").
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/PROG-PROJ", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "PROG-PROJ")

	srv.handleAppCardGET(ctx)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "PROG-PROJ")
	assert.Contains(t, rec.Body.String(), "Title")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}
