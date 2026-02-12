package gecko

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/gecko/config"
	"github.com/jmoiron/sqlx"
	"github.com/kataras/iris/v12"
	"github.com/stretchr/testify/assert"
)

func TestHandleAppCardGET_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	// Create a server instance with the mock DB and initialized Logger
	srv := &Server{
		db:     sqlxDB,
		Logger: &LogHandler{Logger: log.New(os.Stdout, "", 0)},
	}

	// Create query response for AppCards config
	// The handler calls configGETGeneric which does a SELECT
	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("default", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Test Card"}]}`))

	// Using regex to match the query in sql.go: SELECT name, content FROM config_schema.apps_page WHERE name=$1
	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("default").
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/PROG-PROJ", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "PROG-PROJ")

	srv.handleAppCardGET(ctx)

	assert.Equal(t, http.StatusOK, rec.Code)

	var card config.AppCard
	json.Unmarshal(rec.Body.Bytes(), &card)
	assert.Equal(t, "PROG-PROJ", card.Perms)
	assert.Equal(t, "Test Card", card.Title)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestHandleAppCardGET_NotFound(t *testing.T) {
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

	// Case 1: Config exists but card not found
	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("default", []byte(`{"appCards": [{"perms": "OTHER-PROJ"}]}`))

	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("default").
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/config/apps_page/appcard/PROG-PROJ", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "PROG-PROJ")

	srv.handleAppCardGET(ctx)

	// Should be 404 because card is not in the list
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

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

	// Initial state: one card
	rows := sqlmock.NewRows([]string{"name", "content"}).
		AddRow("default", []byte(`{"appCards": [{"perms": "PROG-PROJ", "title": "Old Title"}]}`))

	mock.ExpectQuery("^SELECT name, content FROM config_schema.apps_page WHERE name=.+").
		WithArgs("default").
		WillReturnRows(rows)

	// Expect UPDATE with modified data
	mock.ExpectExec("INSERT INTO config_schema.apps_page").
		WithArgs("default", sqlmock.AnyArg()). // We can't easily match JSON blob exactly without regex, AnyArg is safer for smoke test
		WillReturnResult(sqlmock.NewResult(1, 1))

	body := `{"perms": "PROG-PROJ", "title": "New Title"}`
	req := httptest.NewRequest(http.MethodPost, "/config/apps_page/appcard", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)

	srv.handleAppCardPOST(ctx)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "added or updated")
}
