package health

import (
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

func TestHealthRequiresDatabase(t *testing.T) {
	app := fiber.New()
	RegisterRoutes(app, &shared.Handler{Logger: &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}})

	response, err := app.Test(httptest.NewRequest(http.MethodGet, "/health", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestHealthChecksDatabase(t *testing.T) {
	for _, test := range []struct {
		name   string
		ping   error
		status int
	}{
		{name: "available", status: http.StatusOK},
		{name: "unavailable", ping: errors.New("database down"), status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectPing().WillReturnError(test.ping)
			app := fiber.New()
			RegisterRoutes(app, &shared.Handler{
				DB:     sqlx.NewDb(db, "sqlmock"),
				Logger: &geckologging.Handler{Logger: log.New(io.Discard, "", 0)},
			})
			response, err := app.Test(httptest.NewRequest(http.MethodGet, "/health", nil))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.status)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
