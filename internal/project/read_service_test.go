package project

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/internal/domain"
	"github.com/jmoiron/sqlx"
)

func TestReadServiceResolve(t *testing.T) {
	t.Run("rejects missing path", func(t *testing.T) {
		service := NewReadService(nil, nil)
		_, err := service.Resolve(context.Background(), "", "")
		assertDomainError(t, err, "invalid_request", http.StatusBadRequest)
	})

	t.Run("returns not found when config is missing", func(t *testing.T) {
		db, mock, cleanup := newReadServiceDB(t)
		defer cleanup()
		mock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("ORG/PROJECT").
			WillReturnError(sql.ErrNoRows)

		service := NewReadService(db, nil)
		_, err := service.Resolve(context.Background(), "ORG", "PROJECT")
		assertDomainError(t, err, "not_found", http.StatusNotFound)
	})

	t.Run("returns conflict when GitHub is not connected", func(t *testing.T) {
		db, mock, cleanup := newReadServiceDB(t)
		defer cleanup()
		mock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("ORG/PROJECT").
			WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("ORG/PROJECT", []byte(`{}`)))

		service := NewReadService(db, nil)
		_, err := service.Resolve(context.Background(), "ORG", "PROJECT")
		assertDomainError(t, err, "conflict", http.StatusConflict)
	})

	t.Run("returns validation failed for an invalid repository", func(t *testing.T) {
		db, mock, cleanup := newReadServiceDB(t)
		defer cleanup()
		mock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("ORG/PROJECT").
			WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("ORG/PROJECT", []byte(`{"src_repo":"not a repository"}`)))

		service := NewReadService(db, nil)
		_, err := service.Resolve(context.Background(), "ORG", "PROJECT")
		assertDomainError(t, err, "validation_failed", http.StatusBadRequest)
	})

	t.Run("returns the resolved project and identity", func(t *testing.T) {
		db, mock, cleanup := newReadServiceDB(t)
		defer cleanup()
		mock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("ORG/PROJECT").
			WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("ORG/PROJECT", []byte(`{"src_repo":"https://github.com/owner/repo"}`)))

		service := NewReadService(db, nil)
		resolved, err := service.Resolve(context.Background(), " ORG ", " PROJECT ")
		if err != nil {
			t.Fatalf("resolve project: %v", err)
		}
		if resolved.ID != "ORG/PROJECT" || resolved.Organization != "ORG" || resolved.Name != "PROJECT" {
			t.Fatalf("unexpected project identity: %+v", resolved)
		}
		if resolved.Identity.Host != "github.com" || resolved.Identity.Owner != "owner" || resolved.Identity.Repo != "repo" {
			t.Fatalf("unexpected repository identity: %+v", resolved.Identity)
		}
		if resolved.Config.SrcRepo != "https://github.com/owner/repo" {
			t.Fatalf("unexpected project config: %+v", resolved.Config)
		}
	})
}

func newReadServiceDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	return sqlx.NewDb(db, "sqlmock"), mock, func() { _ = db.Close() }
}

func assertDomainError(t *testing.T, err error, wantType string, wantStatus int) {
	t.Helper()
	var appErr *domain.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if string(appErr.Type) != wantType {
		t.Fatalf("expected type %q, got %q", wantType, appErr.Type)
	}
	if appErr.StatusCode != wantStatus {
		t.Fatalf("expected status %d, got %d", wantStatus, appErr.StatusCode)
	}
}
