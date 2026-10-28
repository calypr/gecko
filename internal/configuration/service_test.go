package configuration

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/domain"
	"github.com/jmoiron/sqlx"
)

func newTestService(t *testing.T) (*Service, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewService(sqlx.NewDb(db, "sqlmock"), nil), mock
}

func TestConfigForTypeRejectsUnknownType(t *testing.T) {
	_, err := ConfigForType("unknown")
	if err == nil {
		t.Fatal("expected unknown config type to fail")
	}
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.StatusCode != 400 {
		t.Fatalf("expected typed 400 error, got %#v", err)
	}
}

func TestGetReturnsConfigNotFound(t *testing.T) {
	service, mock := newTestService(t)
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("org/project").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}))

	_, err := service.Get(context.Background(), string(config.TypeProjects), "org/project")
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.StatusCode != 404 {
		t.Fatalf("expected typed 404 error, got %#v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutProjectConfigSetsOrganizationAndValidatesInitialization(t *testing.T) {
	service, mock := newTestService(t)
	mock.ExpectExec(`INSERT INTO config_schema\.projects`).
		WithArgs("org/project", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	cfg := &config.ProjectConfig{
		Title:        "Project",
		ContactEmail: "owner@example.org",
		OrgTitle:     "stale",
		Description:  "description",
		ProjectTitle: "project",
	}
	if err := service.Put(context.Background(), string(config.TypeProjects), "org/project", cfg); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if cfg.OrgTitle != "org" {
		t.Fatalf("expected org title org, got %q", cfg.OrgTitle)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
