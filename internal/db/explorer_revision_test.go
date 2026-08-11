package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestExplorerRevisionSchemaHasExecutableTableDefinition(t *testing.T) {
	if strings.Contains(ExplorerRevisionSchema, ",\n);") {
		t.Fatal("explorer revision table definition has a trailing comma")
	}
	for _, statement := range []string{
		"DROP CONSTRAINT IF EXISTS explorer_config_revision_config_id_digest_key",
		"CREATE UNIQUE INDEX IF NOT EXISTS explorer_config_revision_identity_idx",
		"COALESCE(parent_revision_id,'')",
		"COALESCE(target_execution_id,'')",
	} {
		if !strings.Contains(ExplorerRevisionSchema, statement) {
			t.Fatalf("explorer revision schema missing %q", statement)
		}
	}
}

func TestUpdateExplorerRevisionPersistsTargetExecutionID(t *testing.T) {
	rawDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()

	database := sqlx.NewDb(rawDB, "sqlmock")
	revision := &ExplorerRevision{
		ID:                 "revision-1",
		Status:             "VALID",
		TargetExecutionID:  sql.NullString{String: "execution-1", Valid: true},
		TargetGeneration:   sql.NullString{String: "generation-1", Valid: true},
		TargetSchemaDigest: sql.NullString{String: "schema-1", Valid: true},
		Diagnostics:        json.RawMessage(`{"targetExecutionId":"execution-1"}`),
	}

	query := `UPDATE config_schema.explorer_config_revision SET status=$2,target_execution_id=$3,target_generation=$4,target_schema_digest=$5,diagnostics=$6,updated_at=now() WHERE id=$1`
	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("revision-1", "VALID", "execution-1", "generation-1", "schema-1", revision.Diagnostics).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := UpdateExplorerRevision(context.Background(), database, revision); err != nil {
		t.Fatalf("UpdateExplorerRevision() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExplorerRevisionByIdentityReturnsOnlyExactRetry(t *testing.T) {
	rawDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()

	database := sqlx.NewDb(rawDB, "sqlmock")
	now := time.Now()
	columns := []string{"id", "config_id", "project_id", "parent_revision_id", "digest", "overlay", "status", "target_execution_id", "target_generation", "target_schema_digest", "diagnostics", "created_by", "created_at", "updated_at"}
	mock.ExpectQuery("SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=\\$1 AND digest=\\$2 AND parent_revision_id IS NOT DISTINCT FROM \\$3 AND target_execution_id IS NOT DISTINCT FROM \\$4").
		WithArgs("project-1", "digest-1", "parent-1", "execution-1").
		WillReturnRows(sqlmock.NewRows(columns).AddRow("revision-1", "project-1", "project-1", "parent-1", "digest-1", []byte(`{"schemaVersion":2}`), "VALIDATED", "execution-1", "generation-1", "schema-1", []byte(`{"status":"VALID"}`), nil, now, now))

	revision, err := ExplorerRevisionByIdentity(context.Background(), database, "project-1", "digest-1", sql.NullString{String: "parent-1", Valid: true}, sql.NullString{String: "execution-1", Valid: true})
	if err != nil || revision == nil || revision.ID != "revision-1" {
		t.Fatalf("revision = %#v, err = %v", revision, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
