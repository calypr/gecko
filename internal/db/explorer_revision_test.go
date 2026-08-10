package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

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
