package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

const ExplorerRevisionSchema = `
CREATE TABLE IF NOT EXISTS config_schema.explorer_config_revision (
 id TEXT PRIMARY KEY,
 config_id TEXT NOT NULL,
 project_id TEXT NOT NULL,
 parent_revision_id TEXT NULL,
 digest TEXT NOT NULL,
 overlay JSONB NOT NULL,
 status TEXT NOT NULL,
 target_execution_id TEXT NULL,
 target_generation TEXT NULL,
 target_schema_digest TEXT NULL,
 diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
 created_by TEXT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS explorer_config_revision_config_idx ON config_schema.explorer_config_revision(config_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS explorer_config_revision_active_idx ON config_schema.explorer_config_revision(config_id) WHERE status = 'ACTIVE';
ALTER TABLE config_schema.explorer_config_revision DROP CONSTRAINT IF EXISTS explorer_config_revision_config_id_digest_key;
CREATE UNIQUE INDEX IF NOT EXISTS explorer_config_revision_identity_idx ON config_schema.explorer_config_revision
 (config_id,digest,COALESCE(parent_revision_id,''),COALESCE(target_execution_id,''));
UPDATE config_schema.explorer_config_revision
SET target_execution_id = NULLIF(diagnostics->>'targetExecutionId', '')
WHERE target_execution_id IS NULL
  AND diagnostics ? 'targetExecutionId';
`

type ExplorerRevision struct {
	ID                 string          `db:"id" json:"revisionId"`
	ConfigID           string          `db:"config_id" json:"configId"`
	ProjectID          string          `db:"project_id" json:"projectId"`
	ParentRevisionID   sql.NullString  `db:"parent_revision_id" json:"-"`
	Digest             string          `db:"digest" json:"digest"`
	Overlay            json.RawMessage `db:"overlay" json:"overlay"`
	Status             string          `db:"status" json:"status"`
	TargetExecutionID  sql.NullString  `db:"target_execution_id" json:"targetExecutionId,omitempty"`
	TargetGeneration   sql.NullString  `db:"target_generation" json:"targetGeneration,omitempty"`
	TargetSchemaDigest sql.NullString  `db:"target_schema_digest" json:"targetSchemaDigest,omitempty"`
	Diagnostics        json.RawMessage `db:"diagnostics" json:"diagnostics"`
	CreatedBy          sql.NullString  `db:"created_by" json:"createdBy,omitempty"`
	CreatedAt          time.Time       `db:"created_at" json:"createdAt"`
	UpdatedAt          time.Time       `db:"updated_at" json:"updatedAt"`
}

func EnsureExplorerRevisionTable(ctx context.Context, db *sqlx.DB) error {
	if db == nil {
		return errors.New("database is nil")
	}
	_, err := db.ExecContext(ctx, ExplorerRevisionSchema)
	return err
}

func InsertExplorerRevision(ctx context.Context, db *sqlx.DB, r *ExplorerRevision) error {
	if db == nil {
		return errors.New("database is nil")
	}
	_, err := db.ExecContext(ctx, `INSERT INTO config_schema.explorer_config_revision
 (id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,diagnostics,created_by)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, r.ID, r.ConfigID, r.ProjectID, nullString(r.ParentRevisionID), r.Digest, r.Overlay, r.Status, nullString(r.TargetExecutionID), r.Diagnostics, nullString(r.CreatedBy))
	return err
}

// ExplorerRevisionByIdentity returns an exact retry of the same candidate.
// Digest alone is insufficient: the same overlay may be published against a
// later active parent or a newly materialized Loom execution.
func ExplorerRevisionByIdentity(ctx context.Context, db *sqlx.DB, configID, digest string, parentRevisionID, targetExecutionID sql.NullString) (*ExplorerRevision, error) {
	var r ExplorerRevision
	err := db.QueryRowContext(ctx, `SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=$1 AND digest=$2 AND parent_revision_id IS NOT DISTINCT FROM $3 AND target_execution_id IS NOT DISTINCT FROM $4`, configID, digest, nullString(parentRevisionID), nullString(targetExecutionID)).Scan(
		&r.ID, &r.ConfigID, &r.ProjectID, &r.ParentRevisionID, &r.Digest, &r.Overlay, &r.Status, &r.TargetExecutionID, &r.TargetGeneration, &r.TargetSchemaDigest, &r.Diagnostics, &r.CreatedBy, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func GetExplorerRevision(ctx context.Context, db *sqlx.DB, configID, id string) (*ExplorerRevision, error) {
	var r ExplorerRevision
	err := db.QueryRowContext(ctx, `SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=$1 AND id=$2`, configID, id).Scan(
		&r.ID, &r.ConfigID, &r.ProjectID, &r.ParentRevisionID, &r.Digest, &r.Overlay, &r.Status, &r.TargetExecutionID, &r.TargetGeneration, &r.TargetSchemaDigest, &r.Diagnostics, &r.CreatedBy, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func ListExplorerRevisions(ctx context.Context, db *sqlx.DB, configID string) ([]ExplorerRevision, error) {
	rows, err := db.QueryContext(ctx, `SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=$1 ORDER BY created_at DESC`, configID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExplorerRevision
	for rows.Next() {
		var r ExplorerRevision
		if err := rows.Scan(&r.ID, &r.ConfigID, &r.ProjectID, &r.ParentRevisionID, &r.Digest, &r.Overlay, &r.Status, &r.TargetExecutionID, &r.TargetGeneration, &r.TargetSchemaDigest, &r.Diagnostics, &r.CreatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func ActiveExplorerRevision(ctx context.Context, db *sqlx.DB, configID string) (*ExplorerRevision, error) {
	var r ExplorerRevision
	err := db.QueryRowContext(ctx, `SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=$1 AND status='ACTIVE'`, configID).Scan(&r.ID, &r.ConfigID, &r.ProjectID, &r.ParentRevisionID, &r.Digest, &r.Overlay, &r.Status, &r.TargetExecutionID, &r.TargetGeneration, &r.TargetSchemaDigest, &r.Diagnostics, &r.CreatedBy, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ExplorerRevisionByStatus returns the newest revision in a given publication
// state. It is used by request-time reconciliation to recover a process that
// stopped after Loom activation but before the Gecko transaction committed.
func ExplorerRevisionByStatus(ctx context.Context, db *sqlx.DB, configID, status string) (*ExplorerRevision, error) {
	var r ExplorerRevision
	err := db.QueryRowContext(ctx, `SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=$1 AND status=$2 ORDER BY updated_at DESC LIMIT 1`, configID, status).Scan(&r.ID, &r.ConfigID, &r.ProjectID, &r.ParentRevisionID, &r.Digest, &r.Overlay, &r.Status, &r.TargetExecutionID, &r.TargetGeneration, &r.TargetSchemaDigest, &r.Diagnostics, &r.CreatedBy, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func UpdateExplorerRevision(ctx context.Context, db *sqlx.DB, r *ExplorerRevision) error {
	_, err := db.ExecContext(ctx, `UPDATE config_schema.explorer_config_revision SET status=$2,target_execution_id=$3,target_generation=$4,target_schema_digest=$5,diagnostics=$6,updated_at=now() WHERE id=$1`, r.ID, r.Status, nullString(r.TargetExecutionID), nullString(r.TargetGeneration), nullString(r.TargetSchemaDigest), r.Diagnostics)
	return err
}

func ActivateExplorerRevision(ctx context.Context, db *sqlx.DB, r *ExplorerRevision, expectedParent string) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT id FROM config_schema.explorer_config_revision WHERE config_id=$1 AND status='ACTIVE' FOR UPDATE`, r.ConfigID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return err
	}
	if current.Valid && current.String != expectedParent {
		return fmt.Errorf("active revision changed: expected %s, got %s", expectedParent, current.String)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE config_schema.explorer_config_revision SET status='SUPERSEDED',updated_at=now() WHERE config_id=$1 AND status='ACTIVE'`, r.ConfigID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE config_schema.explorer_config_revision SET status='ACTIVE',updated_at=now() WHERE id=$1`, r.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func nullString(s sql.NullString) any {
	if s.Valid {
		return s.String
	}
	return nil
}
