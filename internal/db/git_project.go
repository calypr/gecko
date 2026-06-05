package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

func EnsureGitProjectStateTable(db *sqlx.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS config_schema.git_project_state (
			project_id TEXT PRIMARY KEY,
			repo_host TEXT NOT NULL,
			repo_owner TEXT NOT NULL,
			repo_name TEXT NOT NULL,
			installation_id BIGINT NULL,
			installation_target_type TEXT NULL,
			installation_target TEXT NULL,
			mirror_path TEXT NOT NULL,
			sync_state TEXT NOT NULL DEFAULT 'never_synced',
			default_branch TEXT NULL,
			last_refreshed_at TIMESTAMPTZ NULL,
			last_error TEXT NULL
		);
		CREATE TABLE IF NOT EXISTS config_schema.git_organization_state (
			organization TEXT PRIMARY KEY,
			installed BOOLEAN NOT NULL DEFAULT FALSE,
			installation_id BIGINT NULL,
			installation_target_type TEXT NULL,
			installation_target TEXT NULL,
			html_url TEXT NULL,
			repository_selection TEXT NULL,
			configured_at TIMESTAMPTZ NULL,
			last_seen_at TIMESTAMPTZ NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_error TEXT NULL
		);
		CREATE TABLE IF NOT EXISTS config_schema.git_upload_session (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			organization TEXT NOT NULL,
			project TEXT NOT NULL,
			repo_host TEXT NOT NULL,
			repo_owner TEXT NOT NULL,
			repo_name TEXT NOT NULL,
			base_branch TEXT NOT NULL,
			target_subdirectory TEXT NULL,
			branch_name TEXT NOT NULL,
			pr_title TEXT NOT NULL,
			pr_body TEXT NOT NULL,
			status TEXT NOT NULL,
			pull_request_url TEXT NULL,
			commit_sha TEXT NULL,
			last_error TEXT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE IF NOT EXISTS config_schema.git_upload_session_file (
			session_id TEXT NOT NULL,
			file_name TEXT NOT NULL,
			target_path TEXT NOT NULL,
			size BIGINT NOT NULL,
			checksum TEXT NULL,
			drs_object_id TEXT NULL,
			status TEXT NOT NULL,
			error TEXT NULL,
			PRIMARY KEY (session_id, target_path)
		);
		CREATE TABLE IF NOT EXISTS config_schema.git_pending_repository (
			id TEXT PRIMARY KEY,
			installation_id BIGINT NOT NULL,
			setup_session_id TEXT NULL,
			created_by_user_id TEXT NULL,
			source TEXT NOT NULL DEFAULT 'webhook',
			organization TEXT NOT NULL,
			repo_id BIGINT NOT NULL,
			repo_name TEXT NOT NULL,
			repo_full_name TEXT NOT NULL,
			repo_html_url TEXT NULL,
			repo_clone_url TEXT NULL,
			repo_host TEXT NOT NULL,
			repo_owner TEXT NOT NULL,
			repo_path TEXT NOT NULL,
			added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			resolved_at TIMESTAMPTZ NULL,
			removed_at TIMESTAMPTZ NULL
		);
		CREATE TABLE IF NOT EXISTS config_schema.git_setup_session (
			id TEXT PRIMARY KEY,
			created_by_user_id TEXT NOT NULL,
			organization TEXT NOT NULL,
			installation_id BIGINT NULL,
			before_repo_ids TEXT NOT NULL DEFAULT '[]',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMPTZ NULL
		);
		ALTER TABLE config_schema.git_pending_repository ADD COLUMN IF NOT EXISTS setup_session_id TEXT NULL;
		ALTER TABLE config_schema.git_pending_repository ADD COLUMN IF NOT EXISTS created_by_user_id TEXT NULL;
		ALTER TABLE config_schema.git_pending_repository ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'webhook';
		ALTER TABLE config_schema.git_pending_repository DROP CONSTRAINT IF EXISTS git_pending_repository_installation_id_repo_id_key;
		CREATE UNIQUE INDEX IF NOT EXISTS git_pending_repository_webhook_repo_key
			ON config_schema.git_pending_repository (installation_id, repo_id)
			WHERE created_by_user_id IS NULL;
		CREATE UNIQUE INDEX IF NOT EXISTS git_pending_repository_user_repo_key
			ON config_schema.git_pending_repository (installation_id, repo_id, created_by_user_id)
			WHERE created_by_user_id IS NOT NULL;
		CREATE INDEX IF NOT EXISTS git_pending_repository_user_unresolved_idx
			ON config_schema.git_pending_repository (created_by_user_id, added_at)
			WHERE resolved_at IS NULL AND removed_at IS NULL;
		`)
	if err != nil {
		return fmt.Errorf("ensure git state tables: %w", err)
	}
	return nil
}

func GitProjectStateByProjectID(db *sqlx.DB, projectID string) (*GitProjectState, error) {
	if db == nil {
		return nil, nil
	}
	var state GitProjectState
	err := db.Get(&state, `SELECT project_id, repo_host, repo_owner, repo_name, installation_id, installation_target_type, installation_target, mirror_path, sync_state, default_branch, last_refreshed_at, last_error FROM config_schema.git_project_state WHERE project_id = $1`, projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &state, nil
}

func UpsertGitProjectState(db *sqlx.DB, state GitProjectState) error {
	if db == nil {
		return nil
	}
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_project_state (
			project_id, repo_host, repo_owner, repo_name, installation_id, installation_target_type, installation_target, mirror_path, sync_state, default_branch, last_refreshed_at, last_error
		) VALUES (
			:project_id, :repo_host, :repo_owner, :repo_name, :installation_id, :installation_target_type, :installation_target, :mirror_path, :sync_state, :default_branch, :last_refreshed_at, :last_error
		)
		ON CONFLICT (project_id) DO UPDATE SET
			repo_host = EXCLUDED.repo_host,
			repo_owner = EXCLUDED.repo_owner,
			repo_name = EXCLUDED.repo_name,
			installation_id = EXCLUDED.installation_id,
			installation_target_type = EXCLUDED.installation_target_type,
			installation_target = EXCLUDED.installation_target,
			mirror_path = EXCLUDED.mirror_path,
			sync_state = EXCLUDED.sync_state,
			default_branch = EXCLUDED.default_branch,
			last_refreshed_at = EXCLUDED.last_refreshed_at,
			last_error = EXCLUDED.last_error;
	`, state)
	if err != nil {
		return fmt.Errorf("upsert git project state: %w", err)
	}
	return nil
}

func ListGitProjectStates(db *sqlx.DB) (map[string]GitProjectState, error) {
	states := []GitProjectState{}
	if err := db.Select(&states, `SELECT project_id, repo_host, repo_owner, repo_name, installation_id, installation_target_type, installation_target, mirror_path, sync_state, default_branch, last_refreshed_at, last_error FROM config_schema.git_project_state`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]GitProjectState{}, nil
		}
		return nil, err
	}
	indexed := make(map[string]GitProjectState, len(states))
	for _, state := range states {
		indexed[state.ProjectID] = state
	}
	return indexed, nil
}

func DeleteGitProjectArtifacts(db *sqlx.DB, projectID string) error {
	if db == nil || strings.TrimSpace(projectID) == "" {
		return nil
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin delete git project artifacts transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := tx.Exec(`
		DELETE FROM config_schema.git_upload_session_file
		WHERE session_id IN (
			SELECT id FROM config_schema.git_upload_session WHERE project_id = $1
		)
	`, projectID); err != nil {
		return fmt.Errorf("delete git upload session files for %s: %w", projectID, err)
	}
	if _, err := tx.Exec(`DELETE FROM config_schema.git_upload_session WHERE project_id = $1`, projectID); err != nil {
		return fmt.Errorf("delete git upload sessions for %s: %w", projectID, err)
	}
	if _, err := tx.Exec(`DELETE FROM config_schema.git_project_state WHERE project_id = $1`, projectID); err != nil {
		return fmt.Errorf("delete git project state for %s: %w", projectID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete git project artifacts transaction: %w", err)
	}
	return nil
}
