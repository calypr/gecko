package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type GitProjectState struct {
	ProjectID              string         `db:"project_id"`
	RepoHost               string         `db:"repo_host"`
	RepoOwner              string         `db:"repo_owner"`
	RepoName               string         `db:"repo_name"`
	InstallationID         sql.NullInt64  `db:"installation_id"`
	InstallationTargetType sql.NullString `db:"installation_target_type"`
	InstallationTarget     sql.NullString `db:"installation_target"`
	MirrorPath             string         `db:"mirror_path"`
	SyncState              string         `db:"sync_state"`
	DefaultBranch          sql.NullString `db:"default_branch"`
	LastRefreshedAt        sql.NullTime   `db:"last_refreshed_at"`
	LastError              sql.NullString `db:"last_error"`
}

func (state GitProjectState) RefreshedAt() *time.Time {
	if !state.LastRefreshedAt.Valid {
		return nil
	}
	return &state.LastRefreshedAt.Time
}

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
	`)
	if err != nil {
		return fmt.Errorf("ensure git project state table: %w", err)
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
