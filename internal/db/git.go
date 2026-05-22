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

type GitOrganizationState struct {
	Organization           string         `db:"organization"`
	Installed              bool           `db:"installed"`
	InstallationID         sql.NullInt64  `db:"installation_id"`
	InstallationTargetType sql.NullString `db:"installation_target_type"`
	InstallationTarget     sql.NullString `db:"installation_target"`
	HTMLURL                sql.NullString `db:"html_url"`
	RepositorySelection    sql.NullString `db:"repository_selection"`
	ConfiguredAt           sql.NullTime   `db:"configured_at"`
	LastSeenAt             sql.NullTime   `db:"last_seen_at"`
	UpdatedAt              time.Time      `db:"updated_at"`
	LastError              sql.NullString `db:"last_error"`
}

type GitUploadSession struct {
	ID               string         `db:"id"`
	ProjectID        string         `db:"project_id"`
	Organization     string         `db:"organization"`
	Project          string         `db:"project"`
	RepoHost         string         `db:"repo_host"`
	RepoOwner        string         `db:"repo_owner"`
	RepoName         string         `db:"repo_name"`
	BaseBranch       string         `db:"base_branch"`
	TargetSubdir     sql.NullString `db:"target_subdirectory"`
	BranchName       string         `db:"branch_name"`
	PRTitle          string         `db:"pr_title"`
	PRBody           string         `db:"pr_body"`
	Status           string         `db:"status"`
	PullRequestURL   sql.NullString `db:"pull_request_url"`
	CommitSHA        sql.NullString `db:"commit_sha"`
	LastError        sql.NullString `db:"last_error"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
}

type GitUploadSessionFile struct {
	SessionID   string         `db:"session_id"`
	FileName    string         `db:"file_name"`
	TargetPath  string         `db:"target_path"`
	Size        int64          `db:"size"`
	Checksum    sql.NullString `db:"checksum"`
	DRSObjectID sql.NullString `db:"drs_object_id"`
	Status      string         `db:"status"`
	Error       sql.NullString `db:"error"`
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
	`)
	if err != nil {
		return fmt.Errorf("ensure git state tables: %w", err)
	}
	return nil
}

func GitUploadSessionByID(db *sqlx.DB, sessionID string) (*GitUploadSession, error) {
	if db == nil {
		return nil, nil
	}
	var session GitUploadSession
	err := db.Get(&session, `SELECT id, project_id, organization, project, repo_host, repo_owner, repo_name, base_branch, target_subdirectory, branch_name, pr_title, pr_body, status, pull_request_url, commit_sha, last_error, created_at, updated_at FROM config_schema.git_upload_session WHERE id = $1`, sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &session, nil
}

func UpsertGitUploadSession(db *sqlx.DB, session GitUploadSession) error {
	if db == nil {
		return nil
	}
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_upload_session (
			id, project_id, organization, project, repo_host, repo_owner, repo_name, base_branch, target_subdirectory, branch_name, pr_title, pr_body, status, pull_request_url, commit_sha, last_error, created_at, updated_at
		) VALUES (
			:id, :project_id, :organization, :project, :repo_host, :repo_owner, :repo_name, :base_branch, :target_subdirectory, :branch_name, :pr_title, :pr_body, :status, :pull_request_url, :commit_sha, :last_error, :created_at, :updated_at
		)
		ON CONFLICT (id) DO UPDATE SET
			project_id = EXCLUDED.project_id,
			organization = EXCLUDED.organization,
			project = EXCLUDED.project,
			repo_host = EXCLUDED.repo_host,
			repo_owner = EXCLUDED.repo_owner,
			repo_name = EXCLUDED.repo_name,
			base_branch = EXCLUDED.base_branch,
			target_subdirectory = EXCLUDED.target_subdirectory,
			branch_name = EXCLUDED.branch_name,
			pr_title = EXCLUDED.pr_title,
			pr_body = EXCLUDED.pr_body,
			status = EXCLUDED.status,
			pull_request_url = EXCLUDED.pull_request_url,
			commit_sha = EXCLUDED.commit_sha,
			last_error = EXCLUDED.last_error,
			updated_at = EXCLUDED.updated_at;
	`, session)
	if err != nil {
		return fmt.Errorf("upsert git upload session: %w", err)
	}
	return nil
}

func ReplaceGitUploadSessionFiles(db *sqlx.DB, sessionID string, files []GitUploadSessionFile) error {
	if db == nil {
		return nil
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin git upload session file transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := tx.Exec(`DELETE FROM config_schema.git_upload_session_file WHERE session_id = $1`, sessionID); err != nil {
		return fmt.Errorf("delete git upload session files: %w", err)
	}
	for _, file := range files {
		if _, err := tx.NamedExec(`
			INSERT INTO config_schema.git_upload_session_file (
				session_id, file_name, target_path, size, checksum, drs_object_id, status, error
			) VALUES (
				:session_id, :file_name, :target_path, :size, :checksum, :drs_object_id, :status, :error
			)
		`, file); err != nil {
			return fmt.Errorf("insert git upload session file: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit git upload session file transaction: %w", err)
	}
	return nil
}

func ListGitUploadSessionFiles(db *sqlx.DB, sessionID string) ([]GitUploadSessionFile, error) {
	if db == nil {
		return []GitUploadSessionFile{}, nil
	}
	files := []GitUploadSessionFile{}
	if err := db.Select(&files, `SELECT session_id, file_name, target_path, size, checksum, drs_object_id, status, error FROM config_schema.git_upload_session_file WHERE session_id = $1 ORDER BY target_path`, sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []GitUploadSessionFile{}, nil
		}
		return nil, err
	}
	return files, nil
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

func GitOrganizationStateByOrganization(db *sqlx.DB, organization string) (*GitOrganizationState, error) {
	if db == nil {
		return nil, nil
	}
	var state GitOrganizationState
	err := db.Get(&state, `SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema.git_organization_state WHERE organization = $1`, organization)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &state, nil
}

func UpsertGitOrganizationState(db *sqlx.DB, state GitOrganizationState) error {
	if db == nil {
		return nil
	}
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_organization_state (
			organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error
		) VALUES (
			:organization, :installed, :installation_id, :installation_target_type, :installation_target, :html_url, :repository_selection, :configured_at, :last_seen_at, :updated_at, :last_error
		)
		ON CONFLICT (organization) DO UPDATE SET
			installed = EXCLUDED.installed,
			installation_id = EXCLUDED.installation_id,
			installation_target_type = EXCLUDED.installation_target_type,
			installation_target = EXCLUDED.installation_target,
			html_url = EXCLUDED.html_url,
			repository_selection = EXCLUDED.repository_selection,
			configured_at = EXCLUDED.configured_at,
			last_seen_at = EXCLUDED.last_seen_at,
			updated_at = EXCLUDED.updated_at,
			last_error = EXCLUDED.last_error;
	`, state)
	if err != nil {
		return fmt.Errorf("upsert git organization state: %w", err)
	}
	return nil
}

func ListGitOrganizationStates(db *sqlx.DB) (map[string]GitOrganizationState, error) {
	states := []GitOrganizationState{}
	if err := db.Select(&states, `SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema.git_organization_state`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]GitOrganizationState{}, nil
		}
		return nil, err
	}
	indexed := make(map[string]GitOrganizationState, len(states))
	for _, state := range states {
		indexed[state.Organization] = state
	}
	return indexed, nil
}
