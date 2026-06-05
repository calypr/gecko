package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

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
