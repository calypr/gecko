package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func UpsertGitPendingRepository(db *sqlx.DB, pending GitPendingRepository) error {
	if db == nil {
		return nil
	}
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_pending_repository (
			id, installation_id, setup_session_id, created_by_user_id, source, organization, repo_id, repo_name, repo_full_name, repo_html_url, repo_clone_url, repo_host, repo_owner, repo_path, added_at, updated_at, resolved_at, removed_at
		) VALUES (
			:id, :installation_id, :setup_session_id, :created_by_user_id, :source, :organization, :repo_id, :repo_name, :repo_full_name, :repo_html_url, :repo_clone_url, :repo_host, :repo_owner, :repo_path, :added_at, :updated_at, :resolved_at, :removed_at
		)
		ON CONFLICT (id) DO UPDATE SET
			id = EXCLUDED.id,
			setup_session_id = EXCLUDED.setup_session_id,
			created_by_user_id = EXCLUDED.created_by_user_id,
			source = EXCLUDED.source,
			organization = EXCLUDED.organization,
			repo_name = EXCLUDED.repo_name,
			repo_full_name = EXCLUDED.repo_full_name,
			repo_html_url = EXCLUDED.repo_html_url,
			repo_clone_url = EXCLUDED.repo_clone_url,
			repo_host = EXCLUDED.repo_host,
			repo_owner = EXCLUDED.repo_owner,
			repo_path = EXCLUDED.repo_path,
			added_at = EXCLUDED.added_at,
			updated_at = EXCLUDED.updated_at,
			resolved_at = EXCLUDED.resolved_at,
			removed_at = EXCLUDED.removed_at
	`, pending)
	if err != nil {
		return fmt.Errorf("upsert git pending repository: %w", err)
	}
	return nil
}

func gitPendingRepositorySelectSQL() string {
	return `SELECT id, installation_id, setup_session_id, created_by_user_id, source, organization, repo_id, repo_name, repo_full_name, repo_html_url, repo_clone_url, repo_host, repo_owner, repo_path, added_at, updated_at, resolved_at, removed_at FROM config_schema.git_pending_repository`
}

func ListGitPendingRepositoriesByInstallation(db *sqlx.DB, installationID int64) ([]GitPendingRepository, error) {
	if db == nil {
		return []GitPendingRepository{}, nil
	}
	records := []GitPendingRepository{}
	if err := db.Select(&records, gitPendingRepositorySelectSQL()+`
		WHERE installation_id = $1 AND resolved_at IS NULL AND removed_at IS NULL
		ORDER BY added_at, repo_full_name
	`, installationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []GitPendingRepository{}, nil
		}
		return nil, err
	}
	return records, nil
}

func ListGitPendingRepositoriesByUser(db *sqlx.DB, userID string, installationID int64, setupSessionID string) ([]GitPendingRepository, error) {
	if db == nil {
		return []GitPendingRepository{}, nil
	}
	records := []GitPendingRepository{}
	query := gitPendingRepositorySelectSQL() + `
		WHERE created_by_user_id = $1
		  AND resolved_at IS NULL
		  AND removed_at IS NULL`
	args := []any{userID}
	if installationID > 0 {
		args = append(args, installationID)
		query += fmt.Sprintf(" AND installation_id = $%d", len(args))
	}
	if setupSessionID != "" {
		args = append(args, setupSessionID)
		query += fmt.Sprintf(" AND setup_session_id = $%d", len(args))
	}
	query += " ORDER BY added_at, repo_full_name"
	if err := db.Select(&records, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []GitPendingRepository{}, nil
		}
		return nil, err
	}
	return records, nil
}

func ListGitPendingRepositories(db *sqlx.DB) ([]GitPendingRepository, error) {
	if db == nil {
		return []GitPendingRepository{}, nil
	}
	records := []GitPendingRepository{}
	if err := db.Select(&records, gitPendingRepositorySelectSQL()+`
		WHERE resolved_at IS NULL AND removed_at IS NULL
		ORDER BY added_at, repo_full_name
	`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []GitPendingRepository{}, nil
		}
		return nil, err
	}
	return records, nil
}

func GitPendingRepositoryByID(db *sqlx.DB, id string) (*GitPendingRepository, error) {
	if db == nil || id == "" {
		return nil, nil
	}
	var pending GitPendingRepository
	if err := db.Get(&pending, gitPendingRepositorySelectSQL()+` WHERE id = $1`, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get git pending repository by id: %w", err)
	}
	return &pending, nil
}

func ResolveGitPendingRepositoryByID(db *sqlx.DB, id string) error {
	if db == nil || id == "" {
		return nil
	}
	_, err := db.Exec(`UPDATE config_schema.git_pending_repository SET resolved_at = NOW(), updated_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("resolve git pending repository by id: %w", err)
	}
	return nil
}

func ResolveGitPendingRepositoriesByRepo(db *sqlx.DB, installationID int64, repoHost string, repoOwner string, repoPath string) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(`
		UPDATE config_schema.git_pending_repository
		SET resolved_at = NOW(), updated_at = NOW()
		WHERE installation_id = $1
		  AND repo_host = $2
		  AND repo_owner = $3
		  AND repo_path = $4
		  AND resolved_at IS NULL
		  AND removed_at IS NULL
	`, installationID, repoHost, repoOwner, repoPath)
	if err != nil {
		return fmt.Errorf("resolve git pending repositories by repo: %w", err)
	}
	return nil
}

func ResolveGitPendingRepositoriesByRepositoryIdentity(db *sqlx.DB, repoHost string, repoOwner string, repoPath string) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(`
		UPDATE config_schema.git_pending_repository
		SET resolved_at = NOW(), updated_at = NOW()
		WHERE repo_host = $1
		  AND repo_owner = $2
		  AND repo_path = $3
		  AND resolved_at IS NULL
		  AND removed_at IS NULL
	`, repoHost, repoOwner, repoPath)
	if err != nil {
		return fmt.Errorf("resolve git pending repositories by repository identity: %w", err)
	}
	return nil
}

func RemoveGitPendingRepository(db *sqlx.DB, installationID int64, repoID int64) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(`
		UPDATE config_schema.git_pending_repository
		SET removed_at = NOW(), updated_at = NOW()
		WHERE installation_id = $1 AND repo_id = $2 AND removed_at IS NULL
	`, installationID, repoID)
	if err != nil {
		return fmt.Errorf("remove git pending repository: %w", err)
	}
	return nil
}

func UpsertGitSetupSession(db *sqlx.DB, session GitSetupSession) error {
	if db == nil {
		return nil
	}
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_setup_session (
			id, created_by_user_id, organization, installation_id, before_repo_ids, created_at, updated_at, completed_at
		) VALUES (
			:id, :created_by_user_id, :organization, :installation_id, :before_repo_ids, :created_at, :updated_at, :completed_at
		)
		ON CONFLICT (id) DO UPDATE SET
			created_by_user_id = EXCLUDED.created_by_user_id,
			organization = EXCLUDED.organization,
			installation_id = EXCLUDED.installation_id,
			before_repo_ids = EXCLUDED.before_repo_ids,
			updated_at = EXCLUDED.updated_at,
			completed_at = EXCLUDED.completed_at
	`, session)
	if err != nil {
		return fmt.Errorf("upsert git setup session: %w", err)
	}
	return nil
}

func GitSetupSessionByID(db *sqlx.DB, id string) (*GitSetupSession, error) {
	if db == nil || id == "" {
		return nil, nil
	}
	var session GitSetupSession
	err := db.Get(&session, `SELECT id, created_by_user_id, organization, installation_id, before_repo_ids, created_at, updated_at, completed_at FROM config_schema.git_setup_session WHERE id = $1`, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &session, nil
}

func EncodeRepoIDs(repoIDs []int64) string {
	body, err := json.Marshal(repoIDs)
	if err != nil {
		return "[]"
	}
	return string(body)
}

func DecodeRepoIDs(raw string) map[int64]struct{} {
	repoIDs := []int64{}
	_ = json.Unmarshal([]byte(raw), &repoIDs)
	indexed := make(map[int64]struct{}, len(repoIDs))
	for _, repoID := range repoIDs {
		indexed[repoID] = struct{}{}
	}
	return indexed
}
