package db

import (
	"database/sql"
	"time"
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

type GitPendingRepository struct {
	ID              string         `db:"id"`
	InstallationID  int64          `db:"installation_id"`
	SetupSessionID  sql.NullString `db:"setup_session_id"`
	CreatedByUserID sql.NullString `db:"created_by_user_id"`
	Source          string         `db:"source"`
	Organization    string         `db:"organization"`
	RepoID          int64          `db:"repo_id"`
	RepoName        string         `db:"repo_name"`
	RepoFullName    string         `db:"repo_full_name"`
	RepoHTMLURL     sql.NullString `db:"repo_html_url"`
	RepoCloneURL    sql.NullString `db:"repo_clone_url"`
	RepoHost        string         `db:"repo_host"`
	RepoOwner       string         `db:"repo_owner"`
	RepoPath        string         `db:"repo_path"`
	AddedAt         time.Time      `db:"added_at"`
	UpdatedAt       time.Time      `db:"updated_at"`
	ResolvedAt      sql.NullTime   `db:"resolved_at"`
	RemovedAt       sql.NullTime   `db:"removed_at"`
}

type GitSetupSession struct {
	ID              string        `db:"id"`
	CreatedByUserID string        `db:"created_by_user_id"`
	Organization    string        `db:"organization"`
	InstallationID  sql.NullInt64 `db:"installation_id"`
	BeforeRepoIDs   string        `db:"before_repo_ids"`
	CreatedAt       time.Time     `db:"created_at"`
	UpdatedAt       time.Time     `db:"updated_at"`
	CompletedAt     sql.NullTime  `db:"completed_at"`
}

type GitUploadSession struct {
	ID             string         `db:"id"`
	ProjectID      string         `db:"project_id"`
	Organization   string         `db:"organization"`
	Project        string         `db:"project"`
	RepoHost       string         `db:"repo_host"`
	RepoOwner      string         `db:"repo_owner"`
	RepoName       string         `db:"repo_name"`
	BaseBranch     string         `db:"base_branch"`
	TargetSubdir   sql.NullString `db:"target_subdirectory"`
	BranchName     string         `db:"branch_name"`
	PRTitle        string         `db:"pr_title"`
	PRBody         string         `db:"pr_body"`
	Status         string         `db:"status"`
	PullRequestURL sql.NullString `db:"pull_request_url"`
	CommitSHA      sql.NullString `db:"commit_sha"`
	LastError      sql.NullString `db:"last_error"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
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
