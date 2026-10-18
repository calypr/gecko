package upload

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/internal/domain"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
	"github.com/calypr/gecko/internal/repo"
	"github.com/jmoiron/sqlx"
)

func TestFinalizeReturnsFinalizedSessionWithoutRemoteCalls(t *testing.T) {
	service, mock, cleanup := newUploadService(t, nil)
	defer cleanup()

	expectUploadSession(mock, "session-1", SessionFinalized, "https://github.com/owner/repo/pull/1", "commit-sha")
	expectUploadFiles(mock, "session-1")

	session, files, err := service.Finalize(
		context.Background(),
		"Bearer user-token",
		"org/project",
		repo.Identity{Host: "github.com", Owner: "owner", Repo: "repo"},
		"session-1",
		FinalizeRequest{},
	)
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if session.Status != SessionFinalized || session.PullRequestURL.String != "https://github.com/owner/repo/pull/1" || session.CommitSHA.String != "commit-sha" {
		t.Fatalf("unexpected finalized session: %+v", session)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestFinalizeAdoptsExistingPullRequest(t *testing.T) {
	requests := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/credentials/github":
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode token request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request["access"] != "write" {
				t.Errorf("access = %#v, want write", request["access"])
			}
			_, _ = w.Write([]byte(`{"token":"install-token"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/repos/owner/repo/pulls":
			if got := r.URL.Query().Get("state"); got != "open" {
				t.Errorf("state = %q, want open", got)
			}
			if got := r.URL.Query().Get("head"); got != "owner:branch-1" {
				t.Errorf("head = %q, want owner:branch-1", got)
			}
			if got := r.URL.Query().Get("base"); got != "main" {
				t.Errorf("base = %q, want main", got)
			}
			_, _ = w.Write([]byte(`[{"html_url":"https://github.com/owner/repo/pull/7","head":{"sha":"commit-sha"}}]`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	repoService := repo.NewService(repo.ServiceConfig{
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})
	service, mock, cleanup := newUploadService(t, repoService)
	defer cleanup()

	expectUploadSession(mock, "session-1", SessionReady, "", "")
	expectUploadFiles(mock, "session-1")
	mock.ExpectExec(`INSERT INTO config_schema\.git_upload_session`).WillReturnResult(sqlmock.NewResult(1, 1))

	session, files, err := service.Finalize(
		context.Background(),
		"Bearer user-token",
		"org/project",
		repo.Identity{Host: "github.com", Owner: "owner", Repo: "repo"},
		"session-1",
		FinalizeRequest{},
	)
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if session.Status != SessionFinalized {
		t.Fatalf("status = %q, want %q", session.Status, SessionFinalized)
	}
	if session.PullRequestURL.String != "https://github.com/owner/repo/pull/7" || session.CommitSHA.String != "commit-sha" {
		t.Fatalf("unexpected pull request details: %+v", session)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %v, want token and PR lookup only", requests)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestFinalizeRejectsFinalizedSessionWithoutPullRequestDetails(t *testing.T) {
	service, mock, cleanup := newUploadService(t, nil)
	defer cleanup()

	expectUploadSession(mock, "session-1", SessionFinalized, "", "")
	expectUploadFiles(mock, "session-1")

	_, _, err := service.Finalize(
		context.Background(),
		"Bearer user-token",
		"org/project",
		repo.Identity{Host: "github.com", Owner: "owner", Repo: "repo"},
		"session-1",
		FinalizeRequest{},
	)
	if err == nil {
		t.Fatal("expected conflict error")
	}
	var appErr *domain.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("expected domain error, got %T: %v", err, err)
	}
	if appErr.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", appErr.StatusCode)
	}
}

func newUploadService(t *testing.T, repoService *repo.Service) (*Service, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	return NewService(sqlx.NewDb(db, "sqlmock"), repoService), mock, func() { _ = db.Close() }
}

func expectUploadSession(mock sqlmock.Sqlmock, sessionID string, status string, pullRequestURL string, commitSHA string) {
	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"id", "project_id", "organization", "project", "repo_host", "repo_owner", "repo_name",
		"base_branch", "target_subdirectory", "branch_name", "pr_title", "pr_body", "status",
		"pull_request_url", "commit_sha", "last_error", "created_at", "updated_at",
	}).AddRow(
		sessionID, "org/project", "org", "project", "github.com", "owner", "repo",
		"main", nil, "branch-1", "title", "body", status,
		nullableString(pullRequestURL), nullableString(commitSHA), nil, now, now,
	)
	mock.ExpectQuery(`SELECT .* FROM config_schema\.git_upload_session WHERE id = \$1`).
		WithArgs(sessionID).
		WillReturnRows(rows)
}

func expectUploadFiles(mock sqlmock.Sqlmock, sessionID string) {
	rows := sqlmock.NewRows([]string{
		"session_id", "file_name", "target_path", "size", "checksum", "drs_object_id", "status", "error",
	}).AddRow(sessionID, "file.txt", "dir/file.txt", 123, "abc123", "drs-1", FileUploaded, nil)
	mock.ExpectQuery(`SELECT .* FROM config_schema\.git_upload_session_file WHERE session_id = \$1`).
		WithArgs(sessionID).
		WillReturnRows(rows)
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}
