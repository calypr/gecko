package apiserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/jmoiron/sqlx"
)

func TestPointerInventoryRequiresProjectRead(t *testing.T) {
	const route = "/git/projects/org/proj/pointer-inventory?commit="
	for _, tc := range []struct {
		name, token, commit string
		authz               *fakeAuthz
		status              int
	}{
		{"missing token", "", strings.Repeat("a", 40), &fakeAuthz{}, http.StatusUnauthorized},
		{"different project", "Bearer token", strings.Repeat("a", 40), &fakeAuthz{resources: []any{"/programs/org/projects/other"}}, http.StatusForbidden},
		{"authorization unavailable", "Bearer token", strings.Repeat("a", 40), nil, http.StatusServiceUnavailable},
		{"branch name", "Bearer token", "main", &fakeAuthz{resources: []any{"/programs/org/projects/proj"}}, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &Server{}
			if tc.authz != nil {
				server.Authz = tc.authz
			}
			request := httptest.NewRequest(http.MethodGet, route+tc.commit, nil)
			request.Header.Set("Authorization", tc.token)
			response := serveAPI(NewRouter(server), request)
			defer response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, tc.status)
			}
		})
	}
}

func TestPointerInventoryUsesExactCommitAndExcludesMetadata(t *testing.T) {
	root := t.TempDir()
	mirror, err := git.PlainInit(root, false)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := mirror.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		t.Helper()
		fullPath := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := worktree.Add(name); err != nil {
			t.Fatal(err)
		}
	}
	author := &object.Signature{Name: "Test", Email: "test@example.com", When: time.Unix(100, 0)}
	sha := strings.Repeat("a", 64)
	pointer := repo.BuildLFSPointerContent(sha, 1234)
	write("data/nested/first.bin", pointer)
	write("data/second.bin", pointer)
	write("META/DocumentReference.ndjson", pointer)
	write("CONFIG/proj.json", pointer)
	write("ordinary.txt", strings.Repeat("ordinary text ", 20))
	commit, err := worktree.Commit("pinned", &git.CommitOptions{Author: author})
	if err != nil {
		t.Fatal(err)
	}
	write("data/new.bin", repo.BuildLFSPointerContent(strings.Repeat("b", 64), 2))
	if _, err := worktree.Commit("newer", &git.CommitOptions{Author: author}); err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 2 {
		mock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("org/proj").
			WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("org/proj", []byte(`{"src_repo":"https://github.com/owner/repo"}`)))
	}
	service := project.NewReadService(sqlx.NewDb(db, "sqlmock"), &refreshingGitRepository{
		state: &geckodb.GitProjectState{
			ProjectID: "org/proj", RepoHost: "github.com", RepoOwner: "owner", RepoName: "repo", MirrorPath: root,
			SyncState: "ready", DefaultBranch: sql.NullString{String: "master", Valid: true},
		},
	})
	router := NewRouter(&Server{ProjectRead: service, Authz: fakeAuthz{resources: []any{"/programs/org/projects/proj"}}})
	fetch := func(sha string) *http.Response {
		request := httptest.NewRequest(http.MethodGet, "/git/projects/org/proj/pointer-inventory?commit="+sha, nil)
		request.Header.Set("Authorization", "Bearer token")
		return serveAPI(router, request)
	}
	response := fetch(commit.String())
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var got []repo.DataPointer
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	want := []repo.DataPointer{{Path: "data/nested/first.bin", SHA256: sha, Size: 1234}, {Path: "data/second.bin", SHA256: sha, Size: 1234}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pointers = %+v, want %+v", got, want)
	}
	missing := fetch(strings.Repeat("c", 40))
	defer missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing commit status = %d", missing.StatusCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPointerInventoryReportsIncompleteStreamOnReadFailure(t *testing.T) {
	readError := errors.New("unreadable blob")
	response := pointerInventoryResponse{walk: func(emit func(repo.DataPointer) error) error {
		if err := emit(repo.DataPointer{Path: "data/file", SHA256: strings.Repeat("a", 64), Size: 5}); err != nil {
			return err
		}
		return readError
	}}
	recorder := httptest.NewRecorder()
	if err := response.VisitGetGitProjectPointerInventoryResponse(recorder); !errors.Is(err, readError) {
		t.Fatalf("response error = %v, want %v", err, readError)
	}
	if json.Valid(recorder.Body.Bytes()) {
		t.Fatalf("incomplete stream was presented as complete: %s", recorder.Body)
	}
}
