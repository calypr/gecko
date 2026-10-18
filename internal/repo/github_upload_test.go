package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
)

func TestCreateGitHubUploadPullRequest_PropagatesGitHub403(t *testing.T) {
	var tokenRequest map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/credentials/github":
			if err := json.NewDecoder(r.Body).Decode(&tokenRequest); err != nil {
				t.Fatalf("decode token request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"test-token","expires_at":"2030-01-01T00:00:00Z","repository":{"owner":"EllrottLab","repo":"git_drs_test"}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/ref/heads/main"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":{"sha":"base-sha"}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/commits/base-sha"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tree":{"sha":"tree-sha"}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/trees"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		FenceBaseURL:  server.URL,
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})

	_, _, err := service.CreateGitHubUploadPullRequest(
		context.Background(),
		"Bearer user-token",
		"Ellrott_Lab",
		"test",
		Identity{Owner: "EllrottLab", Repo: "git_drs_test"},
		"main",
		"feature/test",
		"title",
		"body",
		[]geckodb.GitUploadSessionFile{{
			TargetPath: "dir/file.txt",
			Size:       123,
			Checksum:   sql.NullString{String: "abc123", Valid: true},
		}},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	statusErr, ok := err.(*domain.HTTPStatusError)
	if !ok {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", statusErr.StatusCode)
	}
	if !strings.Contains(statusErr.Message, "failed to create GitHub tree") {
		t.Fatalf("unexpected message: %q", statusErr.Message)
	}
	if tokenRequest["access"] != "write" {
		t.Fatalf("expected write access token request, got %#v", tokenRequest)
	}
}

func TestCreateGitHubUploadPullRequestCreatesTreeCommitBranchAndPullRequest(t *testing.T) {
	var tokenRequest map[string]string
	var githubRequests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/credentials/github":
			if err := json.NewDecoder(r.Body).Decode(&tokenRequest); err != nil {
				t.Errorf("decode token request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"token":"install-token"}`))

		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/ref/heads/main"):
			githubRequests = append(githubRequests, "GET ref")
			_, _ = w.Write([]byte(`{"object":{"sha":"base-sha"}}`))

		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/commits/base-sha"):
			githubRequests = append(githubRequests, "GET commit")
			_, _ = w.Write([]byte(`{"sha":"base-sha","tree":{"sha":"base-tree-sha"}}`))

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/trees"):
			githubRequests = append(githubRequests, "POST tree")
			var payload struct {
				BaseTree string `json:"base_tree"`
				Tree     []struct {
					Path    string `json:"path"`
					Mode    string `json:"mode"`
					Type    string `json:"type"`
					Content string `json:"content"`
				} `json:"tree"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode tree request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload.BaseTree != "base-tree-sha" {
				t.Errorf("base tree = %q, want base-tree-sha", payload.BaseTree)
			}
			if len(payload.Tree) != 1 {
				t.Errorf("tree entries = %d, want 1", len(payload.Tree))
			} else {
				entry := payload.Tree[0]
				if entry.Path != "dir/file.txt" || entry.Mode != "100644" || entry.Type != "blob" {
					t.Errorf("unexpected tree entry: %+v", entry)
				}
				if entry.Content != BuildLFSPointerContent("abc123", 123) {
					t.Errorf("tree content = %q", entry.Content)
				}
			}
			_, _ = w.Write([]byte(`{"sha":"new-tree-sha"}`))

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/commits"):
			githubRequests = append(githubRequests, "POST commit")
			var payload struct {
				Message string   `json:"message"`
				Tree    string   `json:"tree"`
				Parents []string `json:"parents"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode commit request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload.Message != "title" || payload.Tree != "new-tree-sha" {
				t.Errorf("unexpected commit payload: %+v", payload)
			}
			if len(payload.Parents) != 1 || payload.Parents[0] != "base-sha" {
				t.Errorf("unexpected commit parents: %+v", payload.Parents)
			}
			_, _ = w.Write([]byte(`{"sha":"new-commit-sha"}`))

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/git/refs"):
			githubRequests = append(githubRequests, "POST ref")
			var payload struct {
				Ref string `json:"ref"`
				SHA string `json:"sha"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode ref request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload.Ref != "refs/heads/feature/test" || payload.SHA != "new-commit-sha" {
				t.Errorf("unexpected ref payload: %+v", payload)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ref":"refs/heads/feature/test","object":{"sha":"new-commit-sha"}}`))

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v3/repos/EllrottLab/git_drs_test/pulls"):
			githubRequests = append(githubRequests, "POST pull")
			var payload struct {
				Title string `json:"title"`
				Body  string `json:"body"`
				Base  string `json:"base"`
				Head  string `json:"head"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode pull request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload.Title != "title" || payload.Body != "body" || payload.Base != "main" || payload.Head != "feature/test" {
				t.Errorf("unexpected pull request payload: %+v", payload)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"html_url":"https://github.com/EllrottLab/git_drs_test/pull/7","number":7}`))

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})

	commitSHA, pullRequestURL, err := service.CreateGitHubUploadPullRequest(
		context.Background(),
		"Bearer user-token",
		"Ellrott_Lab",
		"test",
		Identity{Owner: "EllrottLab", Repo: "git_drs_test"},
		"main",
		"feature/test",
		"title",
		"body",
		[]geckodb.GitUploadSessionFile{{
			TargetPath: "dir/file.txt",
			Size:       123,
			Checksum:   sql.NullString{String: "abc123", Valid: true},
		}},
	)
	if err != nil {
		t.Fatalf("create GitHub upload pull request: %v", err)
	}
	if commitSHA != "new-commit-sha" {
		t.Fatalf("commit sha = %q, want new-commit-sha", commitSHA)
	}
	if pullRequestURL != "https://github.com/EllrottLab/git_drs_test/pull/7" {
		t.Fatalf("pull request url = %q", pullRequestURL)
	}
	if tokenRequest["access"] != "write" {
		t.Fatalf("token access = %q, want write", tokenRequest["access"])
	}
	wantRequests := []string{"GET ref", "GET commit", "POST tree", "POST commit", "POST ref", "POST pull"}
	if len(githubRequests) != len(wantRequests) {
		t.Fatalf("github requests = %v, want %v", githubRequests, wantRequests)
	}
	for i := range wantRequests {
		if githubRequests[i] != wantRequests[i] {
			t.Fatalf("github request %d = %q, want %q", i, githubRequests[i], wantRequests[i])
		}
	}
}

func TestCreateGitHubUploadPullRequestReturnsFenceTokenError(t *testing.T) {
	githubRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/credentials/github" {
			githubRequests++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"installation token denied"}`))
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})

	_, _, err := service.CreateGitHubUploadPullRequest(
		context.Background(),
		"Bearer user-token",
		"Ellrott_Lab",
		"test",
		Identity{Owner: "EllrottLab", Repo: "git_drs_test"},
		"main",
		"feature/test",
		"title",
		"body",
		nil,
	)
	if err == nil {
		t.Fatal("expected token error")
	}
	var statusErr *domain.HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusForbidden || statusErr.Code != "forbidden" {
		t.Fatalf("unexpected error: %+v", statusErr)
	}
	if !strings.Contains(statusErr.Message, "installation token denied") {
		t.Fatalf("unexpected error message: %q", statusErr.Message)
	}
	if githubRequests != 0 {
		t.Fatalf("expected no GitHub requests after token failure, got %d", githubRequests)
	}
}
