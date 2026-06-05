package git_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
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

	service := git.NewGitService(git.GitServiceConfig{
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
		git.GitRepositoryIdentity{Owner: "EllrottLab", Repo: "git_drs_test"},
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
	statusErr, ok := err.(*git.HTTPStatusError)
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
