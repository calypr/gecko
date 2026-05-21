package git

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func TestSyncRepositoryMirrorPullsUpdatesAndReadsTree(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source")
	repo, err := gogit.PlainInit(sourcePath, false)
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "README.md"), []byte("hello gecko"), 0o644); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("load worktree: %v", err)
	}
	if _, err := worktree.Add("README.md"); err != nil {
		t.Fatalf("add file: %v", err)
	}
	if _, err := worktree.Commit("initial commit", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit file: %v", err)
	}

	mirrorPath := filepath.Join(tempDir, "mirror.git")
	if err := SyncRepositoryMirror(context.Background(), sourcePath, mirrorPath, nil); err != nil {
		t.Fatalf("sync mirror: %v", err)
	}
	mirrorRepo, err := OpenRepository(mirrorPath)
	if err != nil {
		t.Fatalf("open mirror: %v", err)
	}
	refName, hash, err := ResolveGitReference(mirrorRepo, "", "")
	if err != nil {
		t.Fatalf("resolve HEAD: %v", err)
	}
	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "", mirrorRepo, hash)
	if err != nil {
		t.Fatalf("build tree response: %v", err)
	}
	if len(treeResponse.Entries) != 1 || treeResponse.Entries[0].Name != "README.md" {
		t.Fatalf("unexpected tree entries: %+v", treeResponse.Entries)
	}

	if err := os.WriteFile(filepath.Join(sourcePath, "README.md"), []byte("hello gecko updated"), 0o644); err != nil {
		t.Fatalf("update repo file: %v", err)
	}
	if _, err := worktree.Add("README.md"); err != nil {
		t.Fatalf("add updated file: %v", err)
	}
	if _, err := worktree.Commit("update readme", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit updated file: %v", err)
	}
	if err := SyncRepositoryMirror(context.Background(), sourcePath, mirrorPath, nil); err != nil {
		t.Fatalf("pull mirror update: %v", err)
	}
	mirrorRepo, err = OpenRepository(mirrorPath)
	if err != nil {
		t.Fatalf("open updated mirror: %v", err)
	}
	refName, hash, err = ResolveGitReference(mirrorRepo, "", "")
	if err != nil {
		t.Fatalf("resolve updated HEAD: %v", err)
	}
	fileResponse, err := BuildGitFileResponse("org-a/proj-a", refName, "README.md", mirrorRepo, hash)
	if err != nil {
		t.Fatalf("build updated file response: %v", err)
	}
	if fileResponse.Content != "hello gecko updated" {
		t.Fatalf("expected pulled file content, got %q", fileResponse.Content)
	}
}

func TestRequestOrganizationInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github/organization-installation" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installed":            true,
			"organization":         "HTAN_INT",
			"installation_id":      42,
			"target":               "HTAN_INT",
			"target_type":          "Organization",
			"html_url":             "https://github.com/organizations/HTAN_INT/settings/installations/42",
			"repository_selection": "selected",
		})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{FenceBaseURL: server.URL, HTTPClient: server.Client()})
	status, err := service.RequestOrganizationInstallationStatus(context.Background(), "Bearer user-token", "HTAN_INT")
	if err != nil {
		t.Fatalf("request organization installation status: %v", err)
	}
	if !status.Installed {
		t.Fatal("expected installed status")
	}
	if status.InstallationID == nil || *status.InstallationID != 42 {
		t.Fatalf("unexpected installation id: %+v", status.InstallationID)
	}
	if status.RepositorySelection != "selected" {
		t.Fatalf("unexpected repository selection: %q", status.RepositorySelection)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
}

func TestRequestInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github/installation" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installed":       true,
			"installation_id": 42,
			"target":          "HTAN_INT",
			"target_type":     "Organization",
			"html_url":        "https://github.com/organizations/HTAN_INT/settings/installations/42",
		})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	status, err := service.RequestInstallationStatus(context.Background(), "Bearer user-token", GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	})
	if err != nil {
		t.Fatalf("request installation status: %v", err)
	}
	if !status.Installed {
		t.Fatal("expected installed status")
	}
	if status.InstallationID == nil || *status.InstallationID != 42 {
		t.Fatalf("unexpected installation id: %+v", status.InstallationID)
	}
	if status.Target != "HTAN_INT" {
		t.Fatalf("unexpected target: %q", status.Target)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
}

func TestRequestInstallationTokenForwardsAuthorizationAndParsesToken(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github/token" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"token":      "ghs_test",
			"expires_at": "2026-05-20T18:00:00Z",
			"repository": map[string]string{"owner": "HTAN_INT", "repo": "BForePC"},
		})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	token, err := service.RequestInstallationToken(context.Background(), "Bearer user-token", GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	})
	if err != nil {
		t.Fatalf("request installation token: %v", err)
	}
	if token != "ghs_test" {
		t.Fatalf("expected token ghs_test, got %q", token)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
}

func TestRequestInstallationTokenReturnsFenceStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(writer).Encode(map[string]any{"message": "forbidden by fence"})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	_, err := service.RequestInstallationToken(context.Background(), "Bearer user-token", GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	statusErr, ok := err.(*HTTPStatusError)
	if !ok {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", statusErr.StatusCode)
	}
	if statusErr.Message != "forbidden by fence" {
		t.Fatalf("unexpected status error message: %q", statusErr.Message)
	}
}

func TestRequestInstallationURLForwardsAuthorizationAndParsesInstallURL(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github/install-url" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"install_url": "https://github.com/apps/calypr-github/installations/new?state=abc",
			"owner":       "HTAN_INT",
		})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	redirectURL, err := service.RequestInstallationURL(context.Background(), "Bearer user-token", "HTAN_INT", "/git/HTAN_INT")
	if err != nil {
		t.Fatalf("request installation URL: %v", err)
	}
	if redirectURL != "https://github.com/apps/calypr-github/installations/new?state=abc" {
		t.Fatalf("unexpected redirect URL: %q", redirectURL)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["owner"] != "HTAN_INT" {
		t.Fatalf("expected owner HTAN_INT, got %q", receivedBody["owner"])
	}
	if receivedBody["redirect_path"] != "/git/HTAN_INT" {
		t.Fatalf("expected redirect_path /git/HTAN_INT, got %q", receivedBody["redirect_path"])
	}
}

func TestRequestInstallationURLReturnsFenceStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(writer).Encode(map[string]any{"message": "forbidden by fence"})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	_, err := service.RequestInstallationURL(context.Background(), "Bearer user-token", "HTAN_INT", "/git/HTAN_INT")
	if err == nil {
		t.Fatal("expected error")
	}
	statusErr, ok := err.(*HTTPStatusError)
	if !ok {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", statusErr.StatusCode)
	}
	if statusErr.Message != "forbidden by fence" {
		t.Fatalf("unexpected status error message: %q", statusErr.Message)
	}
}
