package git

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
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
	if fileResponse.Name != "README.md" {
		t.Fatalf("expected README.md file name, got %q", fileResponse.Name)
	}
	if fileResponse.Hash == "" {
		t.Fatal("expected file hash to be populated")
	}
}

func TestBuildGitRefsResponseIncludesRemoteBranches(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source")
	repo, err := gogit.PlainInit(sourcePath, false)
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("load worktree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "README.md"), []byte("main branch"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if _, err := worktree.Add("README.md"); err != nil {
		t.Fatalf("add readme: %v", err)
	}
	if _, err := worktree.Commit("main commit", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit readme: %v", err)
	}
	headRef, err := repo.Head()
	if err != nil {
		t.Fatalf("read initial head: %v", err)
	}
	defaultBranch := headRef.Name().Short()
	if err := worktree.Checkout(&gogit.CheckoutOptions{
		Branch: plumbing.NewBranchReferenceName("benchmarking"),
		Create: true,
	}); err != nil {
		t.Fatalf("checkout benchmarking branch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "benchmark.txt"), []byte("branch file"), 0o644); err != nil {
		t.Fatalf("write branch file: %v", err)
	}
	if _, err := worktree.Add("benchmark.txt"); err != nil {
		t.Fatalf("add branch file: %v", err)
	}
	if _, err := worktree.Commit("benchmark branch commit", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit branch file: %v", err)
	}
	if err := worktree.Checkout(&gogit.CheckoutOptions{
		Branch: plumbing.NewBranchReferenceName(defaultBranch),
	}); err != nil {
		t.Fatalf("checkout default branch: %v", err)
	}

	mirrorPath := filepath.Join(tempDir, "mirror.git")
	if err := SyncRepositoryMirror(context.Background(), sourcePath, mirrorPath, nil); err != nil {
		t.Fatalf("sync mirror: %v", err)
	}
	mirrorRepo, err := OpenRepository(mirrorPath)
	if err != nil {
		t.Fatalf("open mirror: %v", err)
	}
	refsResponse, err := BuildGitRefsResponse("org-a/proj-a", defaultBranch, mirrorRepo)
	if err != nil {
		t.Fatalf("build refs response: %v", err)
	}
	branchNames := make([]string, 0, len(refsResponse.Refs))
	for _, ref := range refsResponse.Refs {
		if ref.Type == "branch" {
			branchNames = append(branchNames, ref.Name)
		}
	}
	if len(branchNames) < 2 {
		t.Fatalf("expected multiple branches in refs response, got %+v", refsResponse.Refs)
	}
	foundBenchmarking := false
	for _, branchName := range branchNames {
		if branchName == "benchmarking" {
			foundBenchmarking = true
			break
		}
	}
	if !foundBenchmarking {
		t.Fatalf("expected benchmarking branch in refs response, got %+v", refsResponse.Refs)
	}

	refName, hash, err := ResolveGitReference(mirrorRepo, "benchmarking", defaultBranch)
	if err != nil {
		t.Fatalf("resolve benchmarking branch: %v", err)
	}
	if refName != "benchmarking" {
		t.Fatalf("expected resolved ref name benchmarking, got %q", refName)
	}
	fileResponse, err := BuildGitFileResponse("org-a/proj-a", refName, "benchmark.txt", mirrorRepo, hash)
	if err != nil {
		t.Fatalf("build branch file response: %v", err)
	}
	if fileResponse.Name != "benchmark.txt" {
		t.Fatalf("expected benchmark.txt file name, got %q", fileResponse.Name)
	}
	if fileResponse.Hash == "" {
		t.Fatal("expected branch file hash to be populated")
	}
}

func TestBuildGitResponsesDetectLFSPointers(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source")
	repo, err := gogit.PlainInit(sourcePath, false)
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("load worktree: %v", err)
	}
	pointerContent := strings.Join([]string{
		"version https://git-lfs.github.com/spec/v1",
		"oid sha256:0bfab2917ce05007ff6297c0ec93ef575209210e4ca998dbd243a270e2f9ca83",
		"size 3780184021",
		"",
	}, "\n")
	if err := os.MkdirAll(filepath.Join(sourcePath, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "data", "tcga.tumor.ensembl.tsv"), []byte(pointerContent), 0o644); err != nil {
		t.Fatalf("write lfs pointer file: %v", err)
	}
	if _, err := worktree.Add("data/tcga.tumor.ensembl.tsv"); err != nil {
		t.Fatalf("add lfs pointer file: %v", err)
	}
	if _, err := worktree.Commit("add lfs pointer", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit lfs pointer file: %v", err)
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
	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "data", mirrorRepo, hash)
	if err != nil {
		t.Fatalf("build tree response: %v", err)
	}
	if len(treeResponse.Entries) != 1 {
		t.Fatalf("expected one tree entry, got %+v", treeResponse.Entries)
	}
	treePointer := treeResponse.Entries[0].LFSPointer
	if treePointer == nil {
		t.Fatalf("expected tree entry to be marked as lfs pointer, got %+v", treeResponse.Entries[0])
	}
	if treePointer.OID != "0bfab2917ce05007ff6297c0ec93ef575209210e4ca998dbd243a270e2f9ca83" {
		t.Fatalf("unexpected lfs oid: %q", treePointer.OID)
	}
	if treePointer.Size != 3780184021 {
		t.Fatalf("unexpected lfs size: %d", treePointer.Size)
	}

	fileResponse, err := BuildGitFileResponse("org-a/proj-a", refName, "data/tcga.tumor.ensembl.tsv", mirrorRepo, hash)
	if err != nil {
		t.Fatalf("build file response: %v", err)
	}
	if fileResponse.LFSPointer == nil {
		t.Fatalf("expected file response to include lfs pointer metadata")
	}
	if fileResponse.LFSPointer.OID != treePointer.OID {
		t.Fatalf("expected matching lfs oid, got %q and %q", fileResponse.LFSPointer.OID, treePointer.OID)
	}
}

func TestRequestOrganizationInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
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
	status, err := service.RequestOrganizationInstallationStatus(context.Background(), "Bearer user-token", "HTAN_INT", "htan-int-github")
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
	if receivedBody["action"] != "organization_installation" {
		t.Fatalf("expected organization_installation action, got %#v", receivedBody)
	}
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization payload, got %#v", receivedBody)
	}
	if receivedBody["owner"] != "htan-int-github" {
		t.Fatalf("expected owner payload, got %#v", receivedBody)
	}
}

func TestRequestInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
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
	status, err := service.RequestInstallationStatus(context.Background(), "Bearer user-token", "HTAN_INT", GitRepositoryIdentity{
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

func TestListInstallationRepositoriesFromFenceForwardsAuthorizationAndParsesRepositories(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
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
			"installation_id": 42,
			"repositories": []map[string]any{
				{
					"id":        101,
					"name":      "git_drs_test",
					"full_name": "Ellrott_Lab/git_drs_test",
					"html_url":  "https://github.com/EllrottLab/git_drs_test",
					"clone_url": "https://github.com/EllrottLab/git_drs_test.git",
				},
			},
		})
	}))
	defer server.Close()

	service := NewGitService(GitServiceConfig{
		FenceBaseURL: server.URL,
		HTTPClient:   server.Client(),
	})
	repositories, err := service.listInstallationRepositoriesFromFence(context.Background(), "Bearer user-token", 42)
	if err != nil {
		t.Fatalf("list installation repositories: %v", err)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["action"] != "installation_repositories" {
		t.Fatalf("expected installation_repositories action, got %#v", receivedBody)
	}
	if receivedBody["installation_id"] != float64(42) {
		t.Fatalf("expected installation id in request body, got %#v", receivedBody)
	}
	if len(repositories) != 1 {
		t.Fatalf("expected one repository, got %+v", repositories)
	}
	if repositories[0].FullName != "Ellrott_Lab/git_drs_test" {
		t.Fatalf("unexpected repository: %+v", repositories[0])
	}
}

func TestRequestInstallationTokenForwardsAuthorizationAndParsesToken(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
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
	token, err := service.RequestInstallationToken(context.Background(), "Bearer user-token", "HTAN_INT", "BForePC", GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	}, "write")
	if err != nil {
		t.Fatalf("request installation token: %v", err)
	}
	if token != "ghs_test" {
		t.Fatalf("expected token ghs_test, got %q", token)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["access"] != "write" {
		t.Fatalf("expected write access request, got %#v", receivedBody)
	}
	if receivedBody["action"] != "installation_token" {
		t.Fatalf("expected installation_token action, got %#v", receivedBody)
	}
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization HTAN_INT, got %#v", receivedBody)
	}
	if receivedBody["project"] != "BForePC" {
		t.Fatalf("expected project BForePC, got %#v", receivedBody)
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
	_, err := service.RequestInstallationToken(context.Background(), "Bearer user-token", "HTAN_INT", "BForePC", GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	}, "read")
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
		if request.URL.Path != "/credentials/github" {
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
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization HTAN_INT, got %q", receivedBody["organization"])
	}
	if receivedBody["action"] != "install_url" {
		t.Fatalf("expected install_url action, got %#v", receivedBody)
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
