package git

import (
	"context"
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
