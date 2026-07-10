package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

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
	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "data", mirrorRepo, hash, GitTreeResponseOptions{
		IncludeLFSPointer: true,
		IncludeSize:       true,
	})
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

}

func TestBuildGitTreeResponseDefaultsToCheapFields(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(sourcePath, "README.md"), []byte("hello gecko"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if _, err := worktree.Add("README.md"); err != nil {
		t.Fatalf("add readme: %v", err)
	}
	if _, err := worktree.Commit("initial commit", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit readme: %v", err)
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

	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "", mirrorRepo, hash, GitTreeResponseOptions{})
	if err != nil {
		t.Fatalf("build tree response: %v", err)
	}
	if treeResponse.EntryCount != 1 {
		t.Fatalf("expected entry count 1, got %d", treeResponse.EntryCount)
	}
	if treeResponse.Truncated {
		t.Fatal("expected non-truncated response by default")
	}
	if len(treeResponse.Entries) != 1 {
		t.Fatalf("expected one tree entry, got %+v", treeResponse.Entries)
	}
	if treeResponse.Entries[0].Size != 0 {
		t.Fatalf("expected default tree response to omit size, got %d", treeResponse.Entries[0].Size)
	}
	if treeResponse.Entries[0].LFSPointer != nil {
		t.Fatalf("expected default tree response to omit lfs pointer, got %+v", treeResponse.Entries[0].LFSPointer)
	}
	if treeResponse.Entries[0].LastModifiedAt != nil {
		t.Fatalf("expected default tree response to omit last modified, got %+v", treeResponse.Entries[0].LastModifiedAt)
	}
}

func TestBuildGitTreeResponseHonorsLimitBeforeEnrichment(t *testing.T) {
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
	for name, content := range map[string]string{
		"a.txt": pointerContent,
		"b.txt": pointerContent,
		"c.txt": "regular file",
	} {
		if err := os.WriteFile(filepath.Join(sourcePath, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if _, err := worktree.Add(name); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}
	committedAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	if _, err := worktree.Commit("add files", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: committedAt}}); err != nil {
		t.Fatalf("commit files: %v", err)
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

	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "", mirrorRepo, hash, GitTreeResponseOptions{
		IncludeLFSPointer:   true,
		IncludeLastModified: true,
		IncludeSize:         true,
		Limit:               2,
	})
	if err != nil {
		t.Fatalf("build tree response: %v", err)
	}
	if !treeResponse.Truncated {
		t.Fatal("expected truncated response when limit is smaller than entry count")
	}
	if treeResponse.EntryCount != 3 {
		t.Fatalf("expected total entry count 3, got %d", treeResponse.EntryCount)
	}
	if len(treeResponse.Entries) != 2 {
		t.Fatalf("expected two returned entries, got %+v", treeResponse.Entries)
	}
	if got := []string{treeResponse.Entries[0].Name, treeResponse.Entries[1].Name}; strings.Join(got, ",") != "a.txt,b.txt" {
		t.Fatalf("expected limited page to contain only first two entries, got %v", got)
	}
	for _, entry := range treeResponse.Entries {
		if entry.Size == 0 {
			t.Fatalf("expected size to be included for limited entry %+v", entry)
		}
		if entry.LFSPointer == nil {
			t.Fatalf("expected lfs pointer to be included for limited entry %+v", entry)
		}
		if entry.LastModifiedAt == nil || !entry.LastModifiedAt.Equal(committedAt) {
			t.Fatalf("expected last modified %s for limited entry, got %+v", committedAt, entry.LastModifiedAt)
		}
	}
}

func TestBuildGitTreeResponseBatchesLastModifiedForFilesAndDirectories(t *testing.T) {
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

	initialCommitAt := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	if err := os.MkdirAll(filepath.Join(sourcePath, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	for path, content := range map[string]string{
		"README.md":  "readme",
		"data/a.txt": "a",
	} {
		fullPath := filepath.Join(sourcePath, filepath.FromSlash(path))
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		if _, err := worktree.Add(path); err != nil {
			t.Fatalf("add %s: %v", path, err)
		}
	}
	if _, err := worktree.Commit("initial files", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: initialCommitAt}}); err != nil {
		t.Fatalf("commit initial files: %v", err)
	}

	directoryCommitAt := time.Date(2026, 7, 6, 11, 0, 0, 0, time.UTC)
	nestedPath := filepath.Join(sourcePath, "data", "nested", "b.txt")
	if err := os.MkdirAll(filepath.Dir(nestedPath), 0o755); err != nil {
		t.Fatalf("create nested dir: %v", err)
	}
	if err := os.WriteFile(nestedPath, []byte("b"), 0o644); err != nil {
		t.Fatalf("write nested file: %v", err)
	}
	if _, err := worktree.Add("data/nested/b.txt"); err != nil {
		t.Fatalf("add nested file: %v", err)
	}
	if _, err := worktree.Commit("add nested file", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: directoryCommitAt}}); err != nil {
		t.Fatalf("commit nested file: %v", err)
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

	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "", mirrorRepo, hash, GitTreeResponseOptions{
		IncludeLastModified: true,
	})
	if err != nil {
		t.Fatalf("build tree response: %v", err)
	}
	if len(treeResponse.Entries) != 2 {
		t.Fatalf("expected data dir and readme entries, got %+v", treeResponse.Entries)
	}

	entriesByPath := make(map[string]GitTreeEntry, len(treeResponse.Entries))
	for _, entry := range treeResponse.Entries {
		entriesByPath[entry.Path] = entry
	}
	assertTreeLastModified(t, entriesByPath["data"], directoryCommitAt)
	assertTreeLastModified(t, entriesByPath["README.md"], initialCommitAt)

	lastModifiedByPath, err := lookupGitPathsLastModified(mirrorRepo, hash, treeResponse.Entries)
	if err != nil {
		t.Fatalf("lookup batched last modified: %v", err)
	}
	if got := lastModifiedByPath["data"]; !got.Equal(directoryCommitAt) {
		t.Fatalf("expected batched data timestamp %s, got %s", directoryCommitAt, got)
	}
	if got := lastModifiedByPath["README.md"]; !got.Equal(initialCommitAt) {
		t.Fatalf("expected batched readme timestamp %s, got %s", initialCommitAt, got)
	}
}

func assertTreeLastModified(t *testing.T, entry GitTreeEntry, expected time.Time) {
	t.Helper()
	if entry.LastModifiedAt == nil {
		t.Fatalf("expected %s to have last modified timestamp", entry.Path)
	}
	if !entry.LastModifiedAt.Equal(expected) {
		t.Fatalf("expected %s last modified %s, got %s", entry.Path, expected, *entry.LastModifiedAt)
	}
}
