package git

import (
	"context"
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
	treeResponse, err := BuildGitTreeResponse("org-a/proj-a", refName, "", mirrorRepo, hash, GitTreeResponseOptions{})
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
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(sourcePath, name), []byte(name), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if _, err := worktree.Add(name); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}
	if _, err := worktree.Commit("add files", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
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
		IncludeSize: true,
		Limit:       2,
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
	for _, entry := range treeResponse.Entries {
		if entry.Size == 0 {
			t.Fatalf("expected size to be included for limited entry %+v", entry)
		}
	}
}

func TestBuildGitManifestResponseRecursesAndPaginates(t *testing.T) {
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
	for path, content := range map[string]string{
		"README.md":           "hello",
		"data/a.txt":          "a",
		"data/nested/b.txt":   "b",
		"data/nested/c/c.txt": "c",
	} {
		fullPath := filepath.Join(sourcePath, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		if _, err := worktree.Add(path); err != nil {
			t.Fatalf("add %s: %v", path, err)
		}
	}
	if _, err := worktree.Commit("add files", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
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

	firstPage, err := BuildGitManifestResponse("org-a/proj-a", refName, "data", mirrorRepo, hash, GitManifestResponseOptions{
		FilesOnly: true,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("build first manifest page: %v", err)
	}
	if firstPage.EntryCount != 2 {
		t.Fatalf("expected two entries on first page, got %d", firstPage.EntryCount)
	}
	if !firstPage.HasMore || strings.TrimSpace(firstPage.NextCursor) == "" {
		t.Fatalf("expected pagination cursor, got %+v", firstPage)
	}
	if got := []string{firstPage.Entries[0].Path, firstPage.Entries[1].Path}; strings.Join(got, ",") != "data/a.txt,data/nested/b.txt" {
		t.Fatalf("unexpected first page paths: %v", got)
	}

	secondPage, err := BuildGitManifestResponse("org-a/proj-a", refName, "data", mirrorRepo, hash, GitManifestResponseOptions{
		FilesOnly: true,
		Limit:     2,
		Cursor:    firstPage.NextCursor,
	})
	if err != nil {
		t.Fatalf("build second manifest page: %v", err)
	}
	if secondPage.HasMore {
		t.Fatalf("expected second page to be terminal, got %+v", secondPage)
	}
	if got := []string{secondPage.Entries[0].Path}; strings.Join(got, ",") != "data/nested/c/c.txt" {
		t.Fatalf("unexpected second page paths: %v", got)
	}
}

func TestBuildGitManifestResponseIncludesDirectoriesWhenRequested(t *testing.T) {
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
	fullPath := filepath.Join(sourcePath, "data", "nested", "b.txt")
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("b"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := worktree.Add("data/nested/b.txt"); err != nil {
		t.Fatalf("add file: %v", err)
	}
	if _, err := worktree.Commit("add file", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
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

	manifest, err := BuildGitManifestResponse("org-a/proj-a", refName, "", mirrorRepo, hash, GitManifestResponseOptions{
		FilesOnly: false,
		Limit:     20,
	})
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	if len(manifest.Entries) != 3 {
		t.Fatalf("expected tree+tree+blob entries, got %+v", manifest.Entries)
	}
	if manifest.Entries[0].Type != "tree" || manifest.Entries[1].Type != "tree" || manifest.Entries[2].Type != "blob" {
		t.Fatalf("unexpected manifest entry ordering: %+v", manifest.Entries)
	}
}
