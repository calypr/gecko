package storageaudit

import (
	"context"
	"testing"

	"github.com/calypr/gecko/internal/storageaudit/repoindex"
)

func TestPersistRepoAnalyticsIndexAndLoadExistingDirectoryWithoutLFSFiles(t *testing.T) {
	repo, mirrorPath, refName, hash := buildAnalyticsMirror(t, map[string]string{
		"data/a.txt":        lfsPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 100),
		"plain/notes.txt":   "plain text only\n",
		"plain/nested/x.md": "still plain\n",
	})
	if err := PersistRepoAnalyticsIndex(context.Background(), mirrorPath, repo, refName, hash); err != nil {
		t.Fatalf("persist repo analytics index: %v", err)
	}
	sidecar, err := repoindex.ReadRepoAnalyticsIndexSidecar(mirrorPath)
	if err != nil {
		t.Fatalf("read repo analytics sidecar: %v", err)
	}
	if sidecar.CommitHash != hash.String() {
		t.Fatalf("unexpected sidecar hash: %+v", sidecar)
	}
	index, err := repoindex.LoadOrBuildRepoAnalyticsIndex(context.Background(), mirrorPath, refName, repo, hash)
	if err != nil {
		t.Fatalf("load repo analytics index: %v", err)
	}
	directory, err := repoindex.RepoDirectoryAggregate(index, "plain")
	if err != nil {
		t.Fatalf("lookup plain directory aggregate: %v", err)
	}
	if directory.FileCount != 0 || directory.DirectChildCount != 0 {
		t.Fatalf("expected zero-lfs directory aggregate, got %+v", directory)
	}
	filtered, err := repoindex.FilterRepoInventoryFiles(index, "plain")
	if err != nil {
		t.Fatalf("filter plain directory inventory: %v", err)
	}
	if len(filtered) != 0 {
		t.Fatalf("expected no lfs files under plain directory, got %+v", filtered)
	}
}
