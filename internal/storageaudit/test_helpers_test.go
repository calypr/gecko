package storageaudit

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gitrepo "github.com/calypr/gecko/internal/repo"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func buildAnalyticsMirror(t *testing.T, files map[string]string) (*gogit.Repository, string, string, plumbing.Hash) {
	t.Helper()
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
	for filePath, content := range files {
		fullPath := filepath.Join(sourcePath, filepath.FromSlash(filePath))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filePath, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", filePath, err)
		}
		if _, err := worktree.Add(filePath); err != nil {
			t.Fatalf("add %s: %v", filePath, err)
		}
	}
	if _, err := worktree.Commit("seed analytics repo", &gogit.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.org", When: time.Now()}}); err != nil {
		t.Fatalf("commit analytics repo: %v", err)
	}
	mirrorPath := filepath.Join(tempDir, "mirror.git")
	if err := gitrepo.SyncRepositoryMirror(context.Background(), sourcePath, mirrorPath, nil); err != nil {
		t.Fatalf("sync mirror: %v", err)
	}
	mirrorRepo, err := gitrepo.OpenRepository(mirrorPath)
	if err != nil {
		t.Fatalf("open mirror: %v", err)
	}
	refName, hash, err := gitrepo.ResolveGitReference(mirrorRepo, "", "")
	if err != nil {
		t.Fatalf("resolve ref: %v", err)
	}
	return mirrorRepo, mirrorPath, refName, hash
}

func lfsPointer(checksum string, size int64) string {
	return strings.Join([]string{
		"version https://git-lfs.github.com/spec/v1",
		"oid sha256:" + checksum,
		"size " + strconv.FormatInt(size, 10),
		"",
	}, "\n")
}

func ptrTime(value time.Time) *time.Time {
	copyValue := value
	return &copyValue
}

func ptrBool(value bool) *bool {
	copyValue := value
	return &copyValue
}

func applyFinding(kind string, normalizedPath string, objectIDs []string, bucketURLs []string) CleanupApplyFinding {
	policy := sarecord.RepairPolicyForKind(kind)
	return CleanupApplyFinding{
		Kind:             kind,
		NormalizedPath:   normalizedPath,
		ObjectIDs:        append([]string(nil), objectIDs...),
		BucketObjectURL:  firstString(bucketURLs),
		BucketObjectURLs: append([]string(nil), bucketURLs...),
		AccessURLs:       append([]string(nil), bucketURLs...),
		AvailableActions: append([]string(nil), policy.Actions...),
		DefaultAction:    policy.DefaultAction,
		Evidence: &Evidence{
			ObjectIDs:        append([]string(nil), objectIDs...),
			AccessURLs:       append([]string(nil), bucketURLs...),
			BucketObjectURLs: append([]string(nil), bucketURLs...),
		},
	}
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func assertStringSet(t *testing.T, label string, got []string, want []string) {
	t.Helper()
	got = sarecord.UniqueStrings(got)
	want = sarecord.UniqueStrings(want)
	if len(got) != len(want) {
		t.Fatalf("%s: expected %v, got %v", label, want, got)
	}
	for _, value := range want {
		if !contains(got, value) {
			t.Fatalf("%s: expected %v, got %v", label, want, got)
		}
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsProbeTarget(values []gintegrationsyfon.BulkStorageProbeItem, target string) bool {
	for _, value := range values {
		if value.ObjectURL == target {
			return true
		}
	}
	return false
}

func assertHasDiffFinding(t *testing.T, findings []ProjectDiffFinding, kind string, path string) ProjectDiffFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return finding
		}
	}
	t.Fatalf("missing diff finding kind=%s path=%s in %+v", kind, path, findings)
	return ProjectDiffFinding{}
}

func assertHasCleanupFinding(t *testing.T, findings []CleanupFinding, kind string, path string) {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return
		}
	}
	t.Fatalf("missing cleanup finding kind=%s path=%s in %+v", kind, path, findings)
}

func loadAllChainFindings(t *testing.T, chain *ChainAuditResponse) []ChainFinding {
	t.Helper()
	if chain == nil {
		t.Fatalf("expected chain audit response, got %+v", chain)
	}
	return append([]ChainFinding(nil), chain.Findings...)
}

func assertHasChainFinding(t *testing.T, findings []ChainFinding, kind string, path string) ChainFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.NormalizedPath == path {
			return finding
		}
	}
	t.Fatalf("missing chain finding kind=%s path=%s in %+v", kind, path, findings)
	return ChainFinding{}
}

func assertNoChainFinding(t *testing.T, findings []ChainFinding, kind string) {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind {
			t.Fatalf("unexpected chain finding kind=%s in %+v", kind, findings)
		}
	}
}
