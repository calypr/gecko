package presentation

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemStoreUsesOrgProjectFilenameLayout(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFilesystemStore(tempDir)

	expectedPath := filepath.Join(tempDir, ProjectPresentationDirectory, "Org_A", "Project_1_presentation.html")
	if got := store.ProjectPresentationPath("Org A", "Project/1"); got != expectedPath {
		t.Fatalf("unexpected presentation path: got %q want %q", got, expectedPath)
	}
}

func TestFilesystemStoreSaveAndGet(t *testing.T) {
	tempDir := t.TempDir()
	store := NewFilesystemStore(tempDir)

	if err := store.Save("org-a", "proj-a", "<p>Hello</p>"); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	got, err := store.Get("org-a", "proj-a")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != "<p>Hello</p>" {
		t.Fatalf("unexpected content: %q", got)
	}
	if _, err := os.Stat(store.ProjectPresentationPath("org-a", "proj-a")); err != nil {
		t.Fatalf("expected stored file: %v", err)
	}
}

func TestFilesystemStoreGetMissing(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	_, err := store.Get("org-a", "proj-a")
	if err != ErrNoPresentation {
		t.Fatalf("expected ErrNoPresentation, got %v", err)
	}
}
