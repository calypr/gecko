package presentation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type FilesystemStore struct {
	dataDir string
}

func NewFilesystemStore(dataDir string) *FilesystemStore {
	return &FilesystemStore{dataDir: strings.TrimSpace(dataDir)}
}

func (s *FilesystemStore) projectPresentationDir(organization string) string {
	return filepath.Join(
		s.dataDir,
		ProjectPresentationDirectory,
		sanitizePathPart(strings.TrimSpace(organization)),
	)
}

func (s *FilesystemStore) ProjectPresentationPath(organization string, project string) string {
	return filepath.Join(
		s.projectPresentationDir(organization),
		sanitizePathPart(strings.TrimSpace(project))+"_presentation.html",
	)
}

func (s *FilesystemStore) Get(organization string, project string) (string, error) {
	if s.dataDir == "" {
		return "", ErrDataDirRequired
	}
	data, err := os.ReadFile(s.ProjectPresentationPath(organization, project))
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNoPresentation
		}
		return "", fmt.Errorf("read presentation HTML: %w", err)
	}
	return string(data), nil
}

func (s *FilesystemStore) Save(organization string, project string, content string) error {
	if s.dataDir == "" {
		return ErrDataDirRequired
	}
	dir := s.projectPresentationDir(organization)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create presentation directory: %w", err)
	}
	path := s.ProjectPresentationPath(organization, project)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write presentation HTML: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist presentation HTML: %w", err)
	}
	return nil
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}
