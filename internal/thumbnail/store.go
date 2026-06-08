package thumbnail

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type FilesystemStore struct {
	dataDir string
}

// NewFilesystemStore constructs a disk-backed thumbnail store.
func NewFilesystemStore(dataDir string) *FilesystemStore {
	return &FilesystemStore{
		dataDir: strings.TrimSpace(dataDir),
	}
}

func (s *FilesystemStore) ProjectThumbnailDir(organization string, project string) string {
	return filepath.Join(
		s.dataDir,
		ProjectThumbnailDirectory,
		sanitizePathPart(strings.TrimSpace(organization)),
		sanitizePathPart(strings.TrimSpace(project)),
	)
}

func (s *FilesystemStore) projectThumbnailPath(organization string, project string, extension string) string {
	return filepath.Join(
		s.ProjectThumbnailDir(organization, project),
		ProjectThumbnailBaseName+extension,
	)
}

func (s *FilesystemStore) GetPath(organization string, project string) (string, string, error) {
	if s.dataDir == "" {
		return "", "", ErrDataDirRequired
	}
	dir := s.ProjectThumbnailDir(organization, project)
	matches, err := filepath.Glob(filepath.Join(dir, ProjectThumbnailBaseName+".*"))
	if err != nil {
		return "", "", fmt.Errorf("resolve thumbnail path: %w", err)
	}
	if len(matches) == 0 {
		return "", "", ErrNoThumbnail
	}
	sort.Strings(matches)
	path := matches[0]
	contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))
	if contentType == "" {
		file, openErr := os.Open(path)
		if openErr != nil {
			return "", "", fmt.Errorf("open thumbnail: %w", openErr)
		}
		defer file.Close()
		header := make([]byte, 512)
		n, readErr := file.Read(header)
		if readErr != nil && readErr != io.EOF {
			return "", "", fmt.Errorf("read thumbnail header: %w", readErr)
		}
		contentType = http.DetectContentType(header[:n])
	}
	return path, contentType, nil
}

func (s *FilesystemStore) Save(organization string, project string, data []byte) (string, string, error) {
	if s.dataDir == "" {
		return "", "", ErrDataDirRequired
	}
	extension, err := ValidateThumbnail(data)
	if err != nil {
		return "", "", err
	}

	dir := s.ProjectThumbnailDir(organization, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("create thumbnail directory: %w", err)
	}

	// Delete old thumbnail files if any exist
	_ = s.Delete(organization, project)

	path := s.projectThumbnailPath(organization, project, extension)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return "", "", fmt.Errorf("write thumbnail: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("persist thumbnail: %w", err)
	}

	contentType := "image/png"
	if extension == ".jpg" {
		contentType = "image/jpeg"
	}
	return path, contentType, nil
}

func (s *FilesystemStore) Delete(organization string, project string) error {
	if s.dataDir == "" {
		return ErrDataDirRequired
	}
	dir := s.ProjectThumbnailDir(organization, project)
	matches, err := filepath.Glob(filepath.Join(dir, ProjectThumbnailBaseName+".*"))
	if err != nil {
		return fmt.Errorf("list thumbnails: %w", err)
	}
	if len(matches) == 0 {
		return ErrNoThumbnail
	}
	for _, path := range matches {
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("delete thumbnail %s: %w", path, removeErr)
		}
	}
	return nil
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}
