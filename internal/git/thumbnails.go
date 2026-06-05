package git

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "image/jpeg"
	_ "image/png"
)

const (
	projectThumbnailDirectory = "_project_thumbnails"
	projectThumbnailBaseName  = "thumbnail"
	maxProjectThumbnailBytes  = 1 << 20
	minProjectThumbnailPixels = 100
	maxProjectThumbnailPixels = 3000
)

var thumbnailExtensionByContentType = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
}

func (service *GitService) ProjectThumbnailDir(organization string, project string) string {
	return filepath.Join(
		service.config.DataDir,
		projectThumbnailDirectory,
		sanitizePathPart(strings.TrimSpace(organization)),
		sanitizePathPart(strings.TrimSpace(project)),
	)
}

func (service *GitService) projectThumbnailPath(organization string, project string, extension string) string {
	return filepath.Join(
		service.ProjectThumbnailDir(organization, project),
		projectThumbnailBaseName+extension,
	)
}

func (service *GitService) ProjectThumbnailPath(organization string, project string) (string, string, error) {
	dir := service.ProjectThumbnailDir(organization, project)
	matches, err := filepath.Glob(filepath.Join(dir, projectThumbnailBaseName+".*"))
	if err != nil {
		return "", "", fmt.Errorf("resolve thumbnail path: %w", err)
	}
	if len(matches) == 0 {
		return "", "", os.ErrNotExist
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

func MaxProjectThumbnailBytes() int {
	return maxProjectThumbnailBytes
}

func MinProjectThumbnailPixels() int {
	return minProjectThumbnailPixels
}

func MaxProjectThumbnailPixels() int {
	return maxProjectThumbnailPixels
}

func (service *GitService) SaveProjectThumbnail(organization string, project string, data []byte) (string, string, error) {
	if strings.TrimSpace(service.config.DataDir) == "" {
		return "", "", fmt.Errorf("git data dir is not configured")
	}
	if len(data) == 0 {
		return "", "", fmt.Errorf("thumbnail image is required")
	}
	if len(data) > maxProjectThumbnailBytes {
		return "", "", fmt.Errorf("thumbnail image exceeds %d bytes", maxProjectThumbnailBytes)
	}
	if err := service.EnsureDataDir(); err != nil {
		return "", "", err
	}

	contentType := http.DetectContentType(data)
	extension, ok := thumbnailExtensionByContentType[contentType]
	if !ok {
		return "", "", fmt.Errorf("thumbnail image must be a PNG or JPG file")
	}

	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return "", "", fmt.Errorf("invalid thumbnail image: %w", err)
	}
	if format != "png" && format != "jpeg" {
		return "", "", fmt.Errorf("thumbnail image must be a PNG or JPG file")
	}
	if config.Width < minProjectThumbnailPixels || config.Height < minProjectThumbnailPixels {
		return "", "", fmt.Errorf(
			"thumbnail image must be at least %dx%d pixels",
			minProjectThumbnailPixels,
			minProjectThumbnailPixels,
		)
	}
	if config.Width > maxProjectThumbnailPixels || config.Height > maxProjectThumbnailPixels {
		return "", "", fmt.Errorf(
			"thumbnail image must be at most %dx%d pixels",
			maxProjectThumbnailPixels,
			maxProjectThumbnailPixels,
		)
	}

	dir := service.ProjectThumbnailDir(organization, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("create thumbnail directory: %w", err)
	}
	if err := service.DeleteProjectThumbnail(organization, project); err != nil && !os.IsNotExist(err) {
		return "", "", err
	}

	path := service.projectThumbnailPath(organization, project, extension)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return "", "", fmt.Errorf("write thumbnail: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("persist thumbnail: %w", err)
	}
	return path, contentType, nil
}

func (service *GitService) DeleteProjectThumbnail(organization string, project string) error {
	dir := service.ProjectThumbnailDir(organization, project)
	matches, err := filepath.Glob(filepath.Join(dir, projectThumbnailBaseName+".*"))
	if err != nil {
		return fmt.Errorf("list thumbnails: %w", err)
	}
	if len(matches) == 0 {
		return os.ErrNotExist
	}
	for _, path := range matches {
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("delete thumbnail %s: %w", path, removeErr)
		}
	}
	return nil
}
