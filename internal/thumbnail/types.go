package thumbnail

import "errors"

const (
	ProjectThumbnailDirectory = "_project_thumbnails"
	ProjectThumbnailBaseName  = "thumbnail"
	MaxProjectThumbnailBytes  = 1 << 20 // 1 MB
	MinProjectThumbnailPixels = 100
	MaxProjectThumbnailPixels = 3000
)

var (
	ErrInvalidFormat    = errors.New("thumbnail image must be a PNG or JPG file")
	ErrImageTooSmall    = errors.New("thumbnail image is too small")
	ErrImageTooLarge    = errors.New("thumbnail image is too large")
	ErrLimitExceeded    = errors.New("thumbnail image size limit exceeded")
	ErrNoThumbnail      = errors.New("project has no thumbnail")
	ErrDataDirRequired  = errors.New("thumbnail storage directory is required")
	ErrThumbnailIsEmpty = errors.New("thumbnail image data is empty")
)
