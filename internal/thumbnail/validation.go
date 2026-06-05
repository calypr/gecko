package thumbnail

import (
	"bytes"
	"fmt"
	"image"
	"net/http"

	_ "image/jpeg"
	_ "image/png"
)

var thumbnailExtensionByContentType = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
}

// ValidateThumbnail verifies the image format, file size, and pixel dimensions.
func ValidateThumbnail(data []byte) (string, error) {
	if len(data) == 0 {
		return "", ErrThumbnailIsEmpty
	}
	if len(data) > MaxProjectThumbnailBytes {
		return "", fmt.Errorf("%w: exceeds %d bytes", ErrLimitExceeded, MaxProjectThumbnailBytes)
	}

	contentType := http.DetectContentType(data)
	extension, ok := thumbnailExtensionByContentType[contentType]
	if !ok {
		return "", ErrInvalidFormat
	}

	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("invalid image data: %w", err)
	}
	if format != "png" && format != "jpeg" {
		return "", ErrInvalidFormat
	}

	if config.Width < MinProjectThumbnailPixels || config.Height < MinProjectThumbnailPixels {
		return "", fmt.Errorf("%w: must be at least %dx%d pixels, got %dx%d", ErrImageTooSmall, MinProjectThumbnailPixels, MinProjectThumbnailPixels, config.Width, config.Height)
	}
	if config.Width > MaxProjectThumbnailPixels || config.Height > MaxProjectThumbnailPixels {
		return "", fmt.Errorf("%w: must be at most %dx%d pixels, got %dx%d", ErrImageTooLarge, MaxProjectThumbnailPixels, MaxProjectThumbnailPixels, config.Width, config.Height)
	}

	return extension, nil
}
