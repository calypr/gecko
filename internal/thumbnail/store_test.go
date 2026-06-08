package thumbnail

import (
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func generateImageBytes(t *testing.T, format string, width, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	// Fill with color
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: 255, G: 0, B: 0, A: 255})
		}
	}

	tempFile, err := os.CreateTemp("", "test-img-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if format == "png" {
		if err := png.Encode(tempFile, img); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := jpeg.Encode(tempFile, img, nil); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tempFile.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(tempFile)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValidateThumbnail(t *testing.T) {
	t.Run("valid png", func(t *testing.T) {
		data := generateImageBytes(t, "png", 150, 150)
		ext, err := ValidateThumbnail(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ext != ".png" {
			t.Errorf("expected extension .png, got %s", ext)
		}
	})

	t.Run("valid jpeg", func(t *testing.T) {
		data := generateImageBytes(t, "jpeg", 150, 150)
		ext, err := ValidateThumbnail(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ext != ".jpg" {
			t.Errorf("expected extension .jpg, got %s", ext)
		}
	})

	t.Run("too small", func(t *testing.T) {
		data := generateImageBytes(t, "png", 50, 150)
		_, err := ValidateThumbnail(data)
		if err == nil || err != ErrImageTooSmall && !filepath.HasPrefix(err.Error(), ErrImageTooSmall.Error()) {
			t.Fatalf("expected ErrImageTooSmall, got %v", err)
		}
	})

	t.Run("too large", func(t *testing.T) {
		data := generateImageBytes(t, "png", 3500, 150)
		_, err := ValidateThumbnail(data)
		if err == nil || err != ErrImageTooLarge && !filepath.HasPrefix(err.Error(), ErrImageTooLarge.Error()) {
			t.Fatalf("expected ErrImageTooLarge, got %v", err)
		}
	})

	t.Run("invalid format text", func(t *testing.T) {
		_, err := ValidateThumbnail([]byte("not-an-image"))
		if err == nil || err != ErrInvalidFormat && !filepath.HasPrefix(err.Error(), ErrInvalidFormat.Error()) {
			t.Fatalf("expected ErrInvalidFormat, got %v", err)
		}
	})
}

func TestFilesystemStore(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "thumbnail-store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFilesystemStore(tempDir)
	org := "TestOrg"
	proj := "TestProj"

	t.Run("get non-existent thumbnail", func(t *testing.T) {
		_, _, err := store.GetPath(org, proj)
		if err != ErrNoThumbnail {
			t.Fatalf("expected ErrNoThumbnail, got %v", err)
		}
	})

	t.Run("save and get valid thumbnail", func(t *testing.T) {
		data := generateImageBytes(t, "png", 120, 120)
		path, contentType, err := store.Save(org, proj, data)
		if err != nil {
			t.Fatalf("failed to save: %v", err)
		}
		if contentType != "image/png" {
			t.Errorf("expected image/png, got %s", contentType)
		}

		retPath, retType, err := store.GetPath(org, proj)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if retPath != path {
			t.Errorf("expected path %s, got %s", path, retPath)
		}
		if retType != contentType {
			t.Errorf("expected type %s, got %s", contentType, retType)
		}
	})

	t.Run("delete thumbnail", func(t *testing.T) {
		err := store.Delete(org, proj)
		if err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		_, _, err = store.GetPath(org, proj)
		if err != ErrNoThumbnail {
			t.Fatalf("expected ErrNoThumbnail after delete, got %v", err)
		}
	})
}
