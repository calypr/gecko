package git

import (
	"path/filepath"
	"testing"
)

func TestShouldRefreshMirrorForRead(t *testing.T) {
	existingMirrorPath := t.TempDir()
	missingMirrorPath := filepath.Join(t.TempDir(), "missing.git")

	tests := []struct {
		name                 string
		mirrorPath           string
		requireCurrentMirror bool
		want                 bool
	}{
		{
			name:       "existing mirror can serve regular reads",
			mirrorPath: existingMirrorPath,
			want:       false,
		},
		{
			name:                 "storage audit refreshes existing mirror",
			mirrorPath:           existingMirrorPath,
			requireCurrentMirror: true,
			want:                 true,
		},
		{
			name:       "missing mirror refreshes regular reads",
			mirrorPath: missingMirrorPath,
			want:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldRefreshMirrorForRead(test.mirrorPath, test.requireCurrentMirror); got != test.want {
				t.Fatalf("expected refresh=%t, got %t", test.want, got)
			}
		})
	}
}
