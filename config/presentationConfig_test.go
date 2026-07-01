package config

import "testing"

func TestPresentationConfigValidateAllowsRawHTML(t *testing.T) {
	cfg := &PresentationConfig{
		PresentationConfig: `<div onclick="alert(1)"><script>alert(1)</script><a href="javascript:alert(1)" target="_blank">link</a><p>Hello</p></div>`,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	if cfg.PresentationConfig != `<div onclick="alert(1)"><script>alert(1)</script><a href="javascript:alert(1)" target="_blank">link</a><p>Hello</p></div>` {
		t.Fatalf("unexpected raw HTML: %q", cfg.PresentationConfig)
	}
}

func TestPresentationConfigValidateAllowsEmptyHTML(t *testing.T) {
	cfg := &PresentationConfig{}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if cfg.PresentationConfig != "" {
		t.Fatalf("expected empty presentationConfig, got %q", cfg.PresentationConfig)
	}
}
