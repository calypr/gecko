package config

import "testing"

func TestPresentationConfigValidateSanitizesUnsafeHTML(t *testing.T) {
	cfg := &PresentationConfig{
		PresentationConfig: `<div onclick="alert(1)"><script>alert(1)</script><a href="javascript:alert(1)" target="_blank">link</a><p>Hello</p></div>`,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	if cfg.PresentationConfig != `<div><a>link</a><p>Hello</p></div>` {
		t.Fatalf("unexpected sanitized HTML: %q", cfg.PresentationConfig)
	}
}

func TestPresentationConfigValidateRejectsMalformedHTML(t *testing.T) {
	cfg := &PresentationConfig{
		PresentationConfig: `<div><p>broken</div>`,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected malformed HTML validation error")
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
