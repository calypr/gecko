package config

import (
	"context"
	"strings"
	"testing"
)

func TestProjectConfigValidateNormalizesAndValidates(t *testing.T) {
	old := ValidateProjectRepository
	ValidateProjectRepository = func(ctx context.Context, raw string) (string, error) {
		if raw != "https://github.com/calypr/gecko.git" {
			t.Fatalf("unexpected raw repo: %q", raw)
		}
		return "github.com/calypr/gecko", nil
	}
	defer func() { ValidateProjectRepository = old }()

	cfg := &ProjectConfig{
		Title:        " Title ",
		ContactEmail: " person@example.org ",
		SrcRepo:      "https://github.com/calypr/gecko.git",
		OrgTitle:     " Org ",
		Description:  " Desc ",
		ProjectTitle: " Project ",
		IconName:     " flask ",
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if cfg.SrcRepo != "github.com/calypr/gecko" {
		t.Fatalf("expected normalized repo, got %q", cfg.SrcRepo)
	}
	if cfg.ContactEmail != "person@example.org" || cfg.Title != "Title" {
		t.Fatalf("expected trimmed fields, got %+v", cfg)
	}
}

func TestProjectConfigValidateRejectsInvalidEmail(t *testing.T) {
	cfg := &ProjectConfig{
		Title:        "Title",
		ContactEmail: "not-an-email",
		SrcRepo:      "https://github.com/calypr/gecko",
		OrgTitle:     "Org",
		Description:  "Desc",
		ProjectTitle: "Project",
		IconName:     "flask",
	}

	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "contact_email") {
		t.Fatalf("expected contact_email validation error, got %v", err)
	}
}

func TestProjectConfigValidateRejectsMissingFields(t *testing.T) {
	cfg := &ProjectConfig{}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "title is required") {
		t.Fatalf("expected missing title error, got %v", err)
	}
}
