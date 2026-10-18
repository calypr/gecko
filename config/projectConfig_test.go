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

func TestNormalizeProjectRepositoryURLRejectsUnlistedHost(t *testing.T) {
	_, err := NormalizeProjectRepositoryURLWithHosts("https://attacker.example/owner/repo.git", []string{"github.com"})
	if err == nil || !strings.Contains(err.Error(), "is not allowed") {
		t.Fatalf("expected host rejection, got %v", err)
	}
}

func TestNormalizeProjectRepositoryURLAllowsEnterpriseHost(t *testing.T) {
	got, err := NormalizeProjectRepositoryURLWithHosts("https://github.example.org/owner/repo.git", []string{"github.example.org"})
	if err != nil {
		t.Fatalf("NormalizeProjectRepositoryURLWithHosts() error = %v", err)
	}
	if got != "github.example.org/owner/repo" {
		t.Fatalf("normalized repo = %q", got)
	}
}

func TestNormalizeProjectRepositoryURLMapsSSHHostToGitHub(t *testing.T) {
	got, err := NormalizeProjectRepositoryURLWithHosts("git@ssh.github.com:443/owner/repo.git", nil)
	if err != nil {
		t.Fatalf("NormalizeProjectRepositoryURLWithHosts() error = %v", err)
	}
	if got != "github.com/owner/repo" {
		t.Fatalf("normalized repo = %q", got)
	}
}

func TestGitHubWebHostsForAPIBase(t *testing.T) {
	if got := GitHubWebHostsForAPIBase("https://api.github.com"); len(got) != 1 || got[0] != "github.com" {
		t.Fatalf("public hosts = %v", got)
	}
	if got := GitHubWebHostsForAPIBase("https://github.example.org/api/v3"); len(got) != 2 || got[0] != "github.com" || got[1] != "github.example.org" {
		t.Fatalf("enterprise hosts = %v", got)
	}
}
