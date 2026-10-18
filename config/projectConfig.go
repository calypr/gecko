package config

import (
	"context"
	"fmt"
	"net/mail"
	"net/url"
	"strings"
)

type ProjectConfig struct {
	Title        string `json:"title"`
	ContactEmail string `json:"contact_email"`
	SrcRepo      string `json:"src_repo"`
	OrgTitle     string `json:"org_title"`
	Description  string `json:"description"`
	ProjectTitle string `json:"project_title"`
}

var ValidateProjectRepository = func(_ context.Context, raw string) (string, error) {
	return NormalizeProjectRepositoryURL(raw)
}

func (p *ProjectConfig) Validate() error {
	return p.validate(true)
}

func (p *ProjectConfig) ValidateInitialization() error {
	return p.validate(false)
}

func (p *ProjectConfig) validate(requireRepository bool) error {
	if p == nil {
		return fmt.Errorf("project config is required")
	}

	p.Title = strings.TrimSpace(p.Title)
	p.ContactEmail = strings.TrimSpace(p.ContactEmail)
	p.SrcRepo = strings.TrimSpace(p.SrcRepo)
	p.OrgTitle = strings.TrimSpace(p.OrgTitle)
	p.Description = strings.TrimSpace(p.Description)
	p.ProjectTitle = strings.TrimSpace(p.ProjectTitle)

	requiredFields := []struct {
		name  string
		value string
	}{
		{name: "title", value: p.Title},
		{name: "contact_email", value: p.ContactEmail},
		{name: "org_title", value: p.OrgTitle},
		{name: "description", value: p.Description},
		{name: "project_title", value: p.ProjectTitle},
	}
	if requireRepository {
		requiredFields = append(requiredFields, struct {
			name  string
			value string
		}{name: "src_repo", value: p.SrcRepo})
	}
	for _, field := range requiredFields {
		if field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}

	if _, err := mail.ParseAddress(p.ContactEmail); err != nil {
		return fmt.Errorf("contact_email must be a valid email address: %w", err)
	}

	if strings.TrimSpace(p.SrcRepo) != "" {
		normalized, err := ValidateProjectRepository(context.Background(), p.SrcRepo)
		if err != nil {
			return err
		}
		p.SrcRepo = normalized
	}
	return nil
}

func NormalizeProjectRepositoryURL(raw string) (string, error) {
	return NormalizeProjectRepositoryURLWithHosts(raw, nil)
}

// NormalizeProjectRepositoryURLWithHosts normalizes a project repository URL and
// rejects hosts outside the configured allowlist. An empty allowlist permits
// github.com only.
func NormalizeProjectRepositoryURLWithHosts(raw string, allowedHosts []string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("src_repo is required")
	}

	host, path, err := splitProjectRepositoryURL(raw)
	if err != nil {
		return "", err
	}

	host = normalizeRepositoryHost(host)
	path = strings.Trim(path, "/")
	path = strings.TrimSuffix(path, ".git")
	parts := strings.Split(path, "/")

	if host == "github.com" && len(parts) == 3 && parts[0] == "443" {
		parts = parts[1:]
	}
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("src_repo must point to a GitHub-style owner/repo path")
	}
	if !repositoryHostAllowed(host, allowedHosts) {
		return "", fmt.Errorf("src_repo host %q is not allowed", host)
	}

	return host + "/" + parts[0] + "/" + parts[1], nil
}

// GitHubWebHostsForAPIBase returns the web hosts that correspond to a GitHub
// API base. The public API maps to github.com. Enterprise API bases map to
// their own host.
func GitHubWebHostsForAPIBase(apiBase string) []string {
	hosts := []string{"github.com"}
	trimmed := strings.TrimSpace(apiBase)
	if trimmed == "" {
		return hosts
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return hosts
	}
	host := normalizeRepositoryHost(parsed.Hostname())
	if host == "" || host == "github.com" || host == "api.github.com" {
		return hosts
	}
	return append(hosts, host)
}

func repositoryHostAllowed(host string, allowedHosts []string) bool {
	if len(allowedHosts) == 0 {
		return host == "github.com"
	}
	for _, allowed := range allowedHosts {
		if normalizeRepositoryHost(allowed) == host {
			return true
		}
	}
	return false
}

func normalizeRepositoryHost(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if strings.Contains(value, "://") {
		if parsed, err := url.Parse(value); err == nil {
			value = parsed.Hostname()
		}
	}
	value = strings.TrimSuffix(value, ".")
	if value == "ssh.github.com" || value == "altssh.github.com" {
		return "github.com"
	}
	return value
}

func splitProjectRepositoryURL(raw string) (string, string, error) {
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", "", fmt.Errorf("invalid src_repo URL: %w", err)
		}
		if parsed.Host == "" {
			return "", "", fmt.Errorf("src_repo host is required")
		}
		return parsed.Hostname(), parsed.EscapedPath(), nil
	}

	if strings.Contains(raw, "@") && strings.Contains(raw, ":") {
		atIdx := strings.LastIndex(raw, "@")
		colonIdx := strings.Index(raw[atIdx+1:], ":")
		if colonIdx >= 0 {
			host := raw[atIdx+1 : atIdx+1+colonIdx]
			path := raw[atIdx+1+colonIdx+1:]
			return host, path, nil
		}
	}

	parts := strings.Split(strings.Trim(raw, "/"), "/")
	if len(parts) >= 3 {
		return parts[0], strings.Join(parts[1:], "/"), nil
	}

	return "", "", fmt.Errorf("invalid src_repo URL: %s", raw)
}
