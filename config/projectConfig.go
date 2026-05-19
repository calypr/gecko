package config

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"
)

type ProjectConfig struct {
	Title        string `json:"title"`
	ContactEmail string `json:"contact_email"`
	SrcRepo      string `json:"src_repo"`
	OrgTitle     string `json:"org_title"`
	Description  string `json:"description"`
	ProjectTitle string `json:"project_title"`
	IconName     string `json:"icon_name"`
}

var ValidateProjectRepository = validateProjectRepository

func (p *ProjectConfig) Validate() error {
	if p == nil {
		return fmt.Errorf("project config is required")
	}

	p.Title = strings.TrimSpace(p.Title)
	p.ContactEmail = strings.TrimSpace(p.ContactEmail)
	p.SrcRepo = strings.TrimSpace(p.SrcRepo)
	p.OrgTitle = strings.TrimSpace(p.OrgTitle)
	p.Description = strings.TrimSpace(p.Description)
	p.ProjectTitle = strings.TrimSpace(p.ProjectTitle)
	p.IconName = strings.TrimSpace(p.IconName)

	requiredFields := []struct {
		name  string
		value string
	}{
		{name: "title", value: p.Title},
		{name: "contact_email", value: p.ContactEmail},
		{name: "src_repo", value: p.SrcRepo},
		{name: "org_title", value: p.OrgTitle},
		{name: "description", value: p.Description},
		{name: "project_title", value: p.ProjectTitle},
		{name: "icon_name", value: p.IconName},
	}
	for _, field := range requiredFields {
		if field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}

	if _, err := mail.ParseAddress(p.ContactEmail); err != nil {
		return fmt.Errorf("contact_email must be a valid email address: %w", err)
	}

	normalized, err := ValidateProjectRepository(context.Background(), p.SrcRepo)
	if err != nil {
		return err
	}
	p.SrcRepo = normalized
	return nil
}

func validateProjectRepository(ctx context.Context, raw string) (string, error) {
	normalized, err := normalizeProjectRepositoryURL(raw)
	if err != nil {
		return "", err
	}
	if err := validateNormalizedProjectRepositoryURL(ctx, normalized); err != nil {
		return "", err
	}
	return normalized, nil
}

func normalizeProjectRepositoryURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("src_repo is required")
	}

	host, path, err := splitProjectRepositoryURL(raw)
	if err != nil {
		return "", err
	}

	host = strings.ToLower(strings.TrimSpace(host))
	path = strings.Trim(path, "/")
	path = strings.TrimSuffix(path, ".git")
	parts := strings.Split(path, "/")

	if host == "ssh.github.com" || host == "altssh.github.com" {
		host = "github.com"
		if len(parts) == 3 && parts[0] == "443" {
			parts = parts[1:]
		}
	}
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("src_repo must point to a GitHub-style owner/repo path")
	}

	return host + "/" + parts[0] + "/" + parts[1], nil
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

func validateNormalizedProjectRepositoryURL(ctx context.Context, normalized string) error {
	validationURL := "https://" + normalized + ".git/info/refs?service=git-upload-pack"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, validationURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create src_repo validation request: %w", err)
	}
	req.Header.Set("User-Agent", "git/2.43.0")

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach src_repo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("src_repo does not exist: %s", normalized)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("src_repo exists but is not publicly accessible: %s", normalized)
	}
	return fmt.Errorf("src_repo validation failed for %s: status %s", normalized, resp.Status)
}
