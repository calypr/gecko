package fence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	servermw "github.com/calypr/gecko/internal/server/middleware"
)

type Config struct {
	BaseURL string
}

type Client struct {
	client *http.Client
	config Config
}

func NewClient(client *http.Client, config Config) *Client {
	if client == nil {
		client = http.DefaultClient
	}
	return &Client{client: client, config: config}
}

type fenceGitHubInstallURLResponse struct {
	InstallURL string `json:"install_url"`
}

type fenceGitHubInstallationStatusResponse struct {
	Installed           bool   `json:"installed"`
	InstallationID      *int64 `json:"installation_id"`
	Target              string `json:"target"`
	TargetType          string `json:"target_type"`
	HTMLURL             string `json:"html_url"`
	RepositorySelection string `json:"repository_selection"`
}

type fenceGitHubInstallationRepositoriesResponse struct {
	Repositories []domain.GitHubInstallationRepository `json:"repositories"`
}

type fenceGitHubTokenResponse struct {
	Token string `json:"token"`
}

func (c *Client) requestFenceGitHubBroker(ctx context.Context, authorizationHeader string, requestPayload map[string]any, responsePayload any) error {
	if strings.TrimSpace(c.config.BaseURL) == "" {
		return &domain.HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for GitHub App broker requests",
		}
	}
	validAuthorizationHeader, err := servermw.ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return &domain.HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	requestBody, err := json.Marshal(requestPayload)
	if err != nil {
		return fmt.Errorf("marshal fence github broker request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(c.config.BaseURL, "/")+"/credentials/github",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return fmt.Errorf("build fence github broker request: %w", err)
	}
	req.Header.Set("Authorization", validAuthorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return &domain.HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("Fence github broker request failed: %s", err),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read fence github broker response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github broker request failed with status %d", resp.StatusCode)
		}
		code := "integration_error"
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			code = "missing_authorization"
		case http.StatusForbidden:
			code = "forbidden"
		case http.StatusNotFound:
			code = "not_found"
		}
		return &domain.HTTPStatusError{
			StatusCode: resp.StatusCode,
			Code:       code,
			Message:    message,
		}
	}
	if err := json.Unmarshal(body, responsePayload); err != nil {
		return &domain.HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github broker response: %s", err),
		}
	}
	return nil
}

func (c *Client) RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error) {
	var payload fenceGitHubInstallURLResponse
	if err := c.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":        "install_url",
		"organization":  owner,
		"owner":         owner,
		"redirect_path": redirectPath,
	}, &payload); err != nil {
		return "", err
	}
	installURL := strings.TrimSpace(payload.InstallURL)
	if installURL == "" {
		return "", &domain.HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence github install URL response did not include install_url",
		}
	}
	return installURL, nil
}

func (c *Client) RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (domain.GitRepositoryInstallationStatus, error) {
	var payload fenceGitHubInstallationStatusResponse
	if err := c.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":       "organization_installation",
		"organization": organization,
		"owner":        owner,
	}, &payload); err != nil {
		return domain.GitRepositoryInstallationStatus{}, err
	}
	return domain.GitRepositoryInstallationStatus{
		Installed:           payload.Installed,
		InstallationID:      payload.InstallationID,
		Target:              strings.TrimSpace(payload.Target),
		TargetType:          strings.TrimSpace(payload.TargetType),
		HTMLURL:             strings.TrimSpace(payload.HTMLURL),
		RepositorySelection: strings.TrimSpace(payload.RepositorySelection),
	}, nil
}

func (c *Client) ListInstallationRepositories(ctx context.Context, authorizationHeader string, installationID int64) ([]domain.GitHubInstallationRepository, error) {
	var payload fenceGitHubInstallationRepositoriesResponse
	if err := c.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":          "installation_repositories",
		"installation_id": installationID,
	}, &payload); err != nil {
		return nil, err
	}
	return payload.Repositories, nil
}

func (c *Client) RequestInstallationStatus(ctx context.Context, authorizationHeader string, organization string, identity domain.GitRepositoryIdentity) (domain.GitRepositoryInstallationStatus, error) {
	var payload fenceGitHubInstallationStatusResponse
	if err := c.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":       "repository_installation",
		"owner":        identity.Owner,
		"repo":         identity.Repo,
		"organization": organization,
	}, &payload); err != nil {
		return domain.GitRepositoryInstallationStatus{}, err
	}
	return domain.GitRepositoryInstallationStatus{
		Installed:           payload.Installed,
		InstallationID:      payload.InstallationID,
		Target:              strings.TrimSpace(payload.Target),
		TargetType:          strings.TrimSpace(payload.TargetType),
		HTMLURL:             strings.TrimSpace(payload.HTMLURL),
		RepositorySelection: strings.TrimSpace(payload.RepositorySelection),
	}, nil
}

func (c *Client) RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity domain.GitRepositoryIdentity, access string) (string, error) {
	requestedAccess := strings.TrimSpace(access)
	if requestedAccess == "" {
		requestedAccess = "read"
	}
	var payload fenceGitHubTokenResponse
	if err := c.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":       "installation_token",
		"owner":        identity.Owner,
		"repo":         identity.Repo,
		"organization": organization,
		"project":      strings.TrimSpace(project),
		"access":       requestedAccess,
	}, &payload); err != nil {
		return "", err
	}
	return servermw.ValidateAccessToken(payload.Token)
}

func decodeFenceErrorResponse(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	if message, ok := payload["message"].(string); ok {
		return message
	}
	return ""
}
