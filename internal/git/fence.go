package git

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (service *GitService) requestFenceGitHubBroker(ctx context.Context, authorizationHeader string, requestPayload map[string]any, responsePayload any) error {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for GitHub App broker requests",
		}
	}
	validAuthorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return &HTTPStatusError{
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
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return fmt.Errorf("build fence github broker request: %w", err)
	}
	req.Header.Set("Authorization", validAuthorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return &HTTPStatusError{
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
		return &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Code:       code,
			Message:    message,
		}
	}
	if err := json.Unmarshal(body, responsePayload); err != nil {
		return &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github broker response: %s", err),
		}
	}
	return nil
}

func (service *GitService) RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error) {
	var payload fenceGitHubInstallURLResponse
	if err := service.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":        "install_url",
		"owner":         owner,
		"redirect_path": redirectPath,
	}, &payload); err != nil {
		return "", err
	}
	installURL := strings.TrimSpace(payload.InstallURL)
	if installURL == "" {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence github install URL response did not include install_url",
		}
	}
	return installURL, nil
}

func (service *GitService) RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string) (GitRepositoryInstallationStatus, error) {
	var payload fenceGitHubInstallationStatusResponse
	if err := service.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action": "organization_installation",
		"owner":  organization,
	}, &payload); err != nil {
		return GitRepositoryInstallationStatus{}, err
	}
	return GitRepositoryInstallationStatus{
		Installed:           payload.Installed,
		InstallationID:      payload.InstallationID,
		Target:              strings.TrimSpace(payload.Target),
		TargetType:          strings.TrimSpace(payload.TargetType),
		HTMLURL:             strings.TrimSpace(payload.HTMLURL),
		RepositorySelection: strings.TrimSpace(payload.RepositorySelection),
	}, nil
}

func (service *GitService) ListInstallationRepositories(ctx context.Context, authorizationHeader string, installationID int64) ([]GitHubWebhookRepository, error) {
	return service.listInstallationRepositoriesFromFence(ctx, authorizationHeader, installationID)
}

func (service *GitService) RequestInstallationStatus(ctx context.Context, authorizationHeader string, organization string, identity GitRepositoryIdentity) (GitRepositoryInstallationStatus, error) {
	var payload fenceGitHubInstallationStatusResponse
	if err := service.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":       "repository_installation",
		"owner":        identity.Owner,
		"repo":         identity.Repo,
		"organization": organization,
	}, &payload); err != nil {
		return GitRepositoryInstallationStatus{}, err
	}
	return GitRepositoryInstallationStatus{
		Installed:           payload.Installed,
		InstallationID:      payload.InstallationID,
		Target:              strings.TrimSpace(payload.Target),
		TargetType:          strings.TrimSpace(payload.TargetType),
		HTMLURL:             strings.TrimSpace(payload.HTMLURL),
		RepositorySelection: strings.TrimSpace(payload.RepositorySelection),
	}, nil
}

func (service *GitService) RequestInstallationToken(ctx context.Context, authorizationHeader string, organization string, identity GitRepositoryIdentity, access string) (string, error) {
	requestedAccess := strings.TrimSpace(access)
	if requestedAccess == "" {
		requestedAccess = "read"
	}
	var payload fenceGitHubTokenResponse
	if err := service.requestFenceGitHubBroker(ctx, authorizationHeader, map[string]any{
		"action":       "installation_token",
		"owner":        identity.Owner,
		"repo":         identity.Repo,
		"organization": organization,
		"access":       requestedAccess,
	}, &payload); err != nil {
		return "", err
	}
	return ValidateAccessToken(payload.Token)
}
