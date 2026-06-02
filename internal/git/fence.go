package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const brokerCacheTTL = 5 * time.Minute

type brokerCacheEntry struct {
	expiresAt time.Time
	body      []byte
}

var brokerCache = struct {
	mu   sync.RWMutex
	data map[string]brokerCacheEntry
}{
	data: map[string]brokerCacheEntry{},
}

func brokerCacheKey(authorizationHeader string, requestBody []byte) string {
	sum := sha256.Sum256(append([]byte(strings.TrimSpace(authorizationHeader)), requestBody...))
	return hex.EncodeToString(sum[:])
}

func brokerAction(requestPayload map[string]any) string {
	action, _ := requestPayload["action"].(string)
	return strings.TrimSpace(action)
}

func shouldCacheBrokerAction(action string) bool {
	switch action {
	case "organization_installation", "repository_installation", "installation_repositories":
		return true
	default:
		return false
	}
}

func getCachedBrokerBody(cacheKey string) ([]byte, bool) {
	now := time.Now()
	brokerCache.mu.RLock()
	entry, ok := brokerCache.data[cacheKey]
	brokerCache.mu.RUnlock()
	if !ok || now.After(entry.expiresAt) {
		return nil, false
	}
	body := make([]byte, len(entry.body))
	copy(body, entry.body)
	return body, true
}

func setCachedBrokerBody(cacheKey string, body []byte) {
	copyBody := make([]byte, len(body))
	copy(copyBody, body)
	brokerCache.mu.Lock()
	brokerCache.data[cacheKey] = brokerCacheEntry{
		expiresAt: time.Now().Add(brokerCacheTTL),
		body:      copyBody,
	}
	brokerCache.mu.Unlock()
}

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
	action := brokerAction(requestPayload)
	if shouldCacheBrokerAction(action) {
		cacheKey := brokerCacheKey(validAuthorizationHeader, requestBody)
		if cachedBody, ok := getCachedBrokerBody(cacheKey); ok {
			if err := json.Unmarshal(cachedBody, responsePayload); err != nil {
				return &HTTPStatusError{
					StatusCode: http.StatusBadGateway,
					Code:       "integration_error",
					Message:    fmt.Sprintf("invalid cached Fence github broker response: %s", err),
				}
			}
			return nil
		}
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
	if shouldCacheBrokerAction(action) {
		cacheKey := brokerCacheKey(validAuthorizationHeader, requestBody)
		setCachedBrokerBody(cacheKey, body)
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
