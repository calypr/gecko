package git

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/jmoiron/sqlx"
)

const githubWebhookSignaturePrefix = "sha256="

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (service *GitService) VerifyWebhookSignature(body []byte, signature string) error {
	secret := strings.TrimSpace(service.config.GitHubWebhookSecret)
	if secret == "" {
		return fmt.Errorf("github webhook secret is not configured")
	}
	signature = strings.TrimSpace(signature)
	if !strings.HasPrefix(signature, githubWebhookSignaturePrefix) {
		return fmt.Errorf("github webhook signature is invalid")
	}
	expectedMAC := hmac.New(sha256.New, []byte(secret))
	expectedMAC.Write(body)
	expectedSignature := hex.EncodeToString(expectedMAC.Sum(nil))
	actualSignature := strings.TrimPrefix(signature, githubWebhookSignaturePrefix)
	if !hmac.Equal([]byte(strings.ToLower(actualSignature)), []byte(expectedSignature)) {
		return fmt.Errorf("github webhook signature verification failed")
	}
	return nil
}

func pendingRepositoryID(installationID int64, repoID int64) string {
	return fmt.Sprintf("%d:%d", installationID, repoID)
}

func pendingRepositoryFromWebhook(installationID int64, repository GitHubWebhookRepository) (*geckodb.GitPendingRepository, error) {
	identity, err := ParseRepositoryIdentity(firstNonEmptyString(repository.CloneURL, repository.HTMLURL, repository.FullName))
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return &geckodb.GitPendingRepository{
		ID:             pendingRepositoryID(installationID, repository.ID),
		InstallationID: installationID,
		Organization:   identity.Owner,
		RepoID:         repository.ID,
		RepoName:       identity.Repo,
		RepoFullName:   firstNonEmptyString(repository.FullName, identity.Owner+"/"+identity.Repo),
		RepoHTMLURL:    nullableString(strings.TrimSpace(repository.HTMLURL)),
		RepoCloneURL:   nullableString(strings.TrimSpace(repository.CloneURL)),
		RepoHost:       identity.Host,
		RepoOwner:      identity.Owner,
		RepoPath:       identity.Repo,
		AddedAt:        now,
		UpdatedAt:      now,
	}, nil
}

func nullableString(value string) sql.NullString {
	if strings.TrimSpace(value) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: strings.TrimSpace(value), Valid: true}
}

func (service *GitService) HandleGitHubWebhook(_ context.Context, db *sqlx.DB, event string, body []byte) error {
	switch strings.TrimSpace(event) {
	case "installation_repositories":
		var payload GitHubWebhookInstallationRepositoriesPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			return fmt.Errorf("decode installation_repositories webhook: %w", err)
		}
		installationID := payload.Installation.ID
		if installationID == 0 {
			return fmt.Errorf("installation_repositories webhook missing installation id")
		}
		for _, repository := range payload.RepositoriesAdded {
			pending, err := pendingRepositoryFromWebhook(installationID, repository)
			if err != nil {
				return fmt.Errorf("normalize added repository %s: %w", repository.FullName, err)
			}
			if err := geckodb.UpsertGitPendingRepository(db, *pending); err != nil {
				return err
			}
		}
		for _, repository := range payload.RepositoriesRemoved {
			if err := geckodb.RemoveGitPendingRepository(db, installationID, repository.ID); err != nil {
				return err
			}
		}
		return nil
	case "installation":
		return nil
	default:
		return nil
	}
}

func ListPendingRepositories(db *sqlx.DB, installationID int64) ([]geckodb.GitPendingRepository, error) {
	if db == nil {
		return []geckodb.GitPendingRepository{}, nil
	}
	return geckodb.ListGitPendingRepositoriesByInstallation(db, installationID)
}

func (service *GitService) ListInstallationRepositoriesFromFence(ctx context.Context, authorizationHeader string, installationID int64) ([]GitHubWebhookRepository, error) {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return nil, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for GitHub installation repository reconcile",
		}
	}
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return nil, &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	requestBody, err := json.Marshal(map[string]int64{"installation_id": installationID})
	if err != nil {
		return nil, fmt.Errorf("marshal fence github installation repositories request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github/installation-repositories",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return nil, fmt.Errorf("build fence github installation repositories request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return nil, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("Fence github installation repositories request failed: %s", err),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read fence github installation repositories response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github installation repositories request failed with status %d", resp.StatusCode)
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
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode, Code: code, Message: message}
	}

	var payload fenceGitHubInstallationRepositoriesResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github installation repositories response: %s", err),
		}
	}
	return payload.Repositories, nil
}

func (service *GitService) ReconcilePendingRepositories(ctx context.Context, db *sqlx.DB, authorizationHeader string, installationID int64) ([]geckodb.GitPendingRepository, error) {
	if db == nil {
		return []geckodb.GitPendingRepository{}, nil
	}
	repositories, err := service.ListInstallationRepositoriesFromFence(ctx, authorizationHeader, installationID)
	if err != nil {
		return nil, err
	}
	existingProjects, err := geckodb.ConfigListByType(db, string(appconfig.TypeProjects))
	if err != nil {
		return nil, fmt.Errorf("list project configs: %w", err)
	}
	currentPendingRepositories, err := geckodb.ListGitPendingRepositoriesByInstallation(db, installationID)
	if err != nil {
		return nil, fmt.Errorf("list current pending repositories: %w", err)
	}

	currentRepoIDs := make(map[int64]struct{}, len(repositories))
	existingRepoKeys := make(map[string]struct{}, len(existingProjects))
	for _, projectID := range existingProjects {
		var cfg appconfig.ProjectConfig
		if err := geckodb.ConfigGETGeneric(db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
			continue
		}
		identity, identityErr := ParseRepositoryIdentity(cfg.SrcRepo)
		if identityErr != nil {
			continue
		}
		existingRepoKeys[strings.ToLower(identity.Host+"/"+identity.Owner+"/"+identity.Repo)] = struct{}{}
	}

	for _, repository := range repositories {
		currentRepoIDs[repository.ID] = struct{}{}
		pending, err := pendingRepositoryFromWebhook(installationID, repository)
		if err != nil {
			continue
		}
		repoKey := strings.ToLower(pending.RepoHost + "/" + pending.RepoOwner + "/" + pending.RepoPath)
		if _, exists := existingRepoKeys[repoKey]; exists {
			_ = geckodb.ResolveGitPendingRepositoriesByRepo(db, installationID, pending.RepoHost, pending.RepoOwner, pending.RepoPath)
			continue
		}
		if err := geckodb.UpsertGitPendingRepository(db, *pending); err != nil {
			return nil, err
		}
	}

	for _, pendingRepository := range currentPendingRepositories {
		if _, exists := currentRepoIDs[pendingRepository.RepoID]; exists {
			continue
		}
		if err := geckodb.RemoveGitPendingRepository(db, installationID, pendingRepository.RepoID); err != nil {
			return nil, err
		}
	}
	return geckodb.ListGitPendingRepositoriesByInstallation(db, installationID)
}

func PendingRepositoryResponse(record geckodb.GitPendingRepository) GitPendingRepository {
	response := GitPendingRepository{
		ID:             record.ID,
		InstallationID: record.InstallationID,
		Organization:   record.Organization,
		RepoID:         record.RepoID,
		RepoName:       record.RepoName,
		RepoFullName:   record.RepoFullName,
		RepoHost:       record.RepoHost,
		RepoOwner:      record.RepoOwner,
		RepoPath:       record.RepoPath,
		AddedAt:        record.AddedAt.UTC().Format(time.RFC3339),
	}
	if record.RepoHTMLURL.Valid {
		response.RepoHTMLURL = record.RepoHTMLURL.String
	}
	if record.RepoCloneURL.Valid {
		response.RepoCloneURL = record.RepoCloneURL.String
	}
	return response
}
