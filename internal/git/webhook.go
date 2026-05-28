package git

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
