package git

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/google/go-github/v87/github"
	"github.com/jmoiron/sqlx"
)

const githubWebhookSignaturePrefix = "sha256="

type gitHubAppTokenResponse struct {
	Token string `json:"token"`
}

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

func (service *GitService) githubAppJWT() (string, error) {
	appID := strings.TrimSpace(service.config.GitHubAppID)
	privateKeyPEM := strings.TrimSpace(service.config.GitHubAppPrivateKey)
	if appID == "" || privateKeyPEM == "" {
		return "", fmt.Errorf("github app credentials are not configured")
	}
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", fmt.Errorf("invalid github app private key PEM")
	}
	var privateKey *rsa.PrivateKey
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		privateKey = key
	} else {
		parsed, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return "", fmt.Errorf("parse github app private key: %w", parseErr)
		}
		rsaKey, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return "", fmt.Errorf("github app private key must be RSA")
		}
		privateKey = rsaKey
	}
	now := time.Now().UTC()
	headerJSON := `{"alg":"RS256","typ":"JWT"}`
	payloadJSON := fmt.Sprintf(`{"iat":%d,"exp":%d,"iss":"%s"}`, now.Add(-30*time.Second).Unix(), now.Add(9*time.Minute).Unix(), appID)
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(headerJSON)) + "." + base64.RawURLEncoding.EncodeToString([]byte(payloadJSON))
	hashed := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return "", fmt.Errorf("sign github app jwt: %w", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (service *GitService) requestGitHubInstallationAccessToken(ctx context.Context, installationID int64) (string, error) {
	jwtToken, err := service.githubAppJWT()
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/app/installations/%d/access_tokens", strings.TrimRight(service.config.GitHubAPIBase, "/"), installationID),
		http.NoBody,
	)
	if err != nil {
		return "", fmt.Errorf("build github installation token request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := service.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request github installation token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read github installation token response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("github installation token request failed with status %d", resp.StatusCode)
	}
	var payload gitHubAppTokenResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decode github installation token response: %w", err)
	}
	return ValidateAccessToken(payload.Token)
}

func (service *GitService) ListInstallationRepositories(ctx context.Context, installationID int64) ([]GitHubWebhookRepository, error) {
	accessToken, err := service.requestGitHubInstallationAccessToken(ctx, installationID)
	if err != nil {
		return nil, err
	}
	client, err := service.githubClient(accessToken)
	if err != nil {
		return nil, err
	}
	records := []GitHubWebhookRepository{}
	options := &github.ListOptions{PerPage: 100}
	for {
		repositories, response, listErr := client.Apps.ListRepos(ctx, options)
		if listErr != nil {
			return nil, fmt.Errorf("list installation repositories: %w", listErr)
		}
		for _, repository := range repositories.Repositories {
			if repository == nil {
				continue
			}
			records = append(records, GitHubWebhookRepository{
				ID:       repository.GetID(),
				Name:     repository.GetName(),
				FullName: repository.GetFullName(),
				HTMLURL:  repository.GetHTMLURL(),
				CloneURL: repository.GetCloneURL(),
			})
		}
		if response == nil || response.NextPage == 0 {
			break
		}
		options.Page = response.NextPage
	}
	return records, nil
}

func (service *GitService) ReconcilePendingRepositories(ctx context.Context, db *sqlx.DB, installationID int64) ([]geckodb.GitPendingRepository, error) {
	if db == nil {
		return []geckodb.GitPendingRepository{}, nil
	}
	repositories, err := service.ListInstallationRepositories(ctx, installationID)
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
