package repo

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/domain"
	gitapi "github.com/calypr/gecko/internal/integrations/github"
	"github.com/google/go-github/v87/github"
)

func (service *Service) GetGitHubFileMetadata(ctx context.Context, authorizationHeader string, organization string, project string, identity Identity, ref string, path string) (*github.RepositoryContent, []byte, error) {
	authorizationHeader, err := access.ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return nil, nil, &domain.HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, "read")
	if err != nil {
		return nil, nil, err
	}
	client, err := service.githubClient(accessToken)
	if err != nil {
		return nil, nil, err
	}
	opts := &github.RepositoryContentGetOptions{}
	if strings.TrimSpace(ref) != "" {
		opts.Ref = strings.TrimSpace(ref)
	}
	metadata, response, err := getGitHubFileContents(ctx, client, identity, path, opts, strings.TrimRight(service.config.GitHubAPIBase, "/"), strings.TrimSpace(accessToken) != "", gitapi.TokenFingerprint(accessToken), gitapi.TokenLength(accessToken))
	if err != nil {
		statusCode := http.StatusBadGateway
		if response != nil && response.StatusCode > 0 {
			statusCode = response.StatusCode
		}
		if statusCode == http.StatusBadGateway && strings.Contains(strings.ToLower(err.Error()), "not found") {
			statusCode = http.StatusNotFound
		}
		return nil, nil, &domain.HTTPStatusError{
			StatusCode: statusCode,
			Code:       "integration_error",
			Message:    fmt.Sprintf("GitHub file lookup failed: %s", err),
		}
	}
	if response != nil && response.Response != nil && response.StatusCode >= http.StatusBadRequest {
		return metadata, nil, &domain.HTTPStatusError{
			StatusCode: response.StatusCode,
			Code:       "integration_error",
			Message:    fmt.Sprintf("GitHub file lookup failed with status %d", response.StatusCode),
		}
	}
	if metadata == nil {
		return nil, nil, &domain.HTTPStatusError{
			StatusCode: http.StatusNotFound,
			Code:       "not_found",
			Message:    fmt.Sprintf("GitHub file %s was not found", path),
		}
	}
	contentString, err := metadata.GetContent()
	if err != nil || contentString == "" {
		return metadata, nil, nil
	}
	return metadata, []byte(contentString), nil
}

func getGitHubFileContents(ctx context.Context, client *github.Client, identity Identity, path string, opts *github.RepositoryContentGetOptions, apiBase string, authConfigured bool, tokenFingerprint string, tokenLength int) (*github.RepositoryContent, *github.Response, error) {
	ref := ""
	if opts != nil {
		ref = strings.TrimSpace(opts.Ref)
	}
	started := time.Now()
	requestURL := githubContentsRequestURL(apiBase, identity, path, ref)
	log.Printf("INFO: github_file_contents_request_start owner=%s repo=%s path=%q ref=%q request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d", identity.Owner, identity.Repo, path, ref, requestURL, authConfigured, tokenFingerprint, tokenLength)
	metadata, _, response, err := client.Repositories.GetContents(ctx, identity.Owner, identity.Repo, path, opts)
	statusCode := 0
	rateLimitRemaining := -1
	rateLimitReset := ""
	if response != nil {
		rateLimitRemaining = response.Rate.Remaining
		if !response.Rate.Reset.IsZero() {
			rateLimitReset = response.Rate.Reset.UTC().Format(time.RFC3339)
		}
		if response.Response != nil {
			statusCode = response.StatusCode
		}
	}
	if err == nil {
		log.Printf("INFO: github_file_contents_request_done owner=%s repo=%s path=%q ref=%q request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d status=%d rate_limit_remaining=%d rate_limit_reset=%q duration_ms=%d", identity.Owner, identity.Repo, path, ref, requestURL, authConfigured, tokenFingerprint, tokenLength, statusCode, rateLimitRemaining, rateLimitReset, time.Since(started).Milliseconds())
		return metadata, response, nil
	}
	log.Printf("INFO: github_file_contents_request_done owner=%s repo=%s path=%q ref=%q request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d status=%d rate_limit_remaining=%d rate_limit_reset=%q duration_ms=%d error_type=%T error=%q", identity.Owner, identity.Repo, path, ref, requestURL, authConfigured, tokenFingerprint, tokenLength, statusCode, rateLimitRemaining, rateLimitReset, time.Since(started).Milliseconds(), err, err.Error())
	return nil, response, err
}

func githubContentsRequestURL(apiBase string, identity Identity, path string, ref string) string {
	if strings.TrimSpace(apiBase) == "" {
		apiBase = "https://api.github.com"
	}
	base := strings.TrimRight(apiBase, "/")
	requestURL := fmt.Sprintf("%s/repos/%s/%s/contents/%s", base, identity.Owner, identity.Repo, strings.TrimLeft(path, "/"))
	if ref != "" {
		requestURL += "?ref=" + ref
	}
	return requestURL
}
