package github

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	"github.com/calypr/gecko/internal/httpclient"
	google_github "github.com/google/go-github/v87/github"
)

type Config struct {
	APIBase                     string
	DisableTransportDiagnostics bool
}

type Client struct {
	client *http.Client
	config Config
}

type GitHubRepositoryMetadata struct {
	DefaultBranch string `json:"default_branch"`
	HTMLURL       string `json:"html_url"`
}

func NewClient(client *http.Client, config Config) *Client {
	if client == nil {
		client = httpclient.NewServiceClient(30 * time.Second)
	}
	if config.APIBase == "" {
		config.APIBase = "https://api.github.com"
	}
	return &Client{client: client, config: config}
}

func (c *Client) FetchRepositoryMetadata(ctx context.Context, accessToken string, identity domain.GitRepositoryIdentity) (*domain.GitHubRepositoryMetadata, error) {
	githubClient, err := c.githubClient(accessToken)
	if err != nil {
		return nil, err
	}
	repo, err := c.fetchRepositoryMetadata(ctx, githubClient, identity, accessToken)
	if err != nil {
		return nil, fmt.Errorf("github repository metadata lookup failed for %s/%s: %w", identity.Owner, identity.Repo, err)
	}
	defaultBranch := repo.GetDefaultBranch()
	htmlURL := repo.GetHTMLURL()
	if htmlURL == "" {
		htmlURL = identity.URL
	}
	return &domain.GitHubRepositoryMetadata{DefaultBranch: defaultBranch, HTMLURL: htmlURL}, nil
}

func (c *Client) fetchRepositoryMetadata(ctx context.Context, client *google_github.Client, identity domain.GitRepositoryIdentity, accessToken string) (*google_github.Repository, error) {
	started := time.Now()
	requestURL := c.repositoryMetadataRequestURL(identity)
	authConfigured := strings.TrimSpace(accessToken) != ""
	tokenFingerprint := githubAccessTokenFingerprint(accessToken)
	tokenLength := githubAccessTokenLength(accessToken)
	log.Printf("INFO: github_repository_metadata_request_start owner=%s repo=%s request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d", identity.Owner, identity.Repo, requestURL, authConfigured, tokenFingerprint, tokenLength)
	repo, response, err := client.Repositories.Get(ctx, identity.Owner, identity.Repo)
	statusCode := 0
	rateLimitRemaining := -1
	rateLimitReset := ""
	if response != nil {
		rateLimitRemaining = response.Rate.Remaining
		if !response.Rate.Reset.Time.IsZero() {
			rateLimitReset = response.Rate.Reset.Time.UTC().Format(time.RFC3339)
		}
		if response.Response != nil {
			statusCode = response.Response.StatusCode
		}
	}
	if err == nil {
		log.Printf("INFO: github_repository_metadata_request_done owner=%s repo=%s request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d status=%d rate_limit_remaining=%d rate_limit_reset=%q duration_ms=%d", identity.Owner, identity.Repo, requestURL, authConfigured, tokenFingerprint, tokenLength, statusCode, rateLimitRemaining, rateLimitReset, time.Since(started).Milliseconds())
		return repo, nil
	}
	log.Printf("INFO: github_repository_metadata_request_done owner=%s repo=%s request_url=%q auth_configured=%t auth_scheme=Bearer token_fingerprint=%s token_length=%d status=%d rate_limit_remaining=%d rate_limit_reset=%q duration_ms=%d error_type=%T error=%q", identity.Owner, identity.Repo, requestURL, authConfigured, tokenFingerprint, tokenLength, statusCode, rateLimitRemaining, rateLimitReset, time.Since(started).Milliseconds(), err, err.Error())
	if response == nil {
		c.logTransportDiagnostic(ctx, accessToken, err)
	}
	return nil, err
}

func (c *Client) repositoryMetadataRequestURL(identity domain.GitRepositoryIdentity) string {
	apiBase := strings.TrimRight(c.config.APIBase, "/")
	if apiBase == "" {
		apiBase = "https://api.github.com"
	}
	return fmt.Sprintf("%s/repos/%s/%s", apiBase, identity.Owner, identity.Repo)
}

func githubAccessTokenFingerprint(accessToken string) string {
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(accessToken))
	return hex.EncodeToString(sum[:])[:16]
}

func githubAccessTokenLength(accessToken string) int {
	return len(strings.TrimSpace(accessToken))
}

func (c *Client) logTransportDiagnostic(parent context.Context, accessToken string, originalErr error) {
	if c.config.DisableTransportDiagnostics {
		return
	}
	apiBase := strings.TrimRight(c.config.APIBase, "/")
	if apiBase == "" {
		apiBase = "https://api.github.com"
	}
	parsed, err := url.Parse(apiBase)
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_done api_base=%q token_fingerprint=%s token_length=%d original_error_type=%T original_error=%q diagnostic_error=%q", apiBase, githubAccessTokenFingerprint(accessToken), githubAccessTokenLength(accessToken), originalErr, originalErr.Error(), err.Error())
		return
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	}
	address := net.JoinHostPort(host, port)
	log.Printf("INFO: github_transport_diagnostic_start api_base=%q host=%s port=%s scheme=%s token_fingerprint=%s token_length=%d original_error_type=%T original_error=%q", apiBase, host, port, parsed.Scheme, githubAccessTokenFingerprint(accessToken), githubAccessTokenLength(accessToken), originalErr, originalErr.Error())

	diagCtx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	c.logDNSDiagnostic(diagCtx, host)
	c.logTCPDiagnostic(diagCtx, address)
	if parsed.Scheme == "https" {
		c.logTLSDiagnostic(host, address)
	}
	c.logHTTPDiagnostic(diagCtx, apiBase, accessToken)
}

func (c *Client) logDNSDiagnostic(ctx context.Context, host string) {
	started := time.Now()
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_stage stage=dns host=%s duration_ms=%d error_type=%T error=%q", host, time.Since(started).Milliseconds(), err, err.Error())
		return
	}
	log.Printf("INFO: github_transport_diagnostic_stage stage=dns host=%s duration_ms=%d ips=%q", host, time.Since(started).Milliseconds(), strings.Join(ips, ","))
}

func (c *Client) logTCPDiagnostic(ctx context.Context, address string) {
	started := time.Now()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_stage stage=tcp address=%s duration_ms=%d error_type=%T error=%q", address, time.Since(started).Milliseconds(), err, err.Error())
		return
	}
	remote := conn.RemoteAddr().String()
	local := conn.LocalAddr().String()
	_ = conn.Close()
	log.Printf("INFO: github_transport_diagnostic_stage stage=tcp address=%s local_addr=%s remote_addr=%s duration_ms=%d", address, local, remote, time.Since(started).Milliseconds())
}

func (c *Client) logTLSDiagnostic(host string, address string) {
	started := time.Now()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", address, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_stage stage=tls host=%s address=%s duration_ms=%d error_type=%T error=%q", host, address, time.Since(started).Milliseconds(), err, err.Error())
		return
	}
	state := conn.ConnectionState()
	_ = conn.Close()
	log.Printf("INFO: github_transport_diagnostic_stage stage=tls host=%s address=%s version=%x cipher_suite=%x server_name=%s duration_ms=%d", host, address, state.Version, state.CipherSuite, state.ServerName, time.Since(started).Milliseconds())
}

func (c *Client) logHTTPDiagnostic(ctx context.Context, apiBase string, accessToken string) {
	started := time.Now()
	requestURL := strings.TrimRight(apiBase, "/") + "/rate_limit"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_stage stage=http request_url=%q token_fingerprint=%s token_length=%d duration_ms=%d error_type=%T error=%q", requestURL, githubAccessTokenFingerprint(accessToken), githubAccessTokenLength(accessToken), time.Since(started).Milliseconds(), err, err.Error())
		return
	}
	if strings.TrimSpace(accessToken) != "" {
		request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(accessToken))
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	response, err := c.client.Do(request)
	if err != nil {
		log.Printf("INFO: github_transport_diagnostic_stage stage=http request_url=%q auth_configured=%t token_fingerprint=%s token_length=%d duration_ms=%d error_type=%T error=%q", requestURL, strings.TrimSpace(accessToken) != "", githubAccessTokenFingerprint(accessToken), githubAccessTokenLength(accessToken), time.Since(started).Milliseconds(), err, err.Error())
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	log.Printf("INFO: github_transport_diagnostic_stage stage=http request_url=%q auth_configured=%t token_fingerprint=%s token_length=%d status=%d rate_limit_remaining=%q rate_limit_reset=%q duration_ms=%d", requestURL, strings.TrimSpace(accessToken) != "", githubAccessTokenFingerprint(accessToken), githubAccessTokenLength(accessToken), response.StatusCode, response.Header.Get("X-RateLimit-Remaining"), response.Header.Get("X-RateLimit-Reset"), time.Since(started).Milliseconds())
}

func (c *Client) githubClient(accessToken string) (*google_github.Client, error) {
	options := []google_github.ClientOptionsFunc{
		google_github.WithAuthToken(accessToken),
		google_github.WithHTTPClient(c.client),
	}
	if strings.TrimRight(c.config.APIBase, "/") != "https://api.github.com" {
		apiBase := strings.TrimRight(c.config.APIBase, "/") + "/"
		options = append(options, google_github.WithEnterpriseURLs(apiBase, apiBase))
	}
	client, err := google_github.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create github client: %w", err)
	}
	return client, nil
}
