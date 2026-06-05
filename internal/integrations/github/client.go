package github

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	google_github "github.com/google/go-github/v87/github"
)

type Config struct {
	APIBase string
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
		client = http.DefaultClient
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
	repo, _, err := githubClient.Repositories.Get(ctx, identity.Owner, identity.Repo)
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
