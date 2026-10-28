package github

import "net/http"

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
