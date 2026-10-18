package repo

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/httpclient"
	"github.com/calypr/gecko/internal/integrations/fence"
	gitapi "github.com/calypr/gecko/internal/integrations/github"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/jmoiron/sqlx"
)

type ServiceConfig struct {
	DataDir              string
	GitHubAPIBase        string
	FenceBaseURL         string
	HTTPClient           *http.Client
	FenceClient          *fence.Client
	GitHubClient         *gitapi.Client
	RepoAnalyticsIndexer func(context.Context, string, *gogit.Repository, string, plumbing.Hash) error
}

type Service struct {
	config    ServiceConfig
	client    *http.Client
	fenceAPI  *fence.Client
	githubAPI *gitapi.Client
}

func NewService(config ServiceConfig) *Service {
	if config.GitHubAPIBase == "" {
		config.GitHubAPIBase = "https://api.github.com"
	}
	client := config.HTTPClient
	if client == nil {
		client = httpclient.NewServiceClient(20 * time.Second)
	}
	return &Service{
		config:    config,
		client:    client,
		fenceAPI:  config.FenceClient,
		githubAPI: config.GitHubClient,
	}
}

func (service *Service) Init(db *sqlx.DB) error {
	if strings.TrimSpace(service.config.DataDir) == "" {
		return fmt.Errorf("git data dir is required; set GIT_DATA_DIR or --git-data-dir")
	}
	if err := service.EnsureDataDir(); err != nil {
		return err
	}
	if db == nil {
		return nil
	}
	return geckodb.EnsureGitProjectStateTable(db)
}

const SyncStateReady = "ready"

type ProjectRefreshResponse struct {
	Success        bool   `json:"success"`
	ProjectID      string `json:"project_id"`
	SyncState      string `json:"sync_state"`
	DefaultBranch  string `json:"default_branch,omitempty"`
	LastFetchedRef string `json:"last_fetched_ref,omitempty"`
	Error          string `json:"error,omitempty"`
}
