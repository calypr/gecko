package git

import (
	"encoding/json"
	"net/http"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/jmoiron/sqlx"
)

const (
	GitSyncNeverSynced = "never_synced"
	GitSyncReady       = "ready"
	GitSyncUpdating    = "updating"
	GitSyncError       = "error"

	GitInstallationNotConnected = "not_connected"
	GitInstallationConnected    = "connected"
)

type GitServiceConfig struct {
	DataDir             string
	GitHubAPIBase       string
	GitHubAppInstallURL string
	FenceBaseURL        string
	HTTPClient          *http.Client
}

type GitService struct {
	config GitServiceConfig
	client *http.Client
}

type GitRepositoryIdentity struct {
	Host  string `json:"host"`
	Owner string `json:"owner"`
	Repo  string `json:"repo"`
	URL   string `json:"url"`
}

type GitProjectStatusResponse struct {
	ProjectID              string                  `json:"project_id"`
	Organization           string                  `json:"organization"`
	Project                string                  `json:"project"`
	ResourcePath           string                  `json:"resource_path"`
	Config                 appconfig.ProjectConfig `json:"config"`
	Repository             GitRepositoryIdentity   `json:"repository"`
	InstallationState      string                  `json:"installation_state"`
	InstallationID         *int64                  `json:"installation_id,omitempty"`
	InstallationTarget     string                  `json:"installation_target,omitempty"`
	InstallationTargetType string                  `json:"installation_target_type,omitempty"`
	SyncState              string                  `json:"sync_state"`
	DefaultBranch          string                  `json:"default_branch,omitempty"`
	LastRefreshedAt        *time.Time              `json:"last_refreshed_at,omitempty"`
	LastError              string                  `json:"last_error,omitempty"`
	MirrorReady            bool                    `json:"mirror_ready"`
}

type GitProjectConnectResponse struct {
	Registered  bool   `json:"registered"`
	Message     string `json:"message,omitempty"`
	RedirectURL string `json:"redirect_url,omitempty"`
}

type GitOrganizationConnectResponse struct {
	RedirectURL string `json:"redirect_url"`
}

type GitProjectRefreshResponse struct {
	Success        bool   `json:"success"`
	ProjectID      string `json:"project_id"`
	SyncState      string `json:"sync_state"`
	DefaultBranch  string `json:"default_branch,omitempty"`
	LastFetchedRef string `json:"last_fetched_ref,omitempty"`
	Error          string `json:"error,omitempty"`
}

type GitRef struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Hash    string `json:"hash"`
	Default bool   `json:"default"`
}

type GitProjectRefsResponse struct {
	ProjectID     string   `json:"project_id"`
	DefaultBranch string   `json:"default_branch,omitempty"`
	Refs          []GitRef `json:"refs"`
}

type GitTreeEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"`
	Hash string `json:"hash"`
	Size int64  `json:"size,omitempty"`
}

type GitProjectTreeResponse struct {
	ProjectID string         `json:"project_id"`
	Ref       string         `json:"ref"`
	Path      string         `json:"path"`
	Entries   []GitTreeEntry `json:"entries"`
}

type GitProjectFileResponse struct {
	ProjectID string `json:"project_id"`
	Ref       string `json:"ref"`
	Path      string `json:"path"`
	Name      string `json:"name"`
	Hash      string `json:"hash"`
	Size      int64  `json:"size"`
	Encoding  string `json:"encoding"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

type githubRepositoryResponse struct {
	DefaultBranch string `json:"default_branch"`
	HTMLURL       string `json:"html_url"`
}

type fenceGitHubTokenResponse struct {
	Token      string                `json:"token"`
	ExpiresAt  string                `json:"expires_at"`
	Repository GitRepositoryIdentity `json:"repository"`
}

type HTTPStatusError struct {
	StatusCode int
	Code       string
	Message    string
}

func (err *HTTPStatusError) Error() string {
	if err == nil {
		return ""
	}
	if err.Message != "" {
		return err.Message
	}
	return http.StatusText(err.StatusCode)
}

func decodeFenceErrorResponse(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	if message, ok := payload["message"].(string); ok {
		return message
	}
	return ""
}

func NewGitService(config GitServiceConfig) *GitService {
	if config.DataDir == "" {
		config.DataDir = "/tmp/gecko-git"
	}
	if config.GitHubAPIBase == "" {
		config.GitHubAPIBase = "https://api.github.com"
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &GitService{config: config, client: client}
}

func (service *GitService) Init(db *sqlx.DB) error {
	if err := service.EnsureDataDir(); err != nil {
		return err
	}
	if db == nil {
		return nil
	}
	return geckodb.EnsureGitProjectStateTable(db)
}
