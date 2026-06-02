package git

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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
	GitHubWebhookSecret string
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
	ProjectID                       string                  `json:"project_id"`
	Organization                    string                  `json:"organization"`
	Project                         string                  `json:"project"`
	ResourcePath                    string                  `json:"resource_path"`
	Accessible                      bool                    `json:"accessible"`
	RequestAccess                   bool                    `json:"request_access"`
	RequestAccessResourcePath       string                  `json:"request_access_resource_path,omitempty"`
	Config                          appconfig.ProjectConfig `json:"config"`
	Repository                      GitRepositoryIdentity   `json:"repository"`
	InstallationState               string                  `json:"installation_state"`
	InstallationID                  *int64                  `json:"installation_id,omitempty"`
	InstallationTarget              string                  `json:"installation_target,omitempty"`
	InstallationTargetType          string                  `json:"installation_target_type,omitempty"`
	OrganizationAppInstalled        bool                    `json:"organization_app_installed"`
	OrganizationHTMLURL             string                  `json:"organization_html_url,omitempty"`
	OrganizationRepositorySelection string                  `json:"organization_repository_selection,omitempty"`
	SyncState                       string                  `json:"sync_state"`
	DefaultBranch                   string                  `json:"default_branch,omitempty"`
	LastRefreshedAt                 *time.Time              `json:"last_refreshed_at,omitempty"`
	LastError                       string                  `json:"last_error,omitempty"`
	MirrorReady                     bool                    `json:"mirror_ready"`
}

type GitOrganizationConnectResponse struct {
	RedirectURL    string `json:"redirect_url"`
	SetupSessionID string `json:"setup_session_id,omitempty"`
}

type GitRepositoryInstallationStatus struct {
	Installed           bool   `json:"installed"`
	InstallationID      *int64 `json:"installation_id,omitempty"`
	Target              string `json:"target,omitempty"`
	TargetType          string `json:"target_type,omitempty"`
	HTMLURL             string `json:"html_url,omitempty"`
	RepositorySelection string `json:"repository_selection,omitempty"`
}

type GitOrganizationProjectStatus struct {
	ProjectID                 string                          `json:"project_id"`
	Project                   string                          `json:"project"`
	ResourcePath              string                          `json:"resource_path"`
	Repository                GitRepositoryIdentity           `json:"repository"`
	Configured                bool                            `json:"configured"`
	Readiness                 *CalyprProjectReadiness         `json:"readiness,omitempty"`
	Accessible                bool                            `json:"accessible"`
	RequestAccess             bool                            `json:"request_access"`
	RequestAccessResourcePath string                          `json:"request_access_resource_path,omitempty"`
	Installation              GitRepositoryInstallationStatus `json:"installation"`
}

type GitPendingRepository struct {
	ID              string `json:"id"`
	InstallationID  int64  `json:"installation_id"`
	SetupSessionID  string `json:"setup_session_id,omitempty"`
	CreatedByUserID string `json:"created_by_user_id,omitempty"`
	Organization    string `json:"organization"`
	RepoID          int64  `json:"repo_id"`
	RepoName        string `json:"repo_name"`
	RepoFullName    string `json:"repo_full_name"`
	RepoHTMLURL     string `json:"repo_html_url,omitempty"`
	RepoCloneURL    string `json:"repo_clone_url,omitempty"`
	RepoHost        string `json:"repo_host"`
	RepoOwner       string `json:"repo_owner"`
	RepoPath        string `json:"repo_path"`
	AddedAt         string `json:"added_at"`
}

type GitPendingRepositoriesResponse struct {
	InstallationID int64                  `json:"installation_id,omitempty"`
	SetupSessionID string                 `json:"setup_session_id,omitempty"`
	Pending        []GitPendingRepository `json:"pending"`
}

type GitPendingRepositoriesReconcileRequest struct {
	InstallationID int64  `json:"installation_id"`
	SetupSessionID string `json:"setup_session_id,omitempty"`
}

type CalyprProjectStorageIntent struct {
	Bucket       string `json:"bucket"`
	Provider     string `json:"provider"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	Organization string `json:"organization"`
	ProjectID    string `json:"project_id"`
	Path         string `json:"path,omitempty"`
}

type CalyprProjectSetupRequest struct {
	Config        appconfig.ProjectConfig     `json:"config"`
	Storage       *CalyprProjectStorageIntent `json:"storage,omitempty"`
	PendingRepoID string                      `json:"pending_repo_id,omitempty"`
}

type CalyprReadinessCheck struct {
	Pass    bool   `json:"pass"`
	Reason  string `json:"reason,omitempty"`
	Details string `json:"details,omitempty"`
}

type CalyprProjectReadiness struct {
	Git    CalyprReadinessCheck `json:"git"`
	Syfon  CalyprReadinessCheck `json:"syfon"`
	Config CalyprReadinessCheck `json:"config"`
}

type CalyprProjectSetupResponse struct {
	ProjectID    string                 `json:"project_id"`
	ResourcePath string                 `json:"resource_path"`
	Configured   bool                   `json:"configured"`
	Readiness    CalyprProjectReadiness `json:"readiness"`
}

type GitHubWebhookRepository struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	HTMLURL  string `json:"html_url"`
	CloneURL string `json:"clone_url"`
}

type fenceGitHubInstallationRepositoriesResponse struct {
	InstallationID int64                     `json:"installation_id"`
	Repositories   []GitHubWebhookRepository `json:"repositories"`
}

type GitHubWebhookInstallation struct {
	ID int64 `json:"id"`
}

type GitHubWebhookInstallationRepositoriesPayload struct {
	Action              string                    `json:"action"`
	Installation        GitHubWebhookInstallation `json:"installation"`
	RepositoriesAdded   []GitHubWebhookRepository `json:"repositories_added"`
	RepositoriesRemoved []GitHubWebhookRepository `json:"repositories_removed"`
}

type GitOrganizationStatusResponse struct {
	Organization        string                         `json:"organization"`
	Connected           bool                           `json:"connected"`
	AppInstalled        bool                           `json:"app_installed"`
	InstallationID      *int64                         `json:"installation_id,omitempty"`
	HTMLURL             string                         `json:"html_url,omitempty"`
	RepositorySelection string                         `json:"repository_selection,omitempty"`
	ConfigurationState  string                         `json:"configuration_state"`
	ConnectedProjects   int                            `json:"connected_projects"`
	ConfiguredProjects  int                            `json:"configured_projects"`
	TotalProjects       int                            `json:"total_projects"`
	Projects            []GitOrganizationProjectStatus `json:"projects"`
}

type GitOrganizationsStatusResponse struct {
	Connected              bool                            `json:"connected"`
	AppInstalled           bool                            `json:"app_installed"`
	ConnectedOrganizations int                             `json:"connected_organizations"`
	InstalledOrganizations int                             `json:"installed_organizations"`
	TotalOrganizations     int                             `json:"total_organizations"`
	ConnectedProjects      int                             `json:"connected_projects"`
	ConfiguredProjects     int                             `json:"configured_projects"`
	TotalProjects          int                             `json:"total_projects"`
	ConfigurationState     string                          `json:"configuration_state"`
	Organizations          []GitOrganizationStatusResponse `json:"organizations"`
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
	Name           string             `json:"name"`
	Path           string             `json:"path"`
	Type           string             `json:"type"`
	Hash           string             `json:"hash"`
	Size           int64              `json:"size,omitempty"`
	LastModifiedAt *time.Time         `json:"last_modified_at,omitempty"`
	LFSPointer     *GitLFSPointerInfo `json:"lfs_pointer,omitempty"`
}

type GitProjectTreeResponse struct {
	ProjectID string         `json:"project_id"`
	Ref       string         `json:"ref"`
	Path      string         `json:"path"`
	Entries   []GitTreeEntry `json:"entries"`
}

type GitProjectFileResponse struct {
	ProjectID   string             `json:"project_id"`
	Ref         string             `json:"ref"`
	Path        string             `json:"path"`
	Name        string             `json:"name"`
	Hash        string             `json:"hash"`
	Size        int64              `json:"size"`
	HTMLURL     string             `json:"html_url,omitempty"`
	DownloadURL string             `json:"download_url,omitempty"`
	LFSPointer  *GitLFSPointerInfo `json:"lfs_pointer,omitempty"`
}

type GitLFSPointerInfo struct {
	Version string `json:"version"`
	OID     string `json:"oid"`
	Size    int64  `json:"size"`
}

type GitUploadSessionFileManifest struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type GitUploadSessionCreateRequest struct {
	BaseBranch   string                         `json:"base_branch"`
	TargetSubdir string                         `json:"target_subdirectory"`
	Files        []GitUploadSessionFileManifest `json:"files"`
}

type GitUploadSessionFileAttachment struct {
	FileName    string `json:"file_name"`
	TargetPath  string `json:"target_path"`
	Checksum    string `json:"checksum"`
	DRSObjectID string `json:"drs_object_id"`
	Size        int64  `json:"size"`
}

type GitUploadSessionAttachFilesRequest struct {
	Files []GitUploadSessionFileAttachment `json:"files"`
}

type GitUploadSessionFinalizeRequest struct {
	PRTitle string `json:"pr_title"`
	PRBody  string `json:"pr_body"`
}

type GitUploadSessionFileStatus struct {
	FileName    string `json:"file_name"`
	TargetPath  string `json:"target_path"`
	Size        int64  `json:"size"`
	Checksum    string `json:"checksum,omitempty"`
	DRSObjectID string `json:"drs_object_id,omitempty"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
	Collision   bool   `json:"collision"`
}

type GitUploadSessionResponse struct {
	SessionID      string                       `json:"session_id"`
	ProjectID      string                       `json:"project_id"`
	BaseBranch     string                       `json:"base_branch"`
	TargetSubdir   string                       `json:"target_subdirectory,omitempty"`
	BranchName     string                       `json:"branch_name"`
	PRTitle        string                       `json:"pr_title"`
	PRBody         string                       `json:"pr_body"`
	Status         string                       `json:"status"`
	PullRequestURL string                       `json:"pull_request_url,omitempty"`
	CommitSHA      string                       `json:"commit_sha,omitempty"`
	Files          []GitUploadSessionFileStatus `json:"files"`
	HasConflicts   bool                         `json:"has_conflicts"`
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

type fenceGitHubInstallURLResponse struct {
	InstallURL string `json:"install_url"`
	Owner      string `json:"owner"`
}

type fenceGitHubInstallationStatusResponse struct {
	Installed           bool   `json:"installed"`
	InstallationID      *int64 `json:"installation_id"`
	Target              string `json:"target"`
	TargetType          string `json:"target_type"`
	HTMLURL             string `json:"html_url"`
	RepositorySelection string `json:"repository_selection"`
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
