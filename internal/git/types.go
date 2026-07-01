package git

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git/domain"
	"github.com/calypr/gecko/internal/integrations/fence"
	gitapi "github.com/calypr/gecko/internal/integrations/github"
	"github.com/jmoiron/sqlx"
)

const (
	GitSyncNeverSynced = "never_synced"
	GitSyncReady       = "ready"
	GitSyncUpdating    = "updating"
	GitSyncError       = "error"

	GitInstallationNotConnected = "not_connected"
	GitInstallationConnected    = "connected"

	GitWorkflowStageAwaitingGitHubConnect = "awaiting_github_connect"
	GitWorkflowStageGitHubConnected       = "github_connected"
)

type GitServiceConfig struct {
	DataDir       string
	GitHubAPIBase string
	FenceBaseURL  string
	HTTPClient    *http.Client
	FenceClient   *fence.Client
	GitHubClient  *gitapi.Client
}

type GitService struct {
	config    GitServiceConfig
	client    *http.Client
	fenceAPI  *fence.Client
	githubAPI *gitapi.Client
}

// GitRepositoryIdentity is an alias for domain.GitRepositoryIdentity.
type GitRepositoryIdentity = domain.GitRepositoryIdentity

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
	WorkflowStage                   string                  `json:"workflow_stage,omitempty"`
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
	Mode           string                         `json:"mode"`
	RedirectURL    string                         `json:"redirect_url,omitempty"`
	InstallationID *int64                         `json:"installation_id,omitempty"`
	Repositories   []GitHubInstallationRepository `json:"repositories,omitempty"`
}

// GitRepositoryInstallationStatus is an alias for domain.GitRepositoryInstallationStatus.
type GitRepositoryInstallationStatus = domain.GitRepositoryInstallationStatus

type ProjectIntegrationCheck struct {
	Pass    bool   `json:"pass"`
	Reason  string `json:"reason,omitempty"`
	Details string `json:"details,omitempty"`
}

type ProjectIntegrationStatus struct {
	GitHub  ProjectIntegrationCheck `json:"github"`
	Storage ProjectIntegrationCheck `json:"storage"`
}

type GitOrganizationProjectStatus struct {
	ProjectID                 string                          `json:"project_id"`
	Project                   string                          `json:"project"`
	ResourcePath              string                          `json:"resource_path"`
	Repository                GitRepositoryIdentity           `json:"repository"`
	WorkflowStage             string                          `json:"workflow_stage,omitempty"`
	Configured                bool                            `json:"configured"`
	Readiness                 *CalyprProjectReadiness         `json:"readiness,omitempty"`
	Integrations              ProjectIntegrationStatus        `json:"integrations"`
	Accessible                bool                            `json:"accessible"`
	CanManageSettings         bool                            `json:"can_manage_settings"`
	RequestAccess             bool                            `json:"request_access"`
	RequestAccessResourcePath string                          `json:"request_access_resource_path,omitempty"`
	Installation              GitRepositoryInstallationStatus `json:"installation"`
}

type CalyprProjectStorageIntent struct {
	Bucket              string `json:"bucket"`
	Provider            string `json:"provider"`
	Endpoint            string `json:"endpoint"`
	Region              string `json:"region"`
	AccessKey           string `json:"access_key"`
	SecretKey           string `json:"secret_key"`
	Organization        string `json:"organization"`
	ProjectID           string `json:"project_id"`
	Path                string `json:"path,omitempty"`
	PathPrefix          string `json:"path_prefix,omitempty"`
	OrganizationSubPath string `json:"organization_sub_path,omitempty"`
	ProjectSubPath      string `json:"project_sub_path,omitempty"`
}

type CalyprProjectSetupRequest struct {
	Config  appconfig.ProjectConfig     `json:"config"`
	Storage *CalyprProjectStorageIntent `json:"storage,omitempty"`
}

type CalyprProjectInitializeResponse struct {
	Success      bool   `json:"success"`
	ProjectID    string `json:"project_id"`
	ResourcePath string `json:"resource_path"`
}

type CalyprProjectStorageRequest struct {
	Storage *CalyprProjectStorageIntent `json:"storage"`
}

type CalyprProjectStorageResponse struct {
	Success      bool                    `json:"success"`
	ProjectID    string                  `json:"project_id"`
	ResourcePath string                  `json:"resource_path"`
	Storage      ProjectIntegrationCheck `json:"storage"`
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

// GitHubInstallationRepository is an alias for domain.GitHubInstallationRepository.
type GitHubInstallationRepository = domain.GitHubInstallationRepository

type GitOrganizationStatusResponse struct {
	Organization        string                         `json:"organization"`
	Connected           bool                           `json:"connected"`
	AppInstalled        bool                           `json:"app_installed"`
	CanAccessSettings   bool                           `json:"can_access_settings"`
	CanCreateProjects   bool                           `json:"can_create_projects"`
	CanManagePeople     bool                           `json:"can_manage_people"`
	CanDeleteOrg        bool                           `json:"can_delete_org"`
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

type GitTreeResponseOptions struct {
	IncludeSize         bool
	IncludeLastModified bool
	IncludeLFSPointer   bool
	Limit               int
}

type GitProjectTreeResponse struct {
	ProjectID  string         `json:"project_id"`
	Ref        string         `json:"ref"`
	Path       string         `json:"path"`
	EntryCount int            `json:"entry_count"`
	Truncated  bool           `json:"truncated,omitempty"`
	Entries    []GitTreeEntry `json:"entries"`
}

type GitManifestResponseOptions struct {
	Limit     int
	Cursor    string
	FilesOnly bool
}

type GitProjectManifestResponse struct {
	ProjectID  string         `json:"project_id"`
	Ref        string         `json:"ref"`
	Path       string         `json:"path"`
	EntryCount int            `json:"entry_count"`
	HasMore    bool           `json:"has_more"`
	NextCursor string         `json:"next_cursor,omitempty"`
	Entries    []GitTreeEntry `json:"entries"`
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

type GitRepoAnalyticsChild struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Type       string `json:"type"`
	FileCount  int    `json:"file_count"`
	TotalBytes int64  `json:"total_bytes"`
}

type GitRepoAnalyticsDirectory struct {
	Path             string                  `json:"path"`
	DirectChildCount int                     `json:"direct_child_count"`
	FileCount        int                     `json:"file_count"`
	TotalBytes       int64                   `json:"total_bytes"`
	Children         []GitRepoAnalyticsChild `json:"children"`
}

type GitRepoAnalyticsIndexSidecar struct {
	SchemaVersion int                         `json:"schema_version"`
	CommitHash    string                      `json:"commit_hash"`
	RefName       string                      `json:"ref_name"`
	GeneratedAt   time.Time                   `json:"generated_at"`
	Files         []RepoInventoryFile         `json:"files"`
	Directories   []GitRepoAnalyticsDirectory `json:"directories"`
}

type GitStorageSummaryResponse struct {
	Path               string `json:"path"`
	FileCount          int    `json:"file_count"`
	RecordCount        int    `json:"record_count"`
	DirectChildCount   int    `json:"direct_child_count"`
	TotalBytes         int64  `json:"total_bytes"`
	DownloadCount      int64  `json:"download_count"`
	LastDownloadTime   string `json:"last_download_time,omitempty"`
	LatestUpdateTime   string `json:"latest_update_time,omitempty"`
	DuplicatePathCount int    `json:"duplicate_path_count"`
}

type GitStorageChildResponseItem struct {
	Name             string `json:"name"`
	Path             string `json:"path"`
	Type             string `json:"type"`
	FileCount        int    `json:"file_count"`
	RecordCount      int    `json:"record_count"`
	TotalBytes       int64  `json:"total_bytes"`
	DownloadCount    int64  `json:"download_count"`
	LastDownloadTime string `json:"last_download_time,omitempty"`
	LatestUpdateTime string `json:"latest_update_time,omitempty"`
}

type GitStorageChildrenResponse struct {
	Items []GitStorageChildResponseItem `json:"items"`
}

type GitProjectDiffAuditRequest struct {
	GitSubpath string `json:"git_subpath,omitempty"`
	Ref        string `json:"ref,omitempty"`
}

type GitAuditEvidence struct {
	Checksum         string   `json:"checksum,omitempty"`
	SourcePaths      []string `json:"source_paths,omitempty"`
	ObjectIDs        []string `json:"object_ids,omitempty"`
	AccessURLs       []string `json:"access_urls,omitempty"`
	BucketObjectURLs []string `json:"bucket_object_urls,omitempty"`
	Buckets          []string `json:"buckets,omitempty"`
	Keys             []string `json:"keys,omitempty"`
	ProbeStatuses    []string `json:"probe_statuses,omitempty"`
	ValidationStates []string `json:"validation_states,omitempty"`
	ErrorKinds       []string `json:"error_kinds,omitempty"`
	Errors           []string `json:"errors,omitempty"`
	BucketEvaluation string   `json:"bucket_evaluation,omitempty"`
}

type GitProjectDiffFinding struct {
	Kind              string            `json:"kind"`
	NormalizedPath    string            `json:"normalized_path"`
	Checksum          string            `json:"checksum,omitempty"`
	SourcePaths       []string          `json:"source_paths,omitempty"`
	ObjectIDs         []string          `json:"object_ids"`
	RecordCount       int               `json:"record_count"`
	SizeBytes         int64             `json:"size_bytes,omitempty"`
	DownloadCount     int64             `json:"download_count,omitempty"`
	LastDownload      string            `json:"last_download_time,omitempty"`
	RecommendedAction string            `json:"recommended_action"`
	Evidence          *GitAuditEvidence `json:"evidence,omitempty"`
}

type GitProjectDiffSummary struct {
	CountsByKind         map[string]int `json:"counts_by_kind"`
	TotalFindings        int            `json:"total_findings"`
	IndexedPathCount     int            `json:"indexed_path_count"`
	ExpectedPathCount    int            `json:"expected_path_count"`
	MatchedPathCount     int            `json:"matched_path_count"`
	IncludesRepoManifest bool           `json:"includes_repo_manifest"`
	ScannedRecordCount   int            `json:"scanned_record_count"`
}

type GitProjectDiffAuditResponse struct {
	Findings   []GitProjectDiffFinding `json:"findings"`
	Summary    GitProjectDiffSummary   `json:"summary"`
	PathPrefix string                  `json:"path_prefix"`
}

type GitStorageCleanupAuditRequest struct {
	GitSubpath        string   `json:"git_subpath,omitempty"`
	Ref               string   `json:"ref,omitempty"`
	CheckStorage      bool     `json:"check_storage,omitempty"`
	SelectedRepoPaths []string `json:"selected_repo_paths,omitempty"`
}

type GitStorageChainAuditRequest struct {
	GitSubpath string `json:"git_subpath,omitempty"`
	Ref        string `json:"ref,omitempty"`
}

type GitStorageChainFinding struct {
	Kind              string            `json:"kind"`
	NormalizedPath    string            `json:"normalized_path"`
	Checksum          string            `json:"checksum,omitempty"`
	SourcePaths       []string          `json:"source_paths,omitempty"`
	ObjectIDs         []string          `json:"object_ids"`
	AccessURLs        []string          `json:"access_urls,omitempty"`
	BucketObjectURL   string            `json:"bucket_object_url,omitempty"`
	ResolvedBucket    string            `json:"resolved_bucket,omitempty"`
	ResolvedKey       string            `json:"resolved_key,omitempty"`
	ProbeStatus       string            `json:"probe_status,omitempty"`
	ErrorKind         string            `json:"error_kind,omitempty"`
	Error             string            `json:"error,omitempty"`
	RecordCount       int               `json:"record_count"`
	SizeBytes         int64             `json:"size_bytes,omitempty"`
	RecommendedAction string            `json:"recommended_action"`
	Evidence          *GitAuditEvidence `json:"evidence,omitempty"`
}

type GitStorageChainAuditSummary struct {
	CountsByKind             map[string]int `json:"counts_by_kind"`
	TotalFindings            int            `json:"total_findings"`
	BucketObjectCount        int            `json:"bucket_object_count"`
	SyfonRecordCount         int            `json:"syfon_record_count"`
	GitTrackedFileCount      int            `json:"git_tracked_file_count"`
	BucketInventoryAvailable bool           `json:"bucket_inventory_available"`
	BucketInventoryError     string         `json:"bucket_inventory_error,omitempty"`
}

type GitStorageChainIssueGroup struct {
	Kind         string `json:"kind"`
	FindingCount int    `json:"finding_count"`
	PathCount    int    `json:"path_count"`
	RecordCount  int    `json:"record_count"`
	ObjectCount  int    `json:"object_count"`
	TotalBytes   int64  `json:"total_bytes,omitempty"`
}

type GitStorageChainAuditResponse struct {
	Findings   []GitStorageChainFinding    `json:"findings"`
	Groups     []GitStorageChainIssueGroup `json:"groups,omitempty"`
	Summary    GitStorageChainAuditSummary `json:"summary"`
	PathPrefix string                      `json:"path_prefix"`
}

type GitStorageCleanupAccessProbe struct {
	URL                  string   `json:"url"`
	Provider             string   `json:"provider,omitempty"`
	Bucket               string   `json:"bucket,omitempty"`
	Key                  string   `json:"key,omitempty"`
	Path                 string   `json:"path,omitempty"`
	Exists               *bool    `json:"exists,omitempty"`
	Status               string   `json:"status,omitempty"`
	Error                string   `json:"error,omitempty"`
	ErrorKind            string   `json:"error_kind,omitempty"`
	SizeBytes            *int64   `json:"size_bytes,omitempty"`
	MetaSHA256           string   `json:"meta_sha256,omitempty"`
	ETag                 string   `json:"etag,omitempty"`
	LastModified         string   `json:"last_modified,omitempty"`
	ValidationStatus     string   `json:"validation_status,omitempty"`
	SizeMatch            *bool    `json:"size_match,omitempty"`
	SHA256Match          *bool    `json:"sha256_match,omitempty"`
	ValidationMismatches []string `json:"validation_mismatches,omitempty"`
}

type GitStorageCleanupRecordAudit struct {
	ObjectID       string                         `json:"object_id"`
	Checksum       string                         `json:"checksum,omitempty"`
	NormalizedPath string                         `json:"normalized_path,omitempty"`
	CleanupScope   string                         `json:"cleanup_scope"`
	AccessProbes   []GitStorageCleanupAccessProbe `json:"access_probes"`
	Status         string                         `json:"status,omitempty"`
	Error          string                         `json:"error,omitempty"`
	SizeBytes      int64                          `json:"size,omitempty"`
	LastUpdated    string                         `json:"updated_time,omitempty"`
	DownloadCount  int64                          `json:"download_count,omitempty"`
	LastDownload   string                         `json:"last_download_time,omitempty"`
}

type GitStorageCleanupFinding struct {
	Kind                string                         `json:"kind"`
	NormalizedPath      string                         `json:"normalized_path"`
	Checksum            string                         `json:"checksum,omitempty"`
	ObjectIDs           []string                       `json:"object_ids"`
	Records             []GitStorageCleanupRecordAudit `json:"records"`
	RecommendedAction   string                         `json:"recommended_action"`
	RepoDeleteCandidate bool                           `json:"repo_delete_candidate"`
	CleanupScope        string                         `json:"cleanup_scope"`
	SizeBytes           int64                          `json:"total_bytes,omitempty"`
	LastUpdated         string                         `json:"last_updated,omitempty"`
	DownloadCount       int64                          `json:"download_count,omitempty"`
	LastDownload        string                         `json:"last_download_time,omitempty"`
	Evidence            *GitAuditEvidence              `json:"evidence,omitempty"`
}

type GitStorageCleanupAuditSummary struct {
	CountsByKind             map[string]int `json:"counts_by_kind"`
	TotalFindings            int            `json:"total_findings"`
	ManualFindingCount       int            `json:"manual_finding_count"`
	RepoDeleteCandidateCount int            `json:"repo_delete_candidate_count"`
	StaleDuplicateCount      int            `json:"stale_duplicate_count"`
	RepoOrphanCount          int            `json:"repo_orphan_count"`
}

type GitStorageCleanupAuditResponse struct {
	Findings             []GitStorageCleanupFinding    `json:"findings"`
	Summary              GitStorageCleanupAuditSummary `json:"summary"`
	ExpectedPathCount    int                           `json:"expected_path_count"`
	IncludesRepoManifest bool                          `json:"includes_repo_manifest"`
	PathPrefix           string                        `json:"path_prefix"`
}

type GitStorageCleanupApplyRequest struct {
	GitSubpath                 string   `json:"git_subpath,omitempty"`
	Ref                        string   `json:"ref,omitempty"`
	DeleteRepoOrphans          bool     `json:"delete_repo_orphans,omitempty"`
	DeleteStaleDuplicates      bool     `json:"delete_stale_duplicates,omitempty"`
	DeleteBucketOnlyObjects    bool     `json:"delete_bucket_only_objects,omitempty"`
	RepairBrokenBucketMappings bool     `json:"repair_broken_bucket_mappings,omitempty"`
	DryRun                     bool     `json:"dry_run,omitempty"`
	SelectedRepoPaths          []string `json:"selected_repo_paths,omitempty"`
}

type GitStorageCleanupPurgeResult struct {
	ObjectID string `json:"object_id"`
	Success  *bool  `json:"success"`
	Status   string `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

type GitStorageCleanupApplyResponse struct {
	DeletedRecordIDs        []string                       `json:"deleted_record_ids"`
	DeletedBucketObjectURLs []string                       `json:"deleted_bucket_object_urls"`
	UpdatedRecordIDs        []string                       `json:"updated_record_ids"`
	PurgeResults            []GitStorageCleanupPurgeResult `json:"purge_results"`
	RepoDeletePaths         []string                       `json:"repo_delete_paths"`
	ManualPaths             []string                       `json:"manual_paths"`
	SkippedPaths            []string                       `json:"skipped_paths"`
	DryRun                  bool                           `json:"dry_run"`
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

// GitHubRepositoryMetadata is an alias for domain.GitHubRepositoryMetadata.
type GitHubRepositoryMetadata = domain.GitHubRepositoryMetadata

// HTTPStatusError is an alias for domain.HTTPStatusError.
type HTTPStatusError = domain.HTTPStatusError

func NewGitService(config GitServiceConfig) *GitService {
	if config.GitHubAPIBase == "" {
		config.GitHubAPIBase = "https://api.github.com"
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &GitService{
		config:    config,
		client:    client,
		fenceAPI:  config.FenceClient,
		githubAPI: config.GitHubClient,
	}
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

func ParseRepositoryIdentity(raw string) (GitRepositoryIdentity, error) {
	normalized, err := appconfig.NormalizeProjectRepositoryURL(raw)
	if err != nil {
		return GitRepositoryIdentity{}, err
	}
	parts := strings.Split(normalized, "/")
	if len(parts) != 3 {
		return GitRepositoryIdentity{}, fmt.Errorf("expected normalized host/owner/repo path, got %q", normalized)
	}
	return GitRepositoryIdentity{
		Host:  parts[0],
		Owner: parts[1],
		Repo:  parts[2],
		URL:   fmt.Sprintf("https://%s/%s/%s", parts[0], parts[1], parts[2]),
	}, nil
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}

// StorageBucket is an alias for domain.StorageBucket.
type StorageBucket = domain.StorageBucket

// StorageConfig is an alias for domain.StorageConfig.
type StorageConfig = domain.StorageConfig

func ProgramProjectResourcePath(organization, project string) string {
	return fmt.Sprintf("/programs/%s/projects/%s", organization, project)
}
