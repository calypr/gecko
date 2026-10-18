package project

import (
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
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

type GitRepositoryInstallationStatus = domain.GitRepositoryInstallationStatus

type GitProjectState = geckodb.GitProjectState

type GitProjectStatusResponse struct {
	ProjectID                       string                       `json:"project_id"`
	Organization                    string                       `json:"organization"`
	Project                         string                       `json:"project"`
	ResourcePath                    string                       `json:"resource_path"`
	Accessible                      bool                         `json:"accessible"`
	RequestAccess                   bool                         `json:"request_access"`
	RequestAccessResourcePath       string                       `json:"request_access_resource_path,omitempty"`
	Config                          appconfig.ProjectConfig      `json:"config"`
	Repository                      domain.GitRepositoryIdentity `json:"repository"`
	WorkflowStage                   string                       `json:"workflow_stage,omitempty"`
	InstallationState               string                       `json:"installation_state"`
	InstallationID                  *int64                       `json:"installation_id,omitempty"`
	InstallationTarget              string                       `json:"installation_target,omitempty"`
	InstallationTargetType          string                       `json:"installation_target_type,omitempty"`
	OrganizationAppInstalled        bool                         `json:"organization_app_installed"`
	OrganizationHTMLURL             string                       `json:"organization_html_url,omitempty"`
	OrganizationRepositorySelection string                       `json:"organization_repository_selection,omitempty"`
	SyncState                       string                       `json:"sync_state"`
	DefaultBranch                   string                       `json:"default_branch,omitempty"`
	LastRefreshedAt                 *time.Time                   `json:"last_refreshed_at,omitempty"`
	LastError                       string                       `json:"last_error,omitempty"`
	MirrorReady                     bool                         `json:"mirror_ready"`
}

type GitOrganizationProjectStatus struct {
	ProjectID                 string                          `json:"project_id"`
	Project                   string                          `json:"project"`
	ResourcePath              string                          `json:"resource_path"`
	Repository                domain.GitRepositoryIdentity    `json:"repository"`
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

func OrganizationConfigurationState(appInstalled bool, configuredProjects int, totalProjects int) string {
	switch {
	case !appInstalled:
		return "not_connected"
	case totalProjects == 0:
		return "connected"
	case configuredProjects <= 0:
		return "installed_unconfigured"
	case configuredProjects < totalProjects:
		return "partially_configured"
	default:
		return "connected"
	}
}
