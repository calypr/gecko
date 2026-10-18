package project

import (
	"os"
	"strings"

	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/repo"
)

func StatusFromState(projectID string, organization string, project string, cfg appconfig.ProjectConfig, identity repo.Identity, state *geckodb.GitProjectState, orgState *geckodb.GitOrganizationState) GitProjectStatusResponse {
	workflowStage := ""
	if strings.TrimSpace(cfg.SrcRepo) == "" {
		workflowStage = GitWorkflowStageAwaitingGitHubConnect
	}
	response := GitProjectStatusResponse{
		ProjectID:                 projectID,
		Organization:              organization,
		Project:                   project,
		ResourcePath:              access.ProgramProjectResourcePath(organization, project),
		RequestAccessResourcePath: access.ProgramProjectResourcePath(organization, project),
		Config:                    cfg,
		Repository:                identity,
		WorkflowStage:             workflowStage,
		InstallationState:         GitInstallationNotConnected,
		SyncState:                 GitSyncNeverSynced,
	}
	if orgState != nil {
		response.OrganizationAppInstalled = orgState.Installed
		if orgState.HTMLURL.Valid {
			response.OrganizationHTMLURL = orgState.HTMLURL.String
		}
		if orgState.RepositorySelection.Valid {
			response.OrganizationRepositorySelection = orgState.RepositorySelection.String
		}
	}
	if state == nil {
		if response.WorkflowStage == "" && response.OrganizationAppInstalled && response.OrganizationRepositorySelection == "all" {
			response.WorkflowStage = GitWorkflowStageGitHubConnected
			response.InstallationState = GitInstallationConnected
		}
		return response
	}
	if state.InstallationID.Valid || state.InstallationTarget.Valid {
		response.InstallationState = GitInstallationConnected
		response.WorkflowStage = GitWorkflowStageGitHubConnected
	}
	if state.InstallationID.Valid {
		installationID := state.InstallationID.Int64
		response.InstallationID = &installationID
	}
	if state.InstallationTarget.Valid {
		response.InstallationTarget = state.InstallationTarget.String
	}
	if state.InstallationTargetType.Valid {
		response.InstallationTargetType = state.InstallationTargetType.String
	}
	if state.SyncState != "" {
		response.SyncState = state.SyncState
	}
	if state.DefaultBranch.Valid {
		response.DefaultBranch = state.DefaultBranch.String
	}
	if state.LastRefreshedAt.Valid {
		refreshedAt := state.LastRefreshedAt.Time
		response.LastRefreshedAt = &refreshedAt
	}
	if state.LastError.Valid {
		response.LastError = state.LastError.String
	}
	if state.MirrorPath != "" {
		if info, err := os.Stat(state.MirrorPath); err == nil && info.IsDir() {
			response.MirrorReady = true
		}
	}
	return response
}
