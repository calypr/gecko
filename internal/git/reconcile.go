package git

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/integrations/syfon"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/jmoiron/sqlx"
)

type ReconcileService struct {
	db      *sqlx.DB
	storage *syfon.Manager
	git     *GitService
}

func NewReconcileService(db *sqlx.DB, storage *syfon.Manager, gitService *GitService) *ReconcileService {
	return &ReconcileService{
		db:      db,
		storage: storage,
		git:     gitService,
	}
}

func (service *ReconcileService) ReconcileOrganizations(ctx context.Context, authorizationHeader string, projectIDs []string) error {
	for _, organization := range projectConfigOrganizations(projectIDs) {
		if err := service.ReconcileOrganization(ctx, organization, authorizationHeader, projectIDs); err != nil {
			return err
		}
	}
	return nil
}

func (service *ReconcileService) ReconcileOrganization(ctx context.Context, organization string, authorizationHeader string, projectIDs []string) error {
	now := time.Now().UTC()
	existingOrgState, _ := geckodb.GitOrganizationStateByOrganization(service.db, organization)
	projects := make([]trackedProject, 0)
	owners := make(map[string]struct{})
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] != organization {
			continue
		}
		var cfg appconfig.ProjectConfig
		if err := geckodb.ConfigGETGeneric(service.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
			continue
		}
		identity, err := ParseRepositoryIdentity(cfg.SrcRepo)
		if err != nil {
			projectState, _ := geckodb.GitProjectStateByProjectID(service.db, projectID)
			if projectState != nil {
				_ = geckodb.UpsertGitProjectState(service.db, *projectState)
			}
			continue
		}
		owners[identity.Owner] = struct{}{}
		projects = append(projects, trackedProject{projectID: projectID, cfg: cfg, identity: identity})
	}

	orgInstallation := GitRepositoryInstallationStatus{}
	if len(owners) > 0 {
		sortedOwners := make([]string, 0, len(owners))
		for owner := range owners {
			sortedOwners = append(sortedOwners, owner)
		}
		sort.Strings(sortedOwners)

		for _, owner := range sortedOwners {
			installation, err := service.git.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
			if err != nil {
				if statusErr, ok := err.(*HTTPStatusError); ok {
					if statusErr.StatusCode == http.StatusNotFound {
						continue
					}
					return NewError(ErrorKindIntegration, statusErr.StatusCode, statusErr.Message, map[string]any{"organization": organization, "github_owner": owner})
				}
				return WrapError(ErrorKindIntegration, http.StatusBadGateway, "failed to load GitHub organization installation status", err, map[string]any{"organization": organization, "github_owner": owner})
			}
			if installation.Installed {
				orgInstallation = installation
				break
			}
		}
	} else if existingOrgState != nil && existingOrgState.Installed {
		orgInstallation.Installed = true
		if existingOrgState.InstallationID.Valid {
			installationID := existingOrgState.InstallationID.Int64
			orgInstallation.InstallationID = &installationID
		}
		if existingOrgState.InstallationTarget.Valid {
			orgInstallation.Target = existingOrgState.InstallationTarget.String
		}
		if existingOrgState.InstallationTargetType.Valid {
			orgInstallation.TargetType = existingOrgState.InstallationTargetType.String
		}
		if existingOrgState.HTMLURL.Valid {
			orgInstallation.HTMLURL = existingOrgState.HTMLURL.String
		}
		if existingOrgState.RepositorySelection.Valid {
			orgInstallation.RepositorySelection = existingOrgState.RepositorySelection.String
		}
	}

	orgState := geckodb.GitOrganizationState{
		Organization: organization,
		Installed:    orgInstallation.Installed,
		UpdatedAt:    now,
		LastSeenAt:   sql.NullTime{Time: now, Valid: true},
		ConfiguredAt: sql.NullTime{Time: now, Valid: orgInstallation.Installed},
		LastError:    sql.NullString{},
	}
	if orgInstallation.Installed {
		if orgInstallation.InstallationID != nil {
			orgState.InstallationID = sql.NullInt64{Int64: *orgInstallation.InstallationID, Valid: true}
		}
		if orgInstallation.Target != "" {
			orgState.InstallationTarget = sql.NullString{String: orgInstallation.Target, Valid: true}
		}
		if orgInstallation.TargetType != "" {
			orgState.InstallationTargetType = sql.NullString{String: orgInstallation.TargetType, Valid: true}
		}
		if orgInstallation.HTMLURL != "" {
			orgState.HTMLURL = sql.NullString{String: orgInstallation.HTMLURL, Valid: true}
		}
		if orgInstallation.RepositorySelection != "" {
			orgState.RepositorySelection = sql.NullString{String: orgInstallation.RepositorySelection, Valid: true}
		}
	}
	if err := geckodb.UpsertGitOrganizationState(service.db, orgState); err != nil {
		return WrapError(ErrorKindDatabase, http.StatusInternalServerError, "failed to persist git organization state", err, map[string]any{"organization": organization})
	}

	for _, tracked := range projects {
		if err := service.reconcileProject(ctx, authorizationHeader, organization, tracked, orgInstallation); err != nil {
			return err
		}
	}
	return nil
}

func (service *ReconcileService) BuildOrganizationsStatus(ctx context.Context, authorizationHeader string, projectIDs []string, allowedResources []string) (GitOrganizationsStatusResponse, error) {
	projectIDs = filterProjectIDsByAllowedResources(projectIDs, allowedResources)
	organizations := projectConfigOrganizations(projectIDs)
	buckets, bucketsErr := service.storage.ListBuckets(ctx, authorizationHeader)
	projectStates, err := geckodb.ListGitProjectStates(service.db)
	if err != nil {
		return GitOrganizationsStatusResponse{}, WrapError(ErrorKindDatabase, http.StatusInternalServerError, "failed to list git project states", err, nil)
	}
	organizationStates, err := geckodb.ListGitOrganizationStates(service.db)
	if err != nil {
		return GitOrganizationsStatusResponse{}, WrapError(ErrorKindDatabase, http.StatusInternalServerError, "failed to list git organization states", err, nil)
	}
	responsePayload := GitOrganizationsStatusResponse{Organizations: make([]GitOrganizationStatusResponse, 0, len(organizations))}
	for _, organization := range organizations {
		organizationStatus, err := service.BuildOrganizationStatus(ctx, organization, projectIDs, projectStates, organizationStates, allowedResources, buckets, bucketsErr)
		if err != nil {
			return GitOrganizationsStatusResponse{}, err
		}
		responsePayload.Organizations = append(responsePayload.Organizations, organizationStatus)
		responsePayload.TotalProjects += organizationStatus.TotalProjects
		responsePayload.ConnectedProjects += organizationStatus.ConnectedProjects
		responsePayload.ConfiguredProjects += organizationStatus.ConfiguredProjects
		if organizationStatus.Connected {
			responsePayload.ConnectedOrganizations++
		}
		if organizationStatus.AppInstalled {
			responsePayload.InstalledOrganizations++
		}
	}
	responsePayload.TotalOrganizations = len(responsePayload.Organizations)
	responsePayload.AppInstalled = responsePayload.InstalledOrganizations > 0
	responsePayload.Connected = responsePayload.AppInstalled
	responsePayload.ConfigurationState = OrganizationConfigurationState(responsePayload.AppInstalled, responsePayload.ConfiguredProjects, responsePayload.TotalProjects)
	return responsePayload, nil
}

func (service *ReconcileService) BuildSingleOrganizationStatus(ctx context.Context, authorizationHeader string, organization string, projectIDs []string, allowedResources []string) (GitOrganizationStatusResponse, error) {
	projectStates, err := geckodb.ListGitProjectStates(service.db)
	if err != nil {
		return GitOrganizationStatusResponse{}, WrapError(ErrorKindDatabase, http.StatusInternalServerError, "failed to list git project states", err, map[string]any{"organization": organization})
	}
	organizationStates, err := geckodb.ListGitOrganizationStates(service.db)
	if err != nil {
		return GitOrganizationStatusResponse{}, WrapError(ErrorKindDatabase, http.StatusInternalServerError, "failed to list git organization states", err, map[string]any{"organization": organization})
	}
	buckets, bucketsErr := service.storage.ListBuckets(ctx, authorizationHeader)
	return service.BuildOrganizationStatus(ctx, organization, projectIDs, projectStates, organizationStates, allowedResources, buckets, bucketsErr)
}

func (service *ReconcileService) BuildOrganizationStatus(ctx context.Context, organization string, projectIDs []string, projectStates map[string]geckodb.GitProjectState, organizationStates map[string]geckodb.GitOrganizationState, allowedResources []string, buckets map[string]StorageBucket, bucketsErr error) (GitOrganizationStatusResponse, error) {
	responsePayload := GitOrganizationStatusResponse{
		Organization: organization,
		Projects:     make([]GitOrganizationProjectStatus, 0),
	}
	orgState, hasOrgState := organizationStates[organization]
	if hasOrgState {
		responsePayload.AppInstalled = orgState.Installed
		responsePayload.Connected = orgState.Installed
		if orgState.InstallationID.Valid {
			installationID := orgState.InstallationID.Int64
			responsePayload.InstallationID = &installationID
		}
		if orgState.HTMLURL.Valid {
			responsePayload.HTMLURL = orgState.HTMLURL.String
		}
		if orgState.RepositorySelection.Valid {
			responsePayload.RepositorySelection = orgState.RepositorySelection.String
		}
	}

	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] != organization {
			continue
		}
		if !servermw.ResourceListAllowsProject(allowedResources, parts[0], parts[1]) {
			continue
		}
		var cfg appconfig.ProjectConfig
		if err := geckodb.ConfigGETGeneric(service.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
			continue
		}
		identity, _ := ParseRepositoryIdentity(cfg.SrcRepo)
		state, hasProjectState := projectStates[projectID]
		if !hasProjectState {
			state = geckodb.GitProjectState{
				ProjectID: projectID,
				RepoHost:  identity.Host,
				RepoOwner: identity.Owner,
				RepoName:  identity.Repo,
				SyncState: GitSyncNeverSynced,
			}
		}
		installation := buildInstallationStatus(responsePayload, state, identity.Owner)
		workflowStage := ""
		if strings.TrimSpace(cfg.SrcRepo) == "" {
			workflowStage = GitWorkflowStageAwaitingGitHubConnect
		} else if installation.Installed {
			workflowStage = GitWorkflowStageGitHubConnected
		}
		integrations := ProjectIntegrationStatus{
			GitHub: ProjectIntegrationCheck{
				Pass: installation.Installed,
			},
			Storage: deriveStorageIntegrationCheck(buckets, bucketsErr, parts[0], parts[1]),
		}
		if strings.TrimSpace(cfg.SrcRepo) == "" {
			integrations.GitHub.Reason = GitWorkflowStageAwaitingGitHubConnect
			if responsePayload.AppInstalled {
				integrations.GitHub.Details = "Project creation is complete. Finish the GitHub connect step to link this project to a repository."
			} else {
				integrations.GitHub.Details = "Project creation is complete. Connect GitHub for this organization, then finish the GitHub connect step for this project."
			}
		} else if !installation.Installed {
			integrations.GitHub.Reason = "missing_github_connection"
			integrations.GitHub.Details = "GitHub App is not connected to this repository"
		}
		configured := integrations.GitHub.Pass && integrations.Storage.Pass
		readable := servermw.ResourceListAllowsProject(allowedResources, parts[0], parts[1])
		responsePayload.Projects = append(responsePayload.Projects, GitOrganizationProjectStatus{
			ProjectID:                 projectID,
			Project:                   parts[1],
			ResourcePath:              ProgramProjectResourcePath(parts[0], parts[1]),
			Repository:                identity,
			WorkflowStage:             workflowStage,
			Configured:                configured,
			Integrations:              integrations,
			Accessible:                readable,
			RequestAccess:             !readable,
			RequestAccessResourcePath: ProgramProjectResourcePath(parts[0], parts[1]),
			Installation:              installation,
		})
	}
	responsePayload.TotalProjects = len(responsePayload.Projects)
	for _, projectStatus := range responsePayload.Projects {
		if projectStatus.Installation.Installed {
			responsePayload.ConnectedProjects++
		}
		if projectStatus.Configured {
			responsePayload.ConfiguredProjects++
		}
	}
	responsePayload.ConfigurationState = OrganizationConfigurationState(responsePayload.AppInstalled, responsePayload.ConfiguredProjects, responsePayload.TotalProjects)
	return responsePayload, nil
}

type trackedProject struct {
	projectID string
	cfg       appconfig.ProjectConfig
	identity  GitRepositoryIdentity
}

func (service *ReconcileService) reconcileProject(ctx context.Context, authorizationHeader, organization string, tracked trackedProject, ownerInstallation GitRepositoryInstallationStatus) error {
	_, project := splitProjectID(tracked.projectID)
	projectState, _ := geckodb.GitProjectStateByProjectID(service.db, tracked.projectID)
	if projectState == nil {
		projectState = &geckodb.GitProjectState{
			ProjectID: tracked.projectID,
			RepoHost:  tracked.identity.Host,
			RepoOwner: tracked.identity.Owner,
			RepoName:  tracked.identity.Repo,
			SyncState: GitSyncNeverSynced,
		}
	}
	if ownerInstallation.Installed && ownerInstallation.RepositorySelection == "all" {
		applyInstalledState(projectState, ownerInstallation.InstallationID, tracked.identity.Owner)
		_ = geckodb.UpsertGitProjectState(service.db, *projectState)
		return nil
	}
	accessToken, err := service.git.RequestInstallationToken(ctx, authorizationHeader, organization, project, tracked.identity, "read")
	if err != nil {
		if statusErr, ok := err.(*HTTPStatusError); ok && (statusErr.StatusCode == http.StatusForbidden || statusErr.StatusCode == http.StatusNotFound) {
			clearInstalledState(projectState)
			_ = geckodb.UpsertGitProjectState(service.db, *projectState)
			return nil
		}
		return WrapError(ErrorKindIntegration, http.StatusBadGateway, "failed to obtain GitHub installation token", err, map[string]any{"organization": organization, "project_id": tracked.projectID, "repository": tracked.cfg.SrcRepo})
	}
	if _, err := service.git.FetchRepositoryMetadata(ctx, accessToken, tracked.identity); err != nil {
		clearInstalledState(projectState)
		_ = geckodb.UpsertGitProjectState(service.db, *projectState)
		return nil
	}
	applyInstalledState(projectState, ownerInstallation.InstallationID, tracked.identity.Owner)
	_ = geckodb.UpsertGitProjectState(service.db, *projectState)
	return nil
}

func splitProjectID(projectID string) (string, string) {
	organization, project, _ := strings.Cut(projectID, "/")
	return strings.TrimSpace(organization), strings.TrimSpace(project)
}

func deriveStorageIntegrationCheck(buckets map[string]StorageBucket, bucketsErr error, organization string, project string) ProjectIntegrationCheck {
	check := ProjectIntegrationCheck{
		Pass:   false,
		Reason: "missing_storage_scope",
	}
	if bucketsErr != nil {
		check.Details = bucketsErr.Error()
		return check
	}
	expectedPrograms := fmt.Sprintf("/programs/%s/projects/%s", strings.TrimSpace(organization), strings.TrimSpace(project))
	expectedOrganization := fmt.Sprintf("/organization/%s/project/%s", strings.TrimSpace(organization), strings.TrimSpace(project))
	for _, metadata := range buckets {
		for _, resource := range metadata.Resources {
			normalized := strings.TrimSpace(resource)
			if normalized == expectedPrograms || normalized == expectedOrganization {
				check.Pass = true
				check.Reason = ""
				return check
			}
		}
	}
	check.Details = "No Syfon bucket scope matched this project"
	return check
}

func buildInstallationStatus(organizationStatus GitOrganizationStatusResponse, state geckodb.GitProjectState, owner string) GitRepositoryInstallationStatus {
	installation := GitRepositoryInstallationStatus{}
	if organizationStatus.AppInstalled && organizationStatus.RepositorySelection == "all" {
		installation.Installed = true
		installation.InstallationID = organizationStatus.InstallationID
		installation.Target = owner
		installation.TargetType = "Organization"
		installation.HTMLURL = organizationStatus.HTMLURL
		installation.RepositorySelection = organizationStatus.RepositorySelection
		return installation
	}
	if state.InstallationID.Valid || state.InstallationTarget.Valid {
		installation.Installed = true
		if state.InstallationID.Valid {
			installationID := state.InstallationID.Int64
			installation.InstallationID = &installationID
		}
		if state.InstallationTarget.Valid {
			installation.Target = state.InstallationTarget.String
		}
		if state.InstallationTargetType.Valid {
			installation.TargetType = state.InstallationTargetType.String
		}
		if organizationStatus.HTMLURL != "" {
			installation.HTMLURL = organizationStatus.HTMLURL
		}
		if organizationStatus.RepositorySelection != "" {
			installation.RepositorySelection = organizationStatus.RepositorySelection
		}
	}
	return installation
}

func clearInstalledState(state *geckodb.GitProjectState) {
	state.InstallationID = sql.NullInt64{}
	state.InstallationTarget = sql.NullString{}
	state.InstallationTargetType = sql.NullString{}
}

func applyInstalledState(state *geckodb.GitProjectState, installationID *int64, owner string) {
	if installationID != nil {
		state.InstallationID = sql.NullInt64{Int64: *installationID, Valid: true}
	} else {
		state.InstallationID = sql.NullInt64{}
	}
	state.InstallationTarget = sql.NullString{String: owner, Valid: owner != ""}
	state.InstallationTargetType = sql.NullString{String: "Organization", Valid: true}
}

func filterProjectIDsByAllowedResources(projectIDs []string, allowedResources []string) []string {
	if len(allowedResources) == 0 {
		return []string{}
	}
	filtered := make([]string, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 {
			continue
		}
		projectParts := strings.SplitN(parts[1], "/", 2)
		if len(projectParts) != 1 || projectParts[0] == "" {
			continue
		}
		if servermw.ResourceListAllowsProject(allowedResources, parts[0], projectParts[0]) {
			filtered = append(filtered, projectID)
		}
	}
	sort.Strings(filtered)
	return filtered
}

func projectConfigOrganizations(projectIDs []string) []string {
	organizations := make([]string, 0)
	seen := make(map[string]struct{})
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		if _, ok := seen[parts[0]]; ok {
			continue
		}
		seen[parts[0]] = struct{}{}
		organizations = append(organizations, parts[0])
	}
	sort.Strings(organizations)
	return organizations
}
