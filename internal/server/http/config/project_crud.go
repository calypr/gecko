package config

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) buildProjectSummaryResponse(projectID string, cfg config.ProjectConfig) (ProjectSummaryResponse, bool) {
	projectOrganization, projectName, found := strings.Cut(projectID, "/")
	if !found {
		return ProjectSummaryResponse{}, false
	}

	organization := strings.TrimSpace(projectOrganization)
	project := strings.TrimSpace(projectName)
	title := strings.TrimSpace(cfg.Title)
	if title == "" {
		title = strings.TrimSpace(cfg.ProjectTitle)
	}
	if title == "" {
		title = project
	}

	summary := ProjectSummaryResponse{
		Organization: organization,
		Project:      project,
		Title:        title,
		ContactEmail: strings.TrimSpace(cfg.ContactEmail),
		Description:  strings.TrimSpace(cfg.Description),
	}

	if handler.thumbnailStore != nil {
		if _, _, err := handler.thumbnailStore.GetPath(organization, project); err == nil {
			summary.ThumbnailURL = fmt.Sprintf(
				"/gecko/git/projects/%s/%s/thumbnail",
				url.PathEscape(organization),
				url.PathEscape(project),
			)
		}
	}

	return summary, true
}

func (handler *Handler) handleProjectConfigGET(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	return handler.handleConfigGETByID(ctx, configType, configID)
}

func (handler *Handler) handleProjectSummaryGET(ctx fiber.Ctx) error {
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(config.TypeProjects))
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Database error: %s", err), http.StatusInternalServerError, map[string]any{"config_type": string(config.TypeProjects)}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	summaries := make([]ProjectSummaryResponse, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		var cfg config.ProjectConfig
		if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(config.TypeProjects), &cfg); err != nil {
			continue
		}
		summary, ok := handler.buildProjectSummaryResponse(projectID, cfg)
		if !ok {
			continue
		}
		summaries = append(summaries, summary)
	}

	sort.Slice(summaries, func(i, j int) bool {
		leftTitle := strings.ToLower(strings.TrimSpace(summaries[i].Title))
		rightTitle := strings.ToLower(strings.TrimSpace(summaries[j].Title))
		if leftTitle != rightTitle {
			return leftTitle < rightTitle
		}
		leftOrg := strings.ToLower(strings.TrimSpace(summaries[i].Organization))
		rightOrg := strings.ToLower(strings.TrimSpace(summaries[j].Organization))
		if leftOrg != rightOrg {
			return leftOrg < rightOrg
		}
		return strings.ToLower(strings.TrimSpace(summaries[i].Project)) <
			strings.ToLower(strings.TrimSpace(summaries[j].Project))
	})

	return httputil.JSON(summaries, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleProjectConfigPUT(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	cfg, errResponse := configForType(configType)
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if errResponse = httputil.ParseJSONBody(ctx.Body(), cfg, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	switch typed := cfg.(type) {
	case *config.ProjectConfig:
		projectOrganization, _, found := strings.Cut(configID, "/")
		if !found {
			errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("invalid project config id: %s", configID), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
		typed.OrgTitle = strings.TrimSpace(projectOrganization)
		if err := typed.ValidateInitialization(); err != nil {
			errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	default:
		if validatable, ok := cfg.(interface{ Validate() error }); ok {
			if err := validatable.Validate(); err != nil {
				errResponse = httputil.NewError(apierror.TypeValidationFailed, fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"config_type": configType, "config_id": configID}, nil)
				errResponse.WriteLog(handler.logger)
				return errResponse.Write(ctx)
			}
		}
	}
	if err := geckodb.ConfigPUTGeneric(handler.db, configID, configType, cfg); err != nil {
		errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("configPut failed: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	if projectCfg, ok := cfg.(*config.ProjectConfig); ok {
		if handler.gitService != nil {
			if identity, err := git.ParseRepositoryIdentity(projectCfg.SrcRepo); err == nil {
				if existingState, existingErr := geckodb.GitProjectStateByProjectID(handler.db, configID); existingErr != nil {
					errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("read existing git project state failed: %s", existingErr), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
					errResponse.WriteLog(handler.logger)
					return errResponse.Write(ctx)
				} else if existingState != nil &&
					existingState.RepoHost == identity.Host &&
					existingState.RepoOwner == identity.Owner &&
					existingState.RepoName == identity.Repo {
					return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
				}
				state := geckodb.GitProjectState{
					ProjectID:  configID,
					RepoHost:   identity.Host,
					RepoOwner:  identity.Owner,
					RepoName:   identity.Repo,
					MirrorPath: handler.gitService.MirrorPathForIdentity(identity),
					SyncState:  git.GitSyncNeverSynced,
				}
				if upsertErr := geckodb.UpsertGitProjectState(handler.db, state); upsertErr != nil {
					errResponse = httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("upsert git project state failed: %s", upsertErr), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
					errResponse.WriteLog(handler.logger)
					return errResponse.Write(ctx)
				}
			}
		}
	}

	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("ACCEPTED: %s for type: %s", configID, configType)}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleProjectConfigDELETE(ctx fiber.Ctx) error {
	configType, configID := handler.resolveProjectConfigParams(ctx)
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if errResponse := handler.deleteProject(ctx, authorizationHeader, configType, configID, organization, project); errResponse != nil {
		return writeAppError(ctx, handler.logger, errResponse)
	}
	return handler.handleConfigDELETEByID(ctx, configType, configID)
}

func (handler *Handler) deleteProject(ctx fiber.Ctx, authorizationHeader, configType, configID, organization, project string) error {
	resourcePath := git.ProgramProjectResourcePath(organization, project)
	handler.logger.Info(
		"starting project delete workflow: config_id=%s resource_path=%s organization=%s project=%s",
		configID,
		resourcePath,
		organization,
		project,
	)
	if handler.projectSetup != nil {
		if err := handler.projectSetup.CleanupProjectStorage(ctx.Context(), authorizationHeader, organization, project); err != nil {
			handler.logger.Error(
				"project delete failed during storage cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
				configID,
				resourcePath,
				organization,
				project,
				err,
			)
			return err
		}
	}
	if err := geckodb.DeleteGitProjectArtifacts(handler.db, configID); err != nil {
		handler.logger.Error(
			"project delete failed during gecko git artifact cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
			configID,
			resourcePath,
			organization,
			project,
			err,
		)
		return git.WrapError(
			git.ErrorKindDatabase,
			http.StatusInternalServerError,
			"failed during project deletion step gecko_git_artifact_cleanup",
			err,
			map[string]any{
				"config_type":   configType,
				"config_id":     configID,
				"organization":  organization,
				"project":       project,
				"resource_path": resourcePath,
				"delete_step":   "gecko_git_artifact_cleanup",
			},
		)
	}
	if handler.thumbnailStore != nil {
		if err := handler.thumbnailStore.Delete(organization, project); err != nil && !errors.Is(err, thumbnail.ErrNoThumbnail) {
			handler.logger.Error(
				"project delete failed during thumbnail cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v",
				configID,
				resourcePath,
				organization,
				project,
				err,
			)
			return git.WrapError(
				git.ErrorKindIntegration,
				http.StatusInternalServerError,
				"failed during project deletion step thumbnail_cleanup",
				err,
				map[string]any{
					"config_type":   configType,
					"config_id":     configID,
					"organization":  organization,
					"project":       project,
					"resource_path": resourcePath,
					"delete_step":   "thumbnail_cleanup",
				},
			)
		}
	}
	if err := git.DeleteAuthzResource(ctx.Context(), authorizationHeader, resourcePath); err != nil {
		handler.logger.Error(
			"project delete failed during arborist resource delete: config_id=%s resource_path=%s organization=%s project=%s err=%v",
			configID,
			resourcePath,
			organization,
			project,
			err,
		)
		return git.WrapError(
			git.ErrorKindIntegration,
			http.StatusBadGateway,
			"failed during project deletion step arborist_resource_delete",
			err,
			map[string]any{
				"config_type":    configType,
				"config_id":      configID,
				"organization":   organization,
				"project":        project,
				"resource_path":  resourcePath,
				"delete_step":    "arborist_resource_delete",
				"upstream_error": err.Error(),
			},
		)
	}
	handler.logger.Info(
		"project delete downstream cleanup succeeded: config_id=%s resource_path=%s organization=%s project=%s",
		configID,
		resourcePath,
		organization,
		project,
	)
	return nil
}

func (handler *Handler) handleProjectOrganizationDELETE(ctx fiber.Ctx) error {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	if organization == "" {
		errResponse := httputil.NewError("invalid_request", "organization is required", http.StatusBadRequest, nil, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := servermw.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		errResponse := httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, map[string]any{"organization": organization}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(config.TypeProjects))
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to list projects for organization delete: %s", err), http.StatusInternalServerError, map[string]any{"organization": organization}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	for _, projectID := range projectIDs {
		projectOrganization, projectName, found := strings.Cut(projectID, "/")
		if !found || strings.TrimSpace(projectOrganization) != organization {
			continue
		}
		projectName = strings.TrimSpace(projectName)
		if projectName == "" {
			continue
		}
		if appErr := handler.deleteProject(ctx, authorizationHeader, string(config.TypeProjects), projectID, organization, projectName); appErr != nil {
			return writeAppError(ctx, handler.logger, appErr)
		}
		if _, err := geckodb.ConfigDELETEGeneric(handler.db, projectID, string(config.TypeProjects)); err != nil {
			errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to delete project config %s during organization delete: %s", projectID, err), http.StatusInternalServerError, map[string]any{"organization": organization, "project_id": projectID}, nil)
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	resourcePath := fmt.Sprintf("/programs/%s", organization)
	if err := git.DeleteAuthzResource(ctx.Context(), authorizationHeader, resourcePath); err != nil {
		errResponse := httputil.NewError("integration_error", fmt.Sprintf("failed to delete arborist organization resource: %s", err), http.StatusBadGateway, map[string]any{"organization": organization, "resource_path": resourcePath}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true}, http.StatusOK).Write(ctx)
}
