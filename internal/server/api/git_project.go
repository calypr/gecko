package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) resolveGitProject(ctx fiber.Ctx) (string, string, string, appconfig.ProjectConfig, git.GitRepositoryIdentity, *httputil.ErrorResponse) {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	projectID := organization + "/" + project
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			response := httputil.NewError("not_found", fmt.Sprintf("no project config found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
		}
		response := httputil.NewError("database_error", fmt.Sprintf("failed to load project config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	identity, err := git.ParseRepositoryIdentity(cfg.SrcRepo)
	if err != nil {
		response := httputil.NewError("validation_failed", fmt.Sprintf("invalid src_repo for %s: %s", projectID, err), http.StatusBadRequest, map[string]any{"project_id": projectID, "src_repo": cfg.SrcRepo}, nil)
		response.WriteLog(handler.logger)
		return "", "", "", appconfig.ProjectConfig{}, git.GitRepositoryIdentity{}, response
	}
	return organization, project, projectID, cfg, identity, nil
}

func (handler *Handler) handleGitProjectsGET(ctx fiber.Ctx) error {
	states, err := geckodb.ListGitProjectStates(handler.db)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list git state: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	projectIDs, err := geckodb.ConfigListByType(handler.db, string(appconfig.TypeProjects))
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to list project configs: %s", err), http.StatusInternalServerError, nil, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	responses := make([]git.GitProjectStatusResponse, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		parts := strings.SplitN(projectID, "/", 2)
		if len(parts) != 2 {
			continue
		}
		var cfg appconfig.ProjectConfig
		if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
			continue
		}
		identity, err := git.ParseRepositoryIdentity(cfg.SrcRepo)
		if err != nil {
			continue
		}
		var statePtr *geckodb.GitProjectState
		if state, ok := states[projectID]; ok {
			copyState := state
			statePtr = &copyState
		}
		responses = append(responses, handler.gitService.StatusFromState(projectID, parts[0], parts[1], cfg, identity, statePtr))
	}
	return httputil.JSON(responses, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectGET(ctx fiber.Ctx) error {
	organization, project, projectID, cfg, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(handler.gitService.StatusFromState(projectID, organization, project, cfg, identity, state), http.StatusOK).Write(ctx)
}
