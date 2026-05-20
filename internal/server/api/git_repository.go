package api

import (
	"fmt"
	"net/http"
	"strings"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectRefsGET(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	repo, err := git.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refsResponse, err := git.BuildGitRefsResponse(projectID, state.DefaultBranch.String, repo)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to read git refs: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(refsResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectTreeGET(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	repo, err := git.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refName, hash, err := git.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	treeResponse, err := git.BuildGitTreeResponse(projectID, refName, path, repo, hash)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git tree: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(treeResponse, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectFileGET(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, err := geckodb.GitProjectStateByProjectID(handler.db, projectID)
	if err != nil {
		response := httputil.NewError("database_error", fmt.Sprintf("failed to read git state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if state == nil || state.MirrorPath == "" {
		response := httputil.NewError("conflict", fmt.Sprintf("project %s has not been refreshed yet", projectID), http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	repo, err := git.OpenRepository(state.MirrorPath)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to open git mirror: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	refName, hash, err := git.ResolveGitReference(repo, strings.TrimSpace(ctx.Query("ref")), state.DefaultBranch.String)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to resolve git ref: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": ctx.Query("ref")}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	path := strings.Trim(ctx.Params("*"), "/")
	fileResponse, err := git.BuildGitFileResponse(projectID, refName, path, repo, hash)
	if err != nil {
		response := httputil.NewError("not_found", fmt.Sprintf("failed to read git file: %s", err), http.StatusNotFound, map[string]any{"project_id": projectID, "ref": refName, "path": path}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(fileResponse, http.StatusOK).Write(ctx)
}
