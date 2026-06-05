package git

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) resolveExistingProject(ctx fiber.Ctx) (string, string, string, *httputil.ErrorResponse) {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		response := httputil.NewError("invalid_request", "organization and project are required", http.StatusBadRequest, nil, nil)
		response.WriteLog(handler.logger)
		return "", "", "", response
	}
	projectID := organization + "/" + project
	var cfg appconfig.ProjectConfig
	if err := geckodb.ConfigGETGeneric(handler.db, projectID, string(appconfig.TypeProjects), &cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			response := httputil.NewError("not_found", fmt.Sprintf("no project config found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return "", "", "", response
		}
		response := httputil.NewError("database_error", fmt.Sprintf("failed to load project config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return "", "", "", response
	}
	return organization, project, projectID, nil
}

func (handler *Handler) handleGitProjectThumbnailGET(ctx fiber.Ctx) error {
	organization, project, projectID, errResponse := handler.resolveExistingProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	path, contentType, err := handler.gitService.ProjectThumbnailPath(organization, project)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			response := httputil.NewError("not_found", fmt.Sprintf("no thumbnail found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError("internal_error", fmt.Sprintf("failed to load project thumbnail: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if strings.TrimSpace(contentType) != "" {
		ctx.Set(fiber.HeaderContentType, contentType)
	}
	return ctx.SendFile(path)
}

func (handler *Handler) handleGitProjectThumbnailPUT(ctx fiber.Ctx) error {
	organization, project, projectID, errResponse := handler.resolveExistingProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	fileHeader, err := ctx.FormFile("thumbnail")
	if err != nil {
		response := httputil.NewError("validation_failed", "thumbnail image is required", http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if fileHeader.Size > int64(git.MaxProjectThumbnailBytes()) {
		response := httputil.NewError("validation_failed", fmt.Sprintf("thumbnail image must be %d bytes or smaller", git.MaxProjectThumbnailBytes()), http.StatusRequestEntityTooLarge, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	file, err := fileHeader.Open()
	if err != nil {
		response := httputil.NewError("invalid_request", fmt.Sprintf("failed to open thumbnail upload: %s", err), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, int64(git.MaxProjectThumbnailBytes())+1))
	if err != nil {
		response := httputil.NewError("invalid_request", fmt.Sprintf("failed to read thumbnail upload: %s", err), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if len(data) > git.MaxProjectThumbnailBytes() {
		response := httputil.NewError("validation_failed", fmt.Sprintf("thumbnail image must be %d bytes or smaller", git.MaxProjectThumbnailBytes()), http.StatusRequestEntityTooLarge, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}

	_, contentType, err := handler.gitService.SaveProjectThumbnail(organization, project, data)
	if err != nil {
		response := httputil.NewError(
			"validation_failed",
			fmt.Sprintf(
				"failed to save project thumbnail: %s. Allowed formats: PNG or JPG. Allowed dimensions: %dpx to %dpx. Maximum size: %d bytes",
				err,
				git.MinProjectThumbnailPixels(),
				git.MaxProjectThumbnailPixels(),
				git.MaxProjectThumbnailBytes(),
			),
			http.StatusBadRequest,
			map[string]any{"project_id": projectID},
			nil,
		)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true, "project_id": projectID, "content_type": contentType}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectThumbnailDELETE(ctx fiber.Ctx) error {
	organization, project, projectID, errResponse := handler.resolveExistingProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	err := handler.gitService.DeleteProjectThumbnail(organization, project)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to delete project thumbnail: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true, "project_id": projectID}, http.StatusOK).Write(ctx)
}
