package git

import (
	"errors"
	"fmt"
	"github.com/calypr/gecko/internal/server/httpapi/core"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) resolveExistingProject(ctx fiber.Ctx) (string, string, string, error) {
	organization := strings.TrimSpace(ctx.Params("orgTitle"))
	project := strings.TrimSpace(ctx.Params("projectTitle"))
	if organization == "" || project == "" {
		return "", "", "", domain.NewError(domain.ErrorKindValidation, http.StatusBadRequest, "organization and project are required", nil)
	}
	projectID := organization + "/" + project
	if _, err := handler.ProjectRead.ProjectConfig(ctx.Context(), projectID); err != nil {
		return "", "", "", err
	}
	return organization, project, projectID, nil
}

func (handler *Handler) handleGitProjectThumbnailGET(ctx fiber.Ctx) error {
	organization, project, projectID, err := handler.resolveExistingProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	path, contentType, err := handler.ThumbnailStore.GetPath(organization, project)
	if err != nil {
		if errors.Is(err, thumbnail.ErrNoThumbnail) {
			response := httputil.NewError("not_found", fmt.Sprintf("no thumbnail found for %s", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
			response.WriteLog(handler.Logger)
			return response.Write(ctx)
		}
		response := httputil.NewError("internal_error", fmt.Sprintf("failed to load project thumbnail: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	if strings.TrimSpace(contentType) != "" {
		ctx.Set(fiber.HeaderContentType, contentType)
	}
	return ctx.SendFile(path)
}

func (handler *Handler) handleGitProjectThumbnailPUT(ctx fiber.Ctx) error {
	organization, project, projectID, err := handler.resolveExistingProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	fileHeader, err := ctx.FormFile("thumbnail")
	if err != nil {
		response := httputil.NewError("validation_failed", "thumbnail image is required", http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	if fileHeader.Size > int64(thumbnail.MaxProjectThumbnailBytes) {
		response := httputil.NewError("validation_failed", fmt.Sprintf("thumbnail image must be %d bytes or smaller", thumbnail.MaxProjectThumbnailBytes), http.StatusRequestEntityTooLarge, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	file, err := fileHeader.Open()
	if err != nil {
		response := httputil.NewError("invalid_request", fmt.Sprintf("failed to open thumbnail upload: %s", err), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, int64(thumbnail.MaxProjectThumbnailBytes)+1))
	if err != nil {
		response := httputil.NewError("invalid_request", fmt.Sprintf("failed to read thumbnail upload: %s", err), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	if len(data) > thumbnail.MaxProjectThumbnailBytes {
		response := httputil.NewError("validation_failed", fmt.Sprintf("thumbnail image must be %d bytes or smaller", thumbnail.MaxProjectThumbnailBytes), http.StatusRequestEntityTooLarge, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}

	_, contentType, err := handler.ThumbnailStore.Save(organization, project, data)
	if err != nil {
		response := httputil.NewError(
			"validation_failed",
			fmt.Sprintf(
				"failed to save project thumbnail: %s. Allowed formats: PNG or JPG. Allowed dimensions: %dpx to %dpx. Maximum size: %d bytes",
				err,
				thumbnail.MinProjectThumbnailPixels,
				thumbnail.MaxProjectThumbnailPixels,
				thumbnail.MaxProjectThumbnailBytes,
			),
			http.StatusBadRequest,
			map[string]any{"project_id": projectID},
			nil,
		)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true, "project_id": projectID, "content_type": contentType}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectThumbnailDELETE(ctx fiber.Ctx) error {
	organization, project, projectID, err := handler.resolveExistingProject(ctx)
	if err != nil {
		return core.WriteAppError(ctx, err, handler.Logger)
	}
	err = handler.ThumbnailStore.Delete(organization, project)
	if err != nil && !errors.Is(err, thumbnail.ErrNoThumbnail) {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to delete project thumbnail: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.Logger)
		return response.Write(ctx)
	}
	return httputil.JSON(map[string]any{"success": true, "project_id": projectID}, http.StatusOK).Write(ctx)
}
