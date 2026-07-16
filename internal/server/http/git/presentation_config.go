package git

import (
	"errors"
	"fmt"
	"net/http"

	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleGitProjectPresentationConfigGET(ctx fiber.Ctx) error {
	organization, project, projectID, errResponse := handler.resolveExistingProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	if handler.presentationStore == nil {
		errResponse = httputil.NewError("internal_error", "presentation storage is not configured", http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	content, err := handler.presentationStore.Get(organization, project)
	if err != nil {
		if errors.Is(err, presentation.ErrNoPresentation) {
			return httputil.JSON(appconfig.PresentationConfig{}, http.StatusOK).Write(ctx)
		}
		errResponse = httputil.NewError("internal_error", fmt.Sprintf("failed to load project presentation config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(appconfig.PresentationConfig{PresentationConfig: content}, http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectPresentationConfigPUT(ctx fiber.Ctx) error {
	organization, project, projectID, errResponse := handler.resolveExistingProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	if handler.presentationStore == nil {
		errResponse = httputil.NewError("internal_error", "presentation storage is not configured", http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	cfg := &appconfig.PresentationConfig{}
	if errResponse = httputil.ParseJSONBody(ctx.Body(), cfg, map[string]any{"project_id": projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if err := cfg.Validate(); err != nil {
		errResponse = httputil.NewError("validation_failed", fmt.Sprintf("body data validation failed: %s", err), http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if err := handler.presentationStore.Save(organization, project, cfg.PresentationConfig); err != nil {
		errResponse = httputil.NewError("internal_error", fmt.Sprintf("failed to persist project presentation config: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(cfg, http.StatusOK).Write(ctx)
}
