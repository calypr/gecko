package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

// handleAppCardGET godoc
// @Summary Get an app card
// @Description Retrieve an apps-page card for a specific project.
// @Tags App Cards
// @Produce json
// @Param projectId path string true "Project ID"
// @Success 200 {object} config.AppCard "App card"
// @Failure 404 {object} ErrorResponse "App card not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [get]
func (handler *Handler) handleAppCardGET(ctx fiber.Ctx) error {
	configType := string(config.TypeAppsPage)
	configID := config.AppsPageConfigID
	projectID := ctx.Params("projectId")
	if projectID == "" {
		errResponse := httputil.NewError(apierror.TypeMissingProjectID, "Missing projectId parameter", http.StatusBadRequest, nil, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	var currentCfg config.AppsConfig
	err := geckodb.ConfigGETGeneric(handler.db, configID, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		errResponse := httputil.NewError(apierror.TypeAppCardNotFound, fmt.Sprintf("AppCard with projectId (perms) %s not found (no config exists)", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Failed to retrieve apps_page config: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	for _, card := range currentCfg.AppCards {
		if card.Perms == projectID {
			return httputil.JSON(card, http.StatusOK).Write(ctx)
		}
	}
	errResponse := httputil.NewError(apierror.TypeAppCardNotFound, fmt.Sprintf("AppCard with projectId (perms) %s not found", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
	errResponse.WriteLog(handler.logger)
	return errResponse.Write(ctx)
}

// handleAppCardPOST godoc
// @Summary Create or update an app card
// @Description Create or update an apps-page card for a specific project.
// @Tags App Cards
// @Accept json
// @Produce json
// @Param projectId path string true "Project ID"
// @Param body body config.AppCard true "App card payload"
// @Success 200 {object} map[string]interface{} "App card successfully upserted"
// @Failure 400 {object} ErrorResponse "Invalid request body"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [post]
func (handler *Handler) handleAppCardPOST(ctx fiber.Ctx) error {
	configType := string(config.TypeAppsPage)
	configID := config.AppsPageConfigID

	var currentCfg config.AppsConfig
	err := geckodb.ConfigGETGeneric(handler.db, configID, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		currentCfg = config.AppsConfig{AppCards: []config.AppCard{}}
	} else if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Failed to get apps_page config: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	projectID := ctx.Params("projectId")
	var newCard config.AppCard
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &newCard, map[string]any{"config_type": configType, "config_id": configID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	if newCard.Perms != projectID {
		errResponse := httputil.NewError(apierror.TypeProjectIDMismatch, fmt.Sprintf("Project ID in path (%s) does not match perms in body (%s)", projectID, newCard.Perms), http.StatusBadRequest, map[string]any{"path_project_id": projectID, "body_project_id": newCard.Perms}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	updated := false
	for i := range currentCfg.AppCards {
		if currentCfg.AppCards[i].Perms == newCard.Perms {
			currentCfg.AppCards[i] = newCard
			updated = true
			break
		}
	}
	if !updated {
		currentCfg.AppCards = append(currentCfg.AppCards, newCard)
	}
	if err := geckodb.ConfigPUTGeneric(handler.db, configID, configType, &currentCfg); err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Failed to update apps_page config: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("AppCard with perms %s added or updated", newCard.Perms)}, http.StatusOK).Write(ctx)
}

// handleAppCardDELETE godoc
// @Summary Delete an app card
// @Description Delete an apps-page card for a specific project.
// @Tags App Cards
// @Produce json
// @Param projectId path string true "Project ID"
// @Success 200 {object} map[string]interface{} "App card deleted"
// @Failure 404 {object} ErrorResponse "App card not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [delete]
func (handler *Handler) handleAppCardDELETE(ctx fiber.Ctx) error {
	configType := string(config.TypeAppsPage)
	configID := config.AppsPageConfigID
	projectID := ctx.Params("projectId")

	var currentCfg config.AppsConfig
	err := geckodb.ConfigGETGeneric(handler.db, configID, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		errResponse := httputil.NewError(apierror.TypeAppCardNotFound, "No apps_page config found", http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	} else if err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Failed to get apps_page config: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	newCards := []config.AppCard{}
	found := false
	for _, card := range currentCfg.AppCards {
		if card.Perms == projectID {
			found = true
			continue
		}
		newCards = append(newCards, card)
	}
	if !found {
		errResponse := httputil.NewError(apierror.TypeAppCardNotFound, fmt.Sprintf("AppCard with projectId (perms) %s not found", projectID), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	currentCfg.AppCards = newCards
	if err := geckodb.ConfigPUTGeneric(handler.db, configID, configType, &currentCfg); err != nil {
		errResponse := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("Failed to update apps_page config: %s", err), http.StatusInternalServerError, map[string]any{"config_type": configType, "config_id": configID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	return httputil.JSON(map[string]any{"code": http.StatusOK, "message": fmt.Sprintf("AppCard with perms %s deleted", projectID)}, http.StatusOK).Write(ctx)
}
