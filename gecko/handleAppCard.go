package gecko

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/calypr/gecko/gecko/config"
	"github.com/kataras/iris/v12"
)

// handleAppCardGET godoc
// @Summary Get a specific AppCard by projectId (perms)
// @Description Retrieves a single AppCard from the apps_page configuration by its perms value (used as projectId).
// @Tags Config
// @Produce json
// @Param projectId path string true "Project ID (AppCard perms value, e.g., HTAN_INT-BForePC)"
// @Success 200 {object} config.AppCard "The requested AppCard"
// @Failure 404 {object} ErrorResponse "AppCard not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [get]
func (server *Server) handleAppCardGET(ctx iris.Context) {
	configType := "apps_page"
	configId := "1" // Matches the ID used in helm chart bootstrap

	projectId := ctx.Params().Get("projectId")
	if projectId == "" {
		errResponse := newErrorResponse("Missing projectId parameter", 400, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	var currentCfg config.AppsConfig
	err := configGETGeneric(server.db, configId, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		// No config exists yet → no AppCards
		msg := fmt.Sprintf("AppCard with projectId (perms) %s not found (no config exists)", projectId)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}
	if err != nil {
		msg := fmt.Sprintf("Failed to retrieve apps_page config: %s", err)
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	// Find the matching AppCard by Perms
	for _, card := range currentCfg.AppCards {
		if card.Perms == projectId {
			jsonResponseFrom(card, http.StatusOK).write(ctx)
			return
		}
	}

	// Not found
	msg := fmt.Sprintf("AppCard with projectId (perms) %s not found", projectId)
	errResponse := newErrorResponse(msg, 404, nil)
	errResponse.log.write(server.Logger)
	_ = errResponse.write(ctx)
}

// handleAppCardPOST godoc
// @Summary Add or update an AppCard
// @Description Adds a new AppCard to the apps_page configuration or updates an existing one if the perms matches. Assumes a fixed configId "default" for apps_page.
// @Tags Config
// @Accept json
// @Produce json
// @Param projectId path string true "Project ID (AppCard perms value, e.g., HTAN_INT-BForePC)"
// @Param body body config.AppCard true "AppCard details"
// @Success 200 {object} map[string]interface{} "AppCard added or updated"
// @Failure 400 {object} ErrorResponse "Invalid request body or ID mismatch"
// @Failure 404 {object} ErrorResponse "Config not found (if required)"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [post]
func (server *Server) handleAppCardPOST(ctx iris.Context) {
	configType := "apps_page"
	configId := "1" // Matches the ID used in helm chart bootstrap

	var currentCfg config.AppsConfig
	err := configGETGeneric(server.db, configId, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		// Initialize empty if not found
		currentCfg = config.AppsConfig{AppCards: []config.AppCard{}}
	} else if err != nil {
		msg := fmt.Sprintf("Failed to get apps_page config: %s", err)
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	projectId := ctx.Params().Get("projectId")
	var newCard config.AppCard
	if err := ctx.ReadJSON(&newCard); err != nil {
		msg := "Invalid JSON format"
		errResponse := newErrorResponse(msg, 400, &err)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	if newCard.Perms != projectId {
		msg := fmt.Sprintf("Project ID in path (%s) does not match perms in body (%s)", projectId, newCard.Perms)
		errResponse := newErrorResponse(msg, 400, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	// Update if Perms exists, else append
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

	// Save the updated config
	err = configPUTGeneric(server.db, configId, configType, &currentCfg)
	if err != nil {
		msg := fmt.Sprintf("Failed to update apps_page config: %s", err)
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponseFrom(
		map[string]any{
			"code":    200,
			"message": fmt.Sprintf("AppCard with perms %s added or updated", newCard.Perms),
		},
		http.StatusOK,
	).write(ctx)
}

// handleAppCardDELETE godoc
// @Summary Delete an AppCard
// @Description Deletes an AppCard from the apps_page configuration by projectId (perms). Assumes a fixed configId "default" for apps_page.
// @Tags Config
// @Produce json
// @Param projectId path string true "Project ID (AppCard perms value, e.g., HTAN_INT-BForePC)"
// @Success 200 {object} map[string]interface{} "AppCard deleted"
// @Failure 404 {object} ErrorResponse "AppCard or config not found"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /config/apps_page/appcard/{projectId} [delete]
func (server *Server) handleAppCardDELETE(ctx iris.Context) {
	configType := "apps_page"
	configId := "1" // Matches the ID used in helm chart bootstrap
	projectId := ctx.Params().Get("projectId")

	var currentCfg config.AppsConfig
	err := configGETGeneric(server.db, configId, configType, &currentCfg)
	if errors.Is(err, sql.ErrNoRows) {
		msg := "No apps_page config found"
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	} else if err != nil {
		msg := fmt.Sprintf("Failed to get apps_page config: %s", err)
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	// Remove the matching AppCard by Perms
	newCards := []config.AppCard{}
	found := false
	for _, card := range currentCfg.AppCards {
		if card.Perms == projectId {
			found = true
			continue
		}
		newCards = append(newCards, card)
	}
	if !found {
		msg := fmt.Sprintf("AppCard with projectId (perms) %s not found", projectId)
		errResponse := newErrorResponse(msg, 404, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}
	currentCfg.AppCards = newCards

	// Save the updated config
	err = configPUTGeneric(server.db, configId, configType, &currentCfg)
	if err != nil {
		msg := fmt.Sprintf("Failed to update apps_page config: %s", err)
		errResponse := newErrorResponse(msg, 500, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	jsonResponseFrom(
		map[string]any{
			"code":    200,
			"message": fmt.Sprintf("AppCard with perms %s deleted", projectId),
		},
		http.StatusOK,
	).write(ctx)
}
