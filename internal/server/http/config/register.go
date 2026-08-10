package config

import (
	"context"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/server/http/shared"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func RegisterRoutes(app *fiber.App, sharedHandler *shared.Handler, authzHandler servermw.ResourceAccessHandler) {
	handler := NewHandler(sharedHandler)
	if handler.DB == nil {
		handler.Logger.Warning("Skipping DB endpoints — no database configured")
		return
	}
	if err := geckodb.EnsureExplorerRevisionTable(context.Background(), handler.DB); err != nil {
		handler.Logger.Warning("Explorer revision endpoints may be unavailable: %v", err)
	}

	configGroup := app.Group("/config")
	configGroup.Get("/types", handler.handleConfigTypesGET)
	configGroup.Get("/list", handler.handleConfigListGET)

	handler.registerTypedConfigRoutes(configGroup.Group("/explorer", shared.ConfigTypeMiddleware("explorer")), true, authzHandler)
	handler.registerExplorerRevisionRoutes(configGroup.Group("/explorer", shared.ConfigTypeMiddleware("explorer")), authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/nav", shared.ConfigTypeMiddleware("nav")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/file_summary", shared.ConfigTypeMiddleware("file_summary")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/project", shared.ConfigTypeMiddleware("project")), false, authzHandler)
	handler.registerProjectConfigRoutes(configGroup.Group("/projects", shared.ConfigTypeMiddleware("projects")), authzHandler)
}

func (handler *Handler) registerTypedConfigRoutes(group fiber.Router, includeDefaultGet bool, authzHandler servermw.ResourceAccessHandler) {
	group.Get("/list", handler.handleConfigListGET)
	if includeDefaultGet {
		group.Get("/", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	}
	group.Get("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	group.Put("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigPUT)
	group.Delete("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigDELETE)
}

func (handler *Handler) registerProjectConfigRoutes(projects fiber.Router, authzHandler servermw.ResourceAccessHandler) {
	projects.Get("", handler.handleConfigListGET)
	projects.Get("/list", handler.handleConfigListGET)
	projects.Get("/summary", handler.handleProjectSummaryGET)
	projects.Delete("/:orgTitle", handler.handleProjectOrganizationDELETE)
	projects.Get("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "read"), handler.handleProjectConfigGET)
	projects.Put("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "update"), handler.handleProjectConfigPUT)
	projects.Delete("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "delete"), handler.handleProjectConfigDELETE)
}
