package httpapi

import (
	"github.com/calypr/gecko/internal/access"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func registerConfigRoutes(app *fiber.App, sharedHandler *Handler, authzHandler access.ResourceAccessHandler) {
	handler := newConfigHandler(sharedHandler)
	if handler.DB == nil {
		handler.Logger.Warning("Skipping DB endpoints — no database configured")
		return
	}

	configGroup := app.Group("/config")
	configGroup.Get("/types", handler.handleConfigTypesGET)
	configGroup.Get("/list", handler.handleConfigListGET)

	handler.registerTypedConfigRoutes(configGroup.Group("/explorer", ConfigTypeMiddleware("explorer")), true, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/nav", ConfigTypeMiddleware("nav")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/file_summary", ConfigTypeMiddleware("file_summary")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/project", ConfigTypeMiddleware("project")), false, authzHandler)
	handler.registerProjectConfigRoutes(configGroup.Group("/projects", ConfigTypeMiddleware("projects")), authzHandler)
}

func (handler *ConfigHandler) registerTypedConfigRoutes(group fiber.Router, includeDefaultGet bool, authzHandler access.ResourceAccessHandler) {
	group.Get("/list", handler.handleConfigListGET)
	if includeDefaultGet {
		group.Get("/", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	}
	group.Get("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	group.Put("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigPUT)
	group.Delete("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigDELETE)
}

func (handler *ConfigHandler) registerProjectConfigRoutes(projects fiber.Router, authzHandler access.ResourceAccessHandler) {
	projects.Get("", handler.handleConfigListGET)
	projects.Get("/list", handler.handleConfigListGET)
	projects.Get("/summary", handler.handleProjectSummaryGET)
	projects.Delete("/:orgTitle", handler.handleProjectOrganizationDELETE)
	projects.Get("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "read"), handler.handleProjectConfigGET)
	projects.Put("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "update"), handler.handleProjectConfigPUT)
	projects.Delete("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "delete"), handler.handleProjectConfigDELETE)
}
