package config

import (
	"github.com/calypr/gecko/internal/access"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func configTypeMiddleware(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}

func Register(app *fiber.App, deps Dependencies, authzHandler access.ResourceAccessHandler) {
	handler := NewHandler(deps)
	if handler.DB == nil {
		handler.Logger.Warning("Skipping DB endpoints — no database configured")
		return
	}

	configGroup := app.Group("/config")
	configGroup.Get("/types", handler.handleConfigTypesGET)
	configGroup.Get("/list", handler.handleConfigListGET)

	handler.registerTypedConfigRoutes(configGroup.Group("/explorer", configTypeMiddleware("explorer")), true, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/nav", configTypeMiddleware("nav")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/file_summary", configTypeMiddleware("file_summary")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/project", configTypeMiddleware("project")), false, authzHandler)
	handler.registerProjectConfigRoutes(configGroup.Group("/projects", configTypeMiddleware("projects")), authzHandler)
}

func (handler *Handler) registerTypedConfigRoutes(group fiber.Router, includeDefaultGet bool, authzHandler access.ResourceAccessHandler) {
	group.Get("/list", handler.handleConfigListGET)
	if includeDefaultGet {
		group.Get("/", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	}
	group.Get("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigGET)
	group.Put("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigPUT)
	group.Delete("/:configId", servermw.ConfigAuth(handler.Logger, authzHandler), handler.handleConfigDELETE)
}

func (handler *Handler) registerProjectConfigRoutes(projects fiber.Router, authzHandler access.ResourceAccessHandler) {
	projects.Get("", handler.handleConfigListGET)
	projects.Get("/list", handler.handleConfigListGET)
	projects.Get("/summary", handler.handleProjectSummaryGET)
	projects.Delete("/:orgTitle", handler.handleProjectOrganizationDELETE)
	projects.Get("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "read"), handler.handleProjectConfigGET)
	projects.Put("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "update"), handler.handleProjectConfigPUT)
	projects.Delete("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.Logger, authzHandler, "delete"), handler.handleProjectConfigDELETE)
}
