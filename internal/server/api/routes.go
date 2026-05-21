package api

import (
	"strings"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/internal/httputil"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func withConfigType(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}

func (handler *Handler) registerRoutes(app *fiber.App) {
	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		return ctx.SendFile("./docs/swagger.json")
	})
	app.Get("/health", handler.handleHealth)

	handler.registerDirectoryRoutes(app)
	handler.registerConfigRoutes(app)
	handler.registerVectorRoutes(app)

	if handler.gripqlClient == nil || handler.gripGraphName == "" {
		handler.logger.Warning("Skipping Grip endpoints — no graph configured")
	}

	app.Use(func(ctx fiber.Ctx) error {
		ctx.Path(strings.TrimSuffix(ctx.Path(), "/"))
		return httputil.NotFound(ctx)
	})
}

func (handler *Handler) registerDirectoryRoutes(app *fiber.App) {
	if handler.gripqlClient == nil {
		handler.logger.Warning("Skipping gripql Directory endpoints — no database configured")
		return
	}
	handler.registerDirectoryHandlers(app, servermw.GeneralAuth(handler.logger, &middleware.ProdJWTHandler{}, "read", "*"))
}

func (handler *Handler) registerConfigRoutes(app *fiber.App) {
	if handler.db == nil {
		handler.logger.Warning("Skipping DB endpoints — no database configured")
		return
	}

	configGroup := app.Group("/config")
	configGroup.Get("/types", handler.handleConfigTypesGET)
	configGroup.Get("/list", handler.handleConfigListGET)

	handler.registerTypedConfigRoutes(configGroup.Group("/explorer", withConfigType("explorer")), true)
	handler.registerAppsPageRoutes(configGroup.Group("/apps_page", withConfigType("apps_page")))
	handler.registerTypedConfigRoutes(configGroup.Group("/nav", withConfigType("nav")), false)
	handler.registerTypedConfigRoutes(configGroup.Group("/file_summary", withConfigType("file_summary")), false)
	handler.registerTypedConfigRoutes(configGroup.Group("/project", withConfigType("project")), false)
	handler.registerProjectConfigRoutes(configGroup.Group("/projects", withConfigType("projects")))
	handler.registerGitRoutes(app)
}

func (handler *Handler) registerTypedConfigRoutes(group fiber.Router, includeDefaultGet bool) {
	group.Get("/list", handler.handleConfigListGET)
	if includeDefaultGet {
		group.Get("/", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleConfigGET)
	}
	group.Get("/:configId", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleConfigGET)
	group.Put("/:configId", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleConfigPUT)
	group.Delete("/:configId", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleConfigDELETE)
}

func (handler *Handler) registerAppsPageRoutes(appsPage fiber.Router) {
	handler.registerTypedConfigRoutes(appsPage, true)
	appCard := appsPage.Group("/appcard")
	appCard.Get("/:projectId", servermw.AppCardAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleAppCardGET)
	appCard.Post("/:projectId", servermw.AppCardAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleAppCardPOST)
	appCard.Delete("/:projectId", servermw.AppCardAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleAppCardDELETE)
}

func (handler *Handler) registerProjectConfigRoutes(projects fiber.Router) {
	projects.Get("", handler.handleConfigListGET)
	projects.Get("/list", handler.handleConfigListGET)
	projects.Get("/:orgTitle/:projectTitle", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleProjectConfigGET)
	projects.Put("/:orgTitle/:projectTitle", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleProjectConfigPUT)
	projects.Delete("/:orgTitle/:projectTitle", servermw.ConfigAuth(handler.logger, &middleware.ProdJWTHandler{}), handler.handleProjectConfigDELETE)
}

func (handler *Handler) registerGitRoutes(app *fiber.App) {
	if handler.gitService == nil {
		return
	}
	gitGroup := app.Group("/git")
	gitGroup.Get("/projects", handler.handleGitProjectsGET)
	gitGroup.Get("/organizations/status", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationsStatusGET)
	gitGroup.Post("/organizations/reconcile", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationsReconcilePOST)
	gitGroup.Post("/organizations/:orgTitle/connect", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationConnectPOST)
	gitGroup.Get("/organizations/:orgTitle/status", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationStatusGET)
	gitGroup.Post("/organizations/:orgTitle/reconcile", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationReconcilePOST)

	projectGitRead := gitGroup.Group("/projects/:orgTitle/:projectTitle", servermw.GitProjectAuth(handler.logger, &middleware.ProdJWTHandler{}))
	projectGitRead.Get("", handler.handleGitProjectGET)
	projectGitRead.Get("/refs", handler.handleGitProjectRefsGET)
	projectGitRead.Get("/tree", handler.handleGitProjectTreeGET)
	projectGitRead.Get("/tree/*", handler.handleGitProjectTreeGET)
	projectGitRead.Get("/file/*", handler.handleGitProjectFileGET)

	projectGitWrite := gitGroup.Group("/projects/:orgTitle/:projectTitle", servermw.RequireAuthorization(handler.logger))
	projectGitWrite.Post("/update", handler.handleGitProjectUpdatePOST)
}

func (handler *Handler) registerVectorRoutes(app *fiber.App) {
	if handler.qdrantClient == nil {
		handler.logger.Warning("Skipping Qdrant endpoints — no vector store configured")
		return
	}
	handler.registerVectorHandlers(app)
}
