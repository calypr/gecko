package api

import (
	"net/http"
	"strings"

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
	authzHandler := servermw.NewFenceUserAccessHandler(http.DefaultClient)
	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		return ctx.SendFile("./docs/swagger.json")
	})
	app.Get("/health", handler.handleHealth)

	handler.registerDirectoryRoutes(app, authzHandler)
	handler.registerConfigRoutes(app, authzHandler)
	handler.registerVectorRoutes(app)

	if handler.gripqlClient == nil || handler.gripGraphName == "" {
		handler.logger.Warning("Skipping Grip endpoints — no graph configured")
	}

	app.Use(func(ctx fiber.Ctx) error {
		ctx.Path(strings.TrimSuffix(ctx.Path(), "/"))
		return httputil.NotFound(ctx)
	})
}

func (handler *Handler) registerDirectoryRoutes(app *fiber.App, authzHandler servermw.ResourceAccessHandler) {
	if handler.gripqlClient == nil {
		handler.logger.Warning("Skipping gripql Directory endpoints — no database configured")
		return
	}
	handler.registerDirectoryHandlers(app, servermw.GeneralAuth(handler.logger, authzHandler, "read", "*"))
}

func (handler *Handler) registerConfigRoutes(app *fiber.App, authzHandler servermw.ResourceAccessHandler) {
	if handler.db == nil {
		handler.logger.Warning("Skipping DB endpoints — no database configured")
		return
	}

	configGroup := app.Group("/config")
	configGroup.Get("/types", handler.handleConfigTypesGET)
	configGroup.Get("/list", handler.handleConfigListGET)

	handler.registerTypedConfigRoutes(configGroup.Group("/explorer", withConfigType("explorer")), true, authzHandler)
	handler.registerAppsPageRoutes(configGroup.Group("/apps_page", withConfigType("apps_page")), authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/nav", withConfigType("nav")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/file_summary", withConfigType("file_summary")), false, authzHandler)
	handler.registerTypedConfigRoutes(configGroup.Group("/project", withConfigType("project")), false, authzHandler)
	handler.registerProjectConfigRoutes(configGroup.Group("/projects", withConfigType("projects")), authzHandler)
	handler.registerGitRoutes(app, authzHandler)
}

func (handler *Handler) registerTypedConfigRoutes(group fiber.Router, includeDefaultGet bool, authzHandler servermw.ResourceAccessHandler) {
	group.Get("/list", handler.handleConfigListGET)
	if includeDefaultGet {
		group.Get("/", servermw.ConfigAuth(handler.logger, authzHandler), handler.handleConfigGET)
	}
	group.Get("/:configId", servermw.ConfigAuth(handler.logger, authzHandler), handler.handleConfigGET)
	group.Put("/:configId", servermw.ConfigAuth(handler.logger, authzHandler), handler.handleConfigPUT)
	group.Delete("/:configId", servermw.ConfigAuth(handler.logger, authzHandler), handler.handleConfigDELETE)
}

func (handler *Handler) registerAppsPageRoutes(appsPage fiber.Router, authzHandler servermw.ResourceAccessHandler) {
	handler.registerTypedConfigRoutes(appsPage, true, authzHandler)
	appCard := appsPage.Group("/appcard")
	appCard.Get("/:projectId", servermw.AppCardAuth(handler.logger, authzHandler), handler.handleAppCardGET)
	appCard.Post("/:projectId", servermw.AppCardAuth(handler.logger, authzHandler), handler.handleAppCardPOST)
	appCard.Delete("/:projectId", servermw.AppCardAuth(handler.logger, authzHandler), handler.handleAppCardDELETE)
}

func (handler *Handler) registerProjectConfigRoutes(projects fiber.Router, authzHandler servermw.ResourceAccessHandler) {
	projects.Get("", handler.handleConfigListGET)
	projects.Get("/list", handler.handleConfigListGET)
	projects.Delete("/:orgTitle", handler.handleProjectOrganizationDELETE)
	projects.Get("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.logger, authzHandler, "read"), handler.handleProjectConfigGET)
	projects.Put("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.logger, authzHandler, "update"), handler.handleProjectConfigPUT)
	projects.Delete("/:orgTitle/:projectTitle", servermw.ProjectConfigAuth(handler.logger, authzHandler, "delete"), handler.handleProjectConfigDELETE)
}

func (handler *Handler) registerGitRoutes(app *fiber.App, authzHandler servermw.ResourceAccessHandler) {
	if handler.gitService == nil {
		return
	}
	gitGroup := app.Group("/git")
	gitGroup.Post("/github/webhook", handler.handleGitHubWebhookPOST)
	gitGroup.Get("/pending", servermw.RequireAuthorization(handler.logger), handler.handleGitPendingRepositoriesGET)
	gitGroup.Post("/pending/reconcile", servermw.RequireAuthorization(handler.logger), handler.handleGitPendingRepositoriesReconcilePOST)
	gitGroup.Get("/projects", servermw.RequireAuthorization(handler.logger), handler.handleGitProjectsGET)
	gitGroup.Get("/organizations/status", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationsStatusGET)
	gitGroup.Post("/organizations/reconcile", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationsReconcilePOST)
	gitGroup.Post("/organizations/:orgTitle/connect", servermw.RequireAuthorization(handler.logger), handler.handleGitOrganizationConnectPOST)
	gitGroup.Get("/organizations/:orgTitle/status", servermw.GitOrganizationAuth(handler.logger, authzHandler), handler.handleGitOrganizationStatusGET)
	gitGroup.Post("/organizations/:orgTitle/reconcile", servermw.GitOrganizationAuth(handler.logger, authzHandler), handler.handleGitOrganizationReconcilePOST)

	projectReadAuth := servermw.GitProjectAuth(handler.logger, authzHandler)
	gitGroup.Get("/projects/:orgTitle/:projectTitle", projectReadAuth, handler.handleGitProjectGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/refs", projectReadAuth, handler.handleGitProjectRefsGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree/*", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/file/*", projectReadAuth, handler.handleGitProjectFileGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/download/*", projectReadAuth, handler.handleGitProjectDownloadGET)

	projectGitWrite := gitGroup.Group("/projects/:orgTitle/:projectTitle", servermw.RequireAuthorization(handler.logger))
	projectGitWrite.Put("/setup", handler.handleCalyprProjectSetupPUT)
	projectGitWrite.Post("/update", handler.handleGitProjectUpdatePOST)
	projectGitWrite.Post("/uploads/session", handler.handleGitProjectUploadSessionPOST)
	projectGitWrite.Get("/uploads/session/:sessionID", handler.handleGitProjectUploadSessionGET)
	projectGitWrite.Post("/uploads/session/:sessionID/files", handler.handleGitProjectUploadSessionFilesPOST)
	projectGitWrite.Post("/uploads/session/:sessionID/finalize", handler.handleGitProjectUploadSessionFinalizePOST)
}

func (handler *Handler) registerVectorRoutes(app *fiber.App) {
	if handler.qdrantClient == nil {
		handler.logger.Warning("Skipping Qdrant endpoints — no vector store configured")
		return
	}
	handler.registerVectorHandlers(app)
}
