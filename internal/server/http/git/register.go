package git

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func RegisterRoutes(app *fiber.App, sharedHandler *shared.Handler, authzHandler servermw.ResourceAccessHandler) {
	handler := NewHandler(sharedHandler)
	if handler.GitService == nil {
		return
	}
	gitGroup := app.Group("/git")
	gitGroup.Get("/projects", servermw.RequireAuthorization(handler.Logger), handler.handleGitProjectsGET)
	gitGroup.Get("/organizations/status", servermw.RequireAuthorization(handler.Logger), handler.handleGitOrganizationsStatusGET)
	gitGroup.Post("/organizations/reconcile", servermw.RequireAuthorization(handler.Logger), handler.handleGitOrganizationsReconcilePOST)
	gitGroup.Post("/organizations/:orgTitle/init-connect", servermw.RequireAuthorization(handler.Logger), handler.handleGitOrganizationInitConnectPOST)
	gitGroup.Post("/organizations/:orgTitle/connect", servermw.RequireAuthorization(handler.Logger), handler.handleGitOrganizationConnectPOST)
	gitGroup.Get("/organizations/:orgTitle/status", servermw.GitOrganizationAuth(handler.Logger, authzHandler), handler.handleGitOrganizationStatusGET)
	gitGroup.Post("/organizations/:orgTitle/reconcile", servermw.GitOrganizationAuth(handler.Logger, authzHandler), handler.handleGitOrganizationReconcilePOST)

	projectReadAuth := servermw.GitProjectAuth(handler.Logger, authzHandler)
	gitGroup.Get("/projects/:orgTitle/:projectTitle", projectReadAuth, handler.handleGitProjectGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/refs", projectReadAuth, handler.handleGitProjectRefsGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree/*", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/file/*", projectReadAuth, handler.handleGitProjectFileGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/download/*", projectReadAuth, handler.handleGitProjectDownloadGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/thumbnail", handler.handleGitProjectThumbnailGET)

	projectGitWrite := gitGroup.Group("/projects/:orgTitle/:projectTitle", servermw.RequireAuthorization(handler.Logger))
	projectGitWrite.Put("/setup", handler.handleCalyprProjectSetupPUT)
	projectGitWrite.Put("/storage", handler.handleCalyprProjectStoragePUT)
	projectGitWrite.Put("/thumbnail", handler.handleGitProjectThumbnailPUT)
	projectGitWrite.Delete("/thumbnail", handler.handleGitProjectThumbnailDELETE)
	projectGitWrite.Post("/update", handler.handleGitProjectUpdatePOST)
	projectGitWrite.Post("/uploads/session", handler.handleGitProjectUploadSessionPOST)
	projectGitWrite.Get("/uploads/session/:sessionID", handler.handleGitProjectUploadSessionGET)
	projectGitWrite.Post("/uploads/session/:sessionID/files", handler.handleGitProjectUploadSessionFilesPOST)
	projectGitWrite.Post("/uploads/session/:sessionID/finalize", handler.handleGitProjectUploadSessionFinalizePOST)
}
