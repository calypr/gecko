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
	projectSetupAuth := servermw.GitProjectSetupAuth(handler.Logger, authzHandler)
	projectWriteAuth := servermw.GitProjectMutationAuth(handler.Logger, authzHandler, "update")
	projectConfigReadAuth := servermw.ProjectConfigAuth(handler.Logger, authzHandler, "read")
	projectConfigWriteAuth := servermw.ProjectConfigAuth(handler.Logger, authzHandler, "update")
	gitGroup.Get("/projects/:orgTitle/:projectTitle", projectReadAuth, handler.handleGitProjectGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/refs", projectReadAuth, handler.handleGitProjectRefsGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/tree/*", projectReadAuth, handler.handleGitProjectTreeGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/manifest", projectReadAuth, handler.handleGitProjectManifestGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/manifest/*", projectReadAuth, handler.handleGitProjectManifestGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/file/*", projectReadAuth, handler.handleGitProjectFileGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/download/*", projectReadAuth, handler.handleGitProjectDownloadGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/thumbnail", handler.handleGitProjectThumbnailGET)
	gitGroup.Get("/projects/:orgTitle/:projectTitle/presentationConfig", projectConfigReadAuth, handler.handleGitProjectPresentationConfigGET)

	projectGitWrite := gitGroup.Group("/projects/:orgTitle/:projectTitle", servermw.RequireAuthorization(handler.Logger))
	// Setup must stay auth-only so a brand-new organization can be bootstrapped
	// before any org/project Arborist resources exist for the caller.
	projectGitWrite.Put("/setup", handler.handleCalyprProjectSetupPUT)
	projectGitWrite.Put("/storage", projectSetupAuth, handler.handleCalyprProjectStoragePUT)
	projectGitWrite.Put("/thumbnail", projectWriteAuth, handler.handleGitProjectThumbnailPUT)
	projectGitWrite.Delete("/thumbnail", projectWriteAuth, handler.handleGitProjectThumbnailDELETE)
	projectGitWrite.Put("/presentationConfig", projectConfigWriteAuth, handler.handleGitProjectPresentationConfigPUT)
	projectGitWrite.Post("/presentationConfig", projectConfigWriteAuth, handler.handleGitProjectPresentationConfigPUT)
	projectGitWrite.Post("/edit-connect", projectWriteAuth, handler.handleGitProjectEditConnectPOST)
	projectGitWrite.Post("/update", projectWriteAuth, handler.handleGitProjectUpdatePOST)
	projectGitWrite.Post("/uploads/session", projectWriteAuth, handler.handleGitProjectUploadSessionPOST)
	projectGitWrite.Get("/uploads/session/:sessionID", projectWriteAuth, handler.handleGitProjectUploadSessionGET)
	projectGitWrite.Post("/uploads/session/:sessionID/files", projectWriteAuth, handler.handleGitProjectUploadSessionFilesPOST)
	projectGitWrite.Post("/uploads/session/:sessionID/finalize", projectWriteAuth, handler.handleGitProjectUploadSessionFinalizePOST)
}
