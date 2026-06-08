package directory

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

func RegisterRoutes(app *fiber.App, sharedHandler *shared.Handler, authzHandler servermw.ResourceAccessHandler) {
	handler := NewHandler(sharedHandler)
	if handler.GripqlClient == nil {
		handler.Logger.Warning("Skipping gripql Directory endpoints — no database configured")
		return
	}
	authMiddleware := servermw.GeneralAuth(handler.Logger, authzHandler, "read", "*")
	app.Get("/dir", handler.handleListProjects)
	app.Get("/dir/:projectId", authMiddleware, handler.handleDirGet)
}
