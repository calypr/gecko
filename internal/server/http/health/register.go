package health

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/gofiber/fiber/v3"
)

func RegisterRoutes(app *fiber.App, sharedHandler *shared.Handler) {
	handler := NewHandler(sharedHandler)
	app.Get("/health", handler.handleHealth)
}
