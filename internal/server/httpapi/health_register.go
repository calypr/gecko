package httpapi

import (
	"github.com/gofiber/fiber/v3"
)

func registerHealthRoutes(app *fiber.App, sharedHandler *Handler) {
	handler := newHealthHandler(sharedHandler)
	app.Get("/health", handler.handleHealth)
}
