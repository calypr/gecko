package vector

import (
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/gofiber/fiber/v3"
)

func RegisterRoutes(app *fiber.App, sharedHandler *shared.Handler) {
	handler := NewHandler(sharedHandler)
	if handler.QdrantClient == nil {
		handler.Logger.Warning("Skipping Qdrant endpoints — no vector store configured")
		return
	}
	handler.registerVectorHandlers(app)
}
