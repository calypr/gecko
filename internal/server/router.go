package server

import (
	"net/http"
	"time"

	"github.com/calypr/gecko/internal/server/httpapi/apiserver"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
)

func (server *Server) MakeRouter() *fiber.App {
	app := fiber.New(fiber.Config{
		BodyLimit:      50 * 1024 * 1024,
		ReadBufferSize: 32 * 1024,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	})

	app.Use(func(ctx fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				server.Logger.Error("panic recovered: %v", r)
				ctx.Status(http.StatusInternalServerError)
				_ = ctx.SendString("Internal Server Error")
			}
		}()
		return ctx.Next()
	})
	app.Use(servermw.RequestLogger(server.Logger))

	// The generated strict OpenAPI router is the single source for the HTTP
	// surface: it routes every config/git/health operation, enforces
	// authorization, and serves the spec at /swagger/doc.json.
	app.Use(adaptor.HTTPHandler(apiserver.NewRouter(server.newAPIServer())))
	return app
}
