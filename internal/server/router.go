package server

import (
	"net/http"
	"time"

	httpapi "github.com/calypr/gecko/internal/server/httpapi"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
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

	httpapi.Register(app, httpapi.Dependencies{
		DB:                server.db,
		Logger:            server.Logger,
		RepoService:       server.repoService,
		ThumbnailStore:    server.thumbnailStore,
		PresentationStore: server.presentationStore,
	})
	return app
}
