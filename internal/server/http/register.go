package httpapi

import (
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/server/http/config"
	"github.com/calypr/gecko/internal/server/http/git"
	"github.com/calypr/gecko/internal/server/http/health"
	"github.com/calypr/gecko/internal/server/http/shared"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

type Dependencies = shared.Dependencies

func Register(app *fiber.App, deps Dependencies) {
	handler := shared.NewHandler(deps)
	authzHandler := servermw.NewFenceUserAccessHandler(nil)

	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		// Intentionally public: the OpenAPI spec contains no secrets and is
		// meant to be browsed by API consumers (e.g. Swagger UI).
		return ctx.SendFile("./docs/swagger.json")
	})

	health.RegisterRoutes(app, handler)
	config.RegisterRoutes(app, handler, authzHandler)
	git.RegisterRoutes(app, handler, authzHandler)

	app.Use(func(ctx fiber.Ctx) error {
		// Terminal fallback: routes have already run, so the request is a 404.
		// Do not mutate ctx.Path() here — earlier routers have matched and
		// rewriting the path would only confuse logs and downstream hooks.
		return httputil.NotFound(ctx)
	})
}
