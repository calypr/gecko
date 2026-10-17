package httpapi

import (
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func Register(app *fiber.App, deps Dependencies) {
	handler := NewHandler(deps)
	authzHandler := access.NewFenceUserAccessHandler(nil)

	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		// Intentionally public: the OpenAPI spec contains no secrets and is
		// meant to be browsed by API consumers (e.g. Swagger UI).
		return ctx.SendFile("./docs/swagger.json")
	})

	registerHealthRoutes(app, handler)
	registerConfigRoutes(app, handler, authzHandler)
	registerGitRoutes(app, handler, authzHandler)

	app.Use(func(ctx fiber.Ctx) error {
		// Terminal fallback: routes have already run, so the request is a 404.
		// Do not mutate ctx.Path() here — earlier routers have matched and
		// rewriting the path would only confuse logs and downstream hooks.
		return httputil.NotFound(ctx)
	})
}
