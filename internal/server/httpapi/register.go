package httpapi

import (
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/httputil"
	configapi "github.com/calypr/gecko/internal/server/httpapi/config"
	gitapi "github.com/calypr/gecko/internal/server/httpapi/git"
	healthapi "github.com/calypr/gecko/internal/server/httpapi/health"
	"github.com/gofiber/fiber/v3"
)

type Dependencies struct {
	Config configapi.Dependencies
	Git    gitapi.Dependencies
	Health healthapi.Dependencies
}

func Register(app *fiber.App, deps Dependencies, authzHandler access.ResourceAccessHandler) {
	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		// Intentionally public: the OpenAPI spec contains no secrets and is
		// meant to be browsed by API consumers (e.g. Swagger UI).
		return ctx.SendFile("./docs/swagger.json")
	})

	healthapi.Register(app, deps.Health)
	configapi.Register(app, deps.Config, authzHandler)
	gitapi.Register(app, deps.Git, authzHandler)

	// Terminal fallback: routes have already run, so the request is a 404.
	// Do not mutate ctx.Path() here — earlier routers have matched and
	// rewriting the path would only confuse logs and downstream hooks.
	app.Use(httputil.NotFound)
}
