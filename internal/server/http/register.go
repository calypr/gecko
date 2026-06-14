package httpapi

import (
	"net/http"
	"strings"

	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/server/http/config"
	"github.com/calypr/gecko/internal/server/http/git"
	"github.com/calypr/gecko/internal/server/http/health"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/calypr/gecko/internal/server/http/vector"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
)

type Dependencies = shared.Dependencies

func Register(app *fiber.App, deps Dependencies) {
	handler := shared.NewHandler(deps)
	authzHandler := servermw.NewFenceUserAccessHandler(http.DefaultClient)

	app.Get("/swagger/doc.json", func(ctx fiber.Ctx) error {
		return ctx.SendFile("./docs/swagger.json")
	})

	health.RegisterRoutes(app, handler)
	config.RegisterRoutes(app, handler, authzHandler)
	git.RegisterRoutes(app, handler, authzHandler)
	vector.RegisterRoutes(app, handler)

	app.Use(func(ctx fiber.Ctx) error {
		ctx.Path(strings.TrimSuffix(ctx.Path(), "/"))
		return httputil.NotFound(ctx)
	})
}
