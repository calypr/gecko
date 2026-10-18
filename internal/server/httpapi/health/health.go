package health

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
)

type Dependencies struct {
	DB     *sqlx.DB
	Logger arborist.Logger
}

type Handler struct {
	Dependencies
}

func NewHandler(deps Dependencies) *Handler {
	return &Handler{Dependencies: deps}
}

func Register(app *fiber.App, deps Dependencies) {
	handler := NewHandler(deps)
	app.Get("/health", handler.handleHealth)
}

func (handler *Handler) handleHealth(ctx fiber.Ctx) error {
	if handler.DB == nil {
		handler.Logger.Warning("Health check: Database connection not configured.")
		return httputil.NewError(apierror.TypeDatabaseUnavailable, "database unavailable", http.StatusServiceUnavailable, nil, nil).Write(ctx)
	}
	if err := handler.DB.Ping(); err != nil {
		handler.Logger.Error("Database ping failed")
		return httputil.NewError(apierror.TypeDatabaseUnavailable, "database unavailable", http.StatusServiceUnavailable, nil, nil).Write(ctx)
	}
	return httputil.JSON("Healthy", http.StatusOK).Write(ctx)
}
