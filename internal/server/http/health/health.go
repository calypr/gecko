package health

import (
	"net/http"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) handleHealth(ctx fiber.Ctx) error {
	if handler.db != nil {
		if err := handler.db.Ping(); err != nil {
			handler.logger.Error("Database ping failed: %v", err)
			return httputil.NewError(apierror.TypeDatabaseUnavailable, "database unavailable", http.StatusInternalServerError, nil, nil).Write(ctx)
		}
	} else {
		handler.logger.Warning("Health check: Database connection not configured.")
	}
	return httputil.JSON("Healthy", http.StatusOK).Write(ctx)
}
