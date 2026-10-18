package apiserver

import (
	"context"
	"net/http"

	"github.com/calypr/gecko/apgen/server/gecko"
)

// HealthCheck implements GET /health.
func (s *Server) HealthCheck(ctx context.Context, request gecko.HealthCheckRequestObject) (gecko.HealthCheckResponseObject, error) {
	if s.DB == nil {
		return nil, newOpError(http.StatusServiceUnavailable, "database_unavailable", "database unavailable", nil)
	}
	if err := s.DB.PingContext(ctx); err != nil {
		return nil, newOpError(http.StatusServiceUnavailable, "database_unavailable", "database unavailable", nil)
	}
	return gecko.HealthCheck200JSONResponse("Healthy"), nil
}
