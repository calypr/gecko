package middleware

import (
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/uc-cdis/arborist/arborist"
)

func RequestLogger(logger arborist.Logger) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		start := time.Now()
		err := ctx.Next()
		latency := time.Since(start)

		routePattern := "<unmatched>"
		if route := ctx.Route(); route != nil && route.Path != "" {
			routePattern = route.Path
		}

		logger.Info(
			"%s %s - Status: %d - Latency: %s - Host: %s - IP: %s - Path: %s - Query: %s - Route: %s - Params: %v - RequestID: %s",
			ctx.Method(),
			ctx.OriginalURL(),
			ctx.Response().StatusCode(),
			latency,
			ctx.Hostname(),
			ctx.IP(),
			ctx.Path(),
			string(ctx.Request().URI().QueryString()),
			routePattern,
			routeParams(ctx),
			ctx.Get("X-Request-Id"),
		)
		return err
	}
}

func routeParams(ctx fiber.Ctx) map[string]string {
	params := make(map[string]string)
	for _, name := range []string{"configType", "configId", "orgTitle", "projectTitle", "projectId", "collection", "id"} {
		if value := ctx.Params(name); value != "" {
			params[name] = value
		}
	}
	if len(params) == 0 {
		return nil
	}
	return params
}
