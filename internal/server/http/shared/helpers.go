package shared

import "github.com/gofiber/fiber/v3"

func ConfigTypeMiddleware(configType string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Locals("configType", configType)
		return ctx.Next()
	}
}
