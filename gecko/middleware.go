package gecko

import (
	"time"

	"github.com/kataras/iris/v12"
)

func (server *Server) logRequestMiddleware(ctx iris.Context) {
	start := time.Now()
	ctx.Next()
	latency := time.Since(start)
	method := ctx.Request().Method
	path := ctx.Request().URL.Path
	status := ctx.ResponseWriter().StatusCode()

	server.logger.Info("%s %s - Status: %d - Latency: %s", method, path, status, latency)
}
