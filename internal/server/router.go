package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/calypr/gecko/internal/server/httpapi/apiserver"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/calypr/gecko/internal/storageaudit"
)

func (server *Server) MakeRouter() (http.Handler, error) {
	// The generated strict OpenAPI router is the single source for the HTTP
	// surface. It already uses net/http, so serving it directly preserves the
	// native request context for application and downstream calls.
	apiServer := server.apiServer
	if apiServer == nil {
		var err error
		apiServer, err = server.newAPIServer()
		if err != nil {
			return nil, fmt.Errorf("initialize API routes: %w", err)
		}
	}

	var handler http.Handler = apiserver.NewRouter(apiServer)
	handler = servermw.LimitRequestBody(servermw.MaxRequestBodyBytes, handler)
	handler = servermw.Recover(server.Logger, handler)
	handler = servermw.RequestLogger(server.Logger, handler)
	return handler, nil
}

// MakeHTTPServer builds the production listener and ties incoming request
// contexts to the service lifetime.
func (server *Server) MakeHTTPServer(addr string, serviceContext context.Context) (*http.Server, error) {
	handler, err := server.MakeRouter()
	if err != nil {
		return nil, err
	}
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 30 * time.Second,
		// Leave one minute beyond the bounded audit work for response delivery.
		WriteTimeout:   storageaudit.StorageChainAuditWorkBudget + time.Minute,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 32 * 1024,
		BaseContext: func(net.Listener) context.Context {
			return serviceContext
		},
	}, nil
}
