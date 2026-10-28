package server

import (
	"context"
	"errors"
	"fmt"
	"log"

	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/server/httpapi/apiserver"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
)

type Server struct {
	db                *sqlx.DB
	Logger            *geckologging.Handler
	repoService       *repo.Service
	thumbnailStore    thumbnail.Manager
	presentationStore presentation.Manager
	apiServer         *apiserver.Server
}

func NewServer() *Server { return &Server{} }

func (server *Server) WithLogger(logger *log.Logger) *Server {
	server.Logger = &geckologging.Handler{Logger: logger}
	return server
}

func (server *Server) WithDB(db *sqlx.DB) *Server {
	server.db = db
	return server
}

func (server *Server) WithRepoService(service *repo.Service) *Server {
	server.repoService = service
	return server
}

func (server *Server) WithThumbnailStore(store thumbnail.Manager) *Server {
	server.thumbnailStore = store
	return server
}

func (server *Server) WithPresentationStore(store presentation.Manager) *Server {
	server.presentationStore = store
	return server
}

func (server *Server) Init() (*Server, error) {
	if server.Logger == nil {
		return nil, errors.New("gecko server initialized without logger")
	}
	if server.db == nil {
		server.Logger.Warning("Database endpoints will be disabled.")
	}
	if server.repoService != nil {
		if err := server.repoService.Init(server.db); err != nil {
			return nil, err
		}
	} else {
		server.Logger.Warning("Git endpoints will be disabled.")
	}
	apiServer, err := server.newAPIServer()
	if err != nil {
		return nil, fmt.Errorf("initialize API dependencies: %w", err)
	}
	server.apiServer = apiServer
	server.Logger.Info("Gecko server initialized successfully.")
	return server, nil
}

// Shutdown cancels service-owned audit work during process shutdown.
func (server *Server) Shutdown(ctx context.Context) error {
	if server.apiServer == nil || server.apiServer.StorageAnalytics == nil {
		return nil
	}
	return server.apiServer.StorageAnalytics.Shutdown(ctx)
}
