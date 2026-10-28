package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"

	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
	integrationgithub "github.com/calypr/gecko/internal/integrations/github"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/repo"
	server "github.com/calypr/gecko/internal/server"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/thumbnail"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	var port = flag.Uint("port", 8080, "port on which to expose the API")
	var dbURL = flag.String("db", "", "URL to connect to database")
	var githubAPIBaseFlag = flag.String("github-api-base-url", "", "GitHub API base URL (overrides GITHUB_API_BASE_URL env var)")
	var fenceBaseURLFlag = flag.String("fence-base-url", "", "Fence base URL for GitHub App token exchange (overrides FENCE_BASE_URL env var)")
	var gitDataDirFlag = flag.String("git-data-dir", "", "Directory for local git mirrors (overrides GIT_DATA_DIR env var)")
	flag.Parse()

	databaseURL := firstNonEmpty(*dbURL, os.Getenv("DATABASE_URL"))
	githubAPIBase := firstNonEmpty(*githubAPIBaseFlag, os.Getenv("GITHUB_API_BASE_URL"))
	fenceBaseURL := firstNonEmpty(*fenceBaseURLFlag, os.Getenv("FENCE_BASE_URL"))
	gitDataDir := firstNonEmpty(*gitDataDirFlag, os.Getenv("GIT_DATA_DIR"))

	if fenceBaseURL != "" {
		access.SetFenceTrust(access.FenceTrust{AllowedIssuers: []string{fenceBaseURL}})
	}
	allowedGitHubHosts := appconfig.GitHubWebHostsForAPIBase(githubAPIBase)
	appconfig.ValidateProjectRepository = func(_ context.Context, raw string) (string, error) {
		return appconfig.NormalizeProjectRepositoryURLWithHosts(raw, allowedGitHubHosts)
	}

	serverBuilder := server.NewServer().WithLogger(logger)
	if db, err := sqlx.Open("postgres", databaseURL); err != nil {
		logger.Println("WARNING: Failed to open database connection. Database endpoints will not be available.")
	} else if err = db.Ping(); err != nil {
		logger.Println("WARNING: DB ping failed. Database endpoints will not be available.")
		_ = db.Close()
	} else {
		logger.Println("Successfully connected to PostgreSQL database.")
		serverBuilder = serverBuilder.WithDB(db)
	}
	repoService := repo.NewService(repo.ServiceConfig{
		GitHubAPIBase:        githubAPIBase,
		AllowedGitHubHosts:   allowedGitHubHosts,
		FenceBaseURL:         fenceBaseURL,
		DataDir:              gitDataDir,
		FenceClient:          integrationfence.NewClient(nil, integrationfence.Config{BaseURL: fenceBaseURL}),
		GitHubClient:         integrationgithub.NewClient(nil, integrationgithub.Config{APIBase: githubAPIBase}),
		RepoAnalyticsIndexer: storageaudit.PersistRepoAnalyticsIndex,
	})
	serverBuilder = serverBuilder.WithRepoService(repoService)
	serverBuilder = serverBuilder.WithThumbnailStore(thumbnail.NewFilesystemStore(gitDataDir))
	serverBuilder = serverBuilder.WithPresentationStore(presentation.NewFilesystemStore(gitDataDir))

	geckoServer, err := serverBuilder.Init()
	if err != nil {
		log.Fatalf("Failed to initialize gecko server: %v", err)
	}
	addr := fmt.Sprintf(":%d", *port)
	logger.Println("gecko serving at", addr)

	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	httpServer, err := geckoServer.MakeHTTPServer(addr, serviceContext)
	if err != nil {
		log.Fatalf("Failed to initialize gecko HTTP server: %v", err)
	}
	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("Server failed to start:", err)
		}
	case <-shutdownSignal.Done():
		logger.Println("shutting down gecko")
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelShutdown()
		cancelService()
		if err := geckoServer.Shutdown(shutdownContext); err != nil {
			logger.Printf("failed to stop storage audit work cleanly: %v", err)
		}
		if err := httpServer.Shutdown(shutdownContext); err != nil {
			logger.Printf("failed to shut down HTTP server cleanly: %v", err)
			_ = httpServer.Close()
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("HTTP server stopped with error: %v", err)
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
