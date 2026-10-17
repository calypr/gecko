package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"

	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
	integrationgithub "github.com/calypr/gecko/internal/integrations/github"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/repo"
	server "github.com/calypr/gecko/internal/server"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/thumbnail"
)

// @title Gecko API
// @version 1.0.0
// @description API for managing configurations and Git-backed projects
// @host localhost:8080
// @BasePath /
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization
// @description JWT token for authentication
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
	app := geckoServer.MakeRouter()
	addr := fmt.Sprintf(":%d", *port)
	logger.Println("gecko serving at", addr)
	if err := app.Listen(addr, fiber.ListenConfig{DisableStartupMessage: true}); err != nil {
		log.Fatal("Server failed to start:", err)
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
