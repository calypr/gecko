package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/go-authutils/authutils"

	"github.com/calypr/gecko/internal/git"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
	integrationgithub "github.com/calypr/gecko/internal/integrations/github"
	"github.com/calypr/gecko/internal/presentation"
	server "github.com/calypr/gecko/internal/server"
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
	var jwkEndpoint = flag.String("jwks", "", "endpoint for JWKS")
	var dbURL = flag.String("db", "", "URL to connect to database")
	var gripGraphName = flag.String("grip-graph-zname", "", "The graph name to use when querying Grip (overrides GRIP_GRAPH env var)")
	var gripPort = flag.String("grip-port", "", "The rpc port to be used for connecting to Grip (overrides GRIP_PORT env var)")
	var gripHost = flag.String("grip-host", "", "The hostname to be usd for connecting to Grip (overrides GRIP_HOST env var)")
	var githubAPIBaseFlag = flag.String("github-api-base-url", "", "GitHub API base URL (overrides GITHUB_API_BASE_URL env var)")
	var fenceBaseURLFlag = flag.String("fence-base-url", "", "Fence base URL for GitHub App token exchange (overrides FENCE_BASE_URL env var)")
	var gitDataDirFlag = flag.String("git-data-dir", "", "Directory for local git mirrors (overrides GIT_DATA_DIR env var)")
	flag.Parse()

	gripGraph := firstNonEmpty(*gripGraphName, os.Getenv("GRIP_GRAPH"))
	gripPortVar := firstNonEmpty(*gripPort, os.Getenv("GRIP_PORT"))
	gripHostVar := firstNonEmpty(*gripHost, os.Getenv("GRIP_HOST"))
	finalJWK := firstNonEmpty(*jwkEndpoint, os.Getenv("JWKS_ENDPOINT"))
	if finalJWK == "" {
		logger.Println("WARNING: no $JWKS_ENDPOINT or --jwks specified; endpoints requiring JWT validation will error")
	}

	serverBuilder := server.NewServer().WithLogger(logger).WithJWTApp(authutils.NewJWTApplication(finalJWK))
	if db, err := sqlx.Open("postgres", *dbURL); err != nil {
		logger.Println("WARNING: Failed to open database connection. Database endpoints will not be available.")
	} else if err = db.Ping(); err != nil {
		logger.Println("WARNING: DB ping failed. Database endpoints will not be available.")
		_ = db.Close()
	} else {
		logger.Println("Successfully connected to PostgreSQL database.")
		serverBuilder = serverBuilder.WithDB(db)
		githubAPIBase := firstNonEmpty(*githubAPIBaseFlag, os.Getenv("GITHUB_API_BASE_URL"))
		fenceBaseURL := firstNonEmpty(*fenceBaseURLFlag, os.Getenv("FENCE_BASE_URL"))
		gitDataDir := firstNonEmpty(*gitDataDirFlag, os.Getenv("GIT_DATA_DIR"))
		gitService := git.NewGitService(git.GitServiceConfig{
			GitHubAPIBase: githubAPIBase,
			FenceBaseURL:  fenceBaseURL,
			DataDir:       gitDataDir,
			FenceClient:   integrationfence.NewClient(nil, integrationfence.Config{BaseURL: fenceBaseURL}),
			GitHubClient:  integrationgithub.NewClient(nil, integrationgithub.Config{APIBase: githubAPIBase}),
		})
		serverBuilder = serverBuilder.WithGitService(gitService)
		serverBuilder = serverBuilder.WithThumbnailStore(thumbnail.NewFilesystemStore(gitDataDir))
		serverBuilder = serverBuilder.WithPresentationStore(presentation.NewFilesystemStore(gitDataDir))
	}

	if gripHostVar != "" && gripPortVar != "" {
		logger.Printf("Attempting to connect to Grip at %s:%s using graph %s", gripHostVar, gripPortVar, gripGraph)
		if gripqlClient, err := gripql.Connect(rpc.ConfigWithDefaults(gripHostVar+":"+gripPortVar), false); err != nil {
			logger.Printf("WARNING: Failed to initialize Grip client: %v. Grip endpoints will not be available.", err)
		} else {
			logger.Println("Successfully connected to Grip.")
			serverBuilder = serverBuilder.WithGripqlClient(&gripqlClient, gripGraph)
		}
	} else {
		logger.Println("INFO: Grip configuration (--grip-host and --grip-port or environment variables) not fully specified. Grip endpoints will not be available.")
	}

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
