package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/calypr/gecko/gecko"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/go-authutils/authutils"
)

// @title Gecko API
// @version 1.0.0
// @description API for managing configurations and a generalizable vector database API
// @host localhost:8080
// @BasePath /
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization
// @description JWT token for authentication
func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	var port = flag.Uint("port", 80, "port on which to expose the API")
	var jwkEndpoint = flag.String("jwks", "", "endpoint for JWKS")
	var dbUrl = flag.String("db", "", "URL to connect to database")

	var qdrantHostFlag = flag.String("qdrant-host", "", "Qdrant host (overrides QDRANT_HOST env var)")
	var qdrantPortFlag = flag.Int("qdrant-port", 0, "Qdrant port (overrides QDRANT_PORT env var)")
	var qdrantAPIKeyFlag = flag.String("qdrant-api-key", "", "Qdrant API Key (overrides QDRANT_API_KEY env var)")

	var gripGraphName = flag.String("grip-graph-zname", "", "The graph name to use when querying Grip (overrides GRIP_GRAPH env var)")
	var gripPort = flag.String("grip-port", "", "The rpc port to be used for connecting to Grip (overrides GRIP_PORT env var)")
	var gripHost = flag.String("grip-host", "", "The hostname to be usd for connecting to Grip (overrides GRIP_HOST env var)")
	flag.Parse()

	gripGraph := *gripGraphName
	if gripGraph == "" {
		gripGraph = os.Getenv("GRIP_GRAPH")
	}

	gripPortVar := *gripPort
	if gripPortVar == "" {
		gripPortVar = os.Getenv("GRIP_PORT")
	}

	gripHostvar := *gripHost
	if gripHostvar == "" {
		gripHostvar = os.Getenv("GRIP_HOST")
	}

	qdrantHost := *qdrantHostFlag
	if qdrantHost == "" {
		qdrantHost = os.Getenv("QDRANT_HOST")
	}

	qdrantPort := *qdrantPortFlag
	if qdrantPort == 0 {
		portStr := os.Getenv("QDRANT_PORT")
		if portStr != "" {
			parsedPort, err := strconv.Atoi(portStr)
			if err == nil {
				qdrantPort = parsedPort
			}
		}
	}

	qdrantAPIKey := *qdrantAPIKeyFlag
	if qdrantAPIKey == "" {
		qdrantAPIKey = os.Getenv("QDRANT_API_KEY")
	}

	finalJwkEndpoint := *jwkEndpoint
	if finalJwkEndpoint == "" {
		finalJwkEndpoint = os.Getenv("JWKS_ENDPOINT")
	}
	if finalJwkEndpoint == "" {
		logger.Println("WARNING: no $JWKS_ENDPOINT or --jwks specified; endpoints requiring JWT validation will error")
	}
	jwtApp := authutils.NewJWTApplication(finalJwkEndpoint)

	serverBuilder := gecko.NewServer().
		WithLogger(logger).
		WithJWTApp(jwtApp)

	db, err := sqlx.Open("postgres", *dbUrl)
	if err != nil {
		logger.Printf("WARNING: Failed to open database connection with URL %s: %v. Database endpoints will not be available.", *dbUrl, err)
	} else {
		if err = db.Ping(); err != nil {
			logger.Printf("WARNING: DB ping failed for URL %s: %v. Database endpoints will not be available.", *dbUrl, err)
			db.Close()
		} else {
			logger.Println("Successfully connected to PostgreSQL database.")
			serverBuilder = serverBuilder.WithDB(db)
		}
	}

	if qdrantHost != "" && qdrantPort != 0 {
		if qdrantHost == "localhost" && *qdrantHostFlag == "" && os.Getenv("QDRANT_HOST") == "" {
			// Skip connection attempt if only default values would be used and no flag/env was set
			// This logic is slightly complex due to your existing defaults;
			// A simpler approach is to only check if the host was explicitly set.
			// Let's rely on the user to provide *at least* the host flag if they want Qdrant.
		} else {
			// Re-apply final defaults only if we decide to connect
			if qdrantHost == "" {
				qdrantHost = "localhost" // Final default
			}
			if qdrantPort == 0 {
				qdrantPort = 6334
			}

			logger.Printf("Attempting to connect to Qdrant at %s:%d", qdrantHost, qdrantPort)
			qdrantConfig := &qdrant.Config{
				Host:   qdrantHost,
				Port:   qdrantPort,
				APIKey: qdrantAPIKey,
			}

			qdrantClient, err := qdrant.NewClient(qdrantConfig)
			if err != nil {
				logger.Printf("WARNING: Failed to initialize Qdrant client at %s:%d: %v. Qdrant endpoints will not be available.", qdrantHost, qdrantPort, err)
			} else {
				logger.Println("Successfully connected to Qdrant.")
				serverBuilder = serverBuilder.WithQdrantClient(qdrantClient)
			}
		}
	} else {
		logger.Println("INFO: Qdrant configuration (--qdrant-host or QDRANT_HOST) not fully specified. Qdrant endpoints will not be available.")
	}

	if gripHostvar != "" && gripPortVar != "" {
		logger.Printf("Attempting to connect to Grip at %s:%s using graph %s", gripHostvar, gripPortVar, gripGraph)
		gripqlClient, err := gripql.Connect(rpc.ConfigWithDefaults(gripHostvar+":"+gripPortVar), false)
		if err != nil {
			logger.Printf("WARNING: Failed to initialize Grip client: %v. Grip endpoints will not be available.", err)
		} else {
			if gripGraph == "" {
				logger.Println("WARNING: Connected to Grip but no --grip-graph-name or GRIP_GRAPH specified. Grip endpoints may fail.")
			}
			logger.Println("Successfully connected to Grip.")
			serverBuilder = serverBuilder.WithGripqlClient(&gripqlClient, gripGraph)
		}
	} else {
		logger.Println("INFO: Grip configuration (--grip-host and --grip-port or environment variables) not fully specified. Grip endpoints will not be available.")
	}

	geckoServer, err := serverBuilder.Init()
	if err != nil {
		// Log fatal only if the core server initialization fails, independent of the clients
		log.Fatalf("Failed to initialize gecko server: %v", err)
	}

	app := geckoServer.MakeRouter()
	httpLogger := log.New(os.Stdout, "", log.LstdFlags)
	app.Logger().SetOutput(httpLogger.Writer())
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLog:     httpLogger,
		Handler:      app,
	}

	httpLogger.Println("gecko serving at", httpServer.Addr)
	if err = httpServer.ListenAndServe(); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
