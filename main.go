package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/calypr/gecko/gecko"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/go-authutils/authutils"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	var port = flag.Uint("port", 8080, "port on which to expose the API")
	var jwkEndpoint = flag.String("jwks", "", "endpoint for JWKS")
	var dbUrl = flag.String("db", "", "URL to connect to database")

	var qdrantHostFlag = flag.String("qdrant-host", "", "Qdrant host (overrides QDRANT_HOST env var)")
	var qdrantPortFlag = flag.Int("qdrant-port", 0, "Qdrant port (overrides QDRANT_PORT env var)")
	var qdrantAPIKeyFlag = flag.String("qdrant-api-key", "", "Qdrant API Key (overrides QDRANT_API_KEY env var)")

	flag.Parse()

	qdrantHost := *qdrantHostFlag
	if qdrantHost == "" {
		qdrantHost = os.Getenv("QDRANT_HOST")
	}
	if qdrantHost == "" {
		qdrantHost = "localhost" // Final default
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
	if qdrantPort == 0 {
		qdrantPort = 6334 // Final default
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

	db, err := sqlx.Open("postgres", *dbUrl)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	if err = db.Ping(); err != nil {
		logger.Fatalf("DB ping failed: %v", err)
	}
	defer db.Close()

	jwtApp := authutils.NewJWTApplication(finalJwkEndpoint)

	logger.Printf("Connecting to Qdrant at %s:%d", qdrantHost, qdrantPort)
	qdrantConfig := &qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort,
		APIKey: qdrantAPIKey,
	}

	qdrantClient, err := qdrant.NewClient(qdrantConfig)
	if err != nil {
		logger.Fatalf("Failed to initialize Qdrant client: %v", err)
	}

	// 4. Initialize the server. It will now use the correctly configured client.
	geckoServer, err := gecko.NewServer().
		WithLogger(logger).
		WithJWTApp(jwtApp).
		WithDB(db).
		WithQdrantClient(qdrantClient). // This client is now correctly configured
		Init()
	if err != nil {
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
