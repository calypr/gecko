package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/calypr/gecko/gecko"
	"github.com/jmoiron/sqlx"
	"github.com/qdrant/go-client/qdrant"
	"github.com/uc-cdis/go-authutils/authutils"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	var jwkEndpointEnv string = os.Getenv("JWKS_ENDPOINT")

	// EXISTING FLAGS
	var port *uint = flag.Uint("port", 80, "port on which to expose the API")
	var jwkEndpoint *string = flag.String(
		"jwks",
		jwkEndpointEnv,
		"endpoint from which the application can fetch a JWKS",
	)

	// NEW FLAGS FOR QDRANT CONFIGURATION
	var qdrantHost *string = flag.String(
		"qdrant-host",
		"localhost:6334", // Default to common gRPC port
		"The host and port for the Qdrant gRPC endpoint (e.g., localhost:6334)",
	)
	var qdrantPort *uint = flag.Uint( // Add a new flag for the port
		"qdrant-port",
		6334,
		"The port for the Qdrant gRPC endpoint (default 6334)",
	)
	var qdrantAPIKey *string = flag.String(
		"qdrant-api-key",
		"",
		"API Key for Qdrant authentication (optional)",
	)

	if *jwkEndpoint == "" {
		logger.Println("WARNING: no $JWKS_ENDPOINT or --jwks specified; endpoints requiring JWT validation will error")
	}

	var dbUrl *string = flag.String(
		"db",
		"",
		"URL to connect to database: postgresql://user:password@netloc:port/dbname\n"+
			"can also be specified through the postgres\n"+
			"environment variables. If using the commandline argument, add\n"+
			"?sslmode=disable",
	)

	// IMPORTANT: flag.Parse() is correctly placed here before accessing any flag values
	flag.Parse()

	// --- Database Initialization (Existing Logic) ---
	db, err := sqlx.Open("postgres", *dbUrl)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		logger.Fatalf("DB ping failed: %v", err)
		panic(err)
	}
	defer db.Close()

	jwtApp := authutils.NewJWTApplication(*jwkEndpoint)
	logger.Printf("JWT App Init: %#v\n", jwtApp.Keys)

	// --- Qdrant Client Initialization (NEW LOGIC) ---
	qdrantConfig := &qdrant.Config{
		// Use the values from the command-line flags
		Host:   *qdrantHost,
		APIKey: *qdrantAPIKey,
		Port:   int(*qdrantPort),
		// Add UseTLS: true if you need TLS/SSL for cloud
	}

	qdrantClient, err := qdrant.NewClient(qdrantConfig)
	if err != nil {
		logger.Fatalf("Failed to initialize Qdrant client: %v", err)
		panic(err)
	}

	// --- Server Initialization (UPDATED LOGIC) ---
	geckoServer, err := gecko.NewServer().
		WithLogger(logger).
		WithJWTApp(jwtApp).
		WithDB(db).
		// NEW: Pass the initialized Qdrant client to the server builder
		WithQdrantClient(qdrantClient).
		Init()
	if err != nil {
		log.Fatalf("Failed to initialize gecko server: %v", err)
	}

	// ... (rest of the server setup remains the same)
	app := geckoServer.MakeRouter()

	// Configure Iris logger to output to your httpLogger
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
	err = httpServer.ListenAndServe()
	if err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
