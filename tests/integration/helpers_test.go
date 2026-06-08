package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

const defaultBaseURL = "http://localhost:8080"

func integrationBaseURL() string {
	if base := os.Getenv("GECKO_INTEGRATION_BASE_URL"); base != "" {
		return base
	}
	return defaultBaseURL
}

func requireIntegrationServer(t *testing.T) string {
	t.Helper()
	baseURL := integrationBaseURL()
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("invalid integration base URL %q: %v", baseURL, err)
	}
	host := parsed.Host
	if parsed.Port() == "" {
		switch parsed.Scheme {
		case "https":
			host = net.JoinHostPort(parsed.Hostname(), "443")
		default:
			host = net.JoinHostPort(parsed.Hostname(), "80")
		}
	}
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		t.Skipf("skipping integration test: gecko server is not reachable at %s: %v", baseURL, err)
	}
	_ = conn.Close()
	return baseURL
}

func integrationURL(path string) string {
	return fmt.Sprintf("%s%s", integrationBaseURL(), path)
}
