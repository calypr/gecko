package repo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/gecko/internal/domain"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
)

func TestGetGitHubFileMetadataUsesReadTokenAndReturnsContent(t *testing.T) {
	var tokenRequest map[string]any
	githubRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/credentials/github":
			if got := r.Header.Get("Authorization"); got != "Bearer user-token" {
				t.Errorf("Fence authorization = %q, want Bearer user-token", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&tokenRequest); err != nil {
				t.Errorf("decode token request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"install-token"}`))

		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/repos/BForePC/BForePC/contents/META/Practitioner.ndjson":
			githubRequests++
			if got := r.Header.Get("Authorization"); got != "Bearer install-token" {
				t.Errorf("GitHub authorization = %q, want Bearer install-token", got)
			}
			if got := r.URL.Query().Get("ref"); got != "main" {
				t.Errorf("GitHub ref = %q, want main", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"file","name":"Practitioner.ndjson","path":"META/Practitioner.ndjson","sha":"abc","size":43,"content":"dmVyc2lvbg==","encoding":"base64"}`))

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})

	metadata, content, err := service.GetGitHubFileMetadata(
		context.Background(),
		"Bearer user-token",
		"Ellrott_Lab",
		"test",
		Identity{Owner: "BForePC", Repo: "BForePC"},
		"main",
		"META/Practitioner.ndjson",
	)
	if err != nil {
		t.Fatalf("get GitHub file metadata: %v", err)
	}
	if githubRequests != 1 {
		t.Fatalf("GitHub requests = %d, want 1", githubRequests)
	}
	if metadata.GetPath() != "META/Practitioner.ndjson" {
		t.Fatalf("metadata path = %q", metadata.GetPath())
	}
	if string(content) != "version" {
		t.Fatalf("content = %q, want version", string(content))
	}
	if tokenRequest["action"] != "installation_token" {
		t.Fatalf("token action = %#v", tokenRequest["action"])
	}
	if tokenRequest["access"] != "read" {
		t.Fatalf("token access = %#v, want read", tokenRequest["access"])
	}
	if tokenRequest["owner"] != "BForePC" || tokenRequest["repo"] != "BForePC" {
		t.Fatalf("unexpected token repository: %#v", tokenRequest)
	}
	if tokenRequest["organization"] != "Ellrott_Lab" || tokenRequest["project"] != "test" {
		t.Fatalf("unexpected token project: %#v", tokenRequest)
	}
}

func TestGetGitHubFileMetadataMapsGitHubNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/credentials/github":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"install-token"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/repos/BForePC/BForePC/contents/META/Missing.ndjson":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		GitHubAPIBase: server.URL + "/api/v3",
		HTTPClient:    server.Client(),
		FenceClient:   integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})

	_, _, err := service.GetGitHubFileMetadata(
		context.Background(),
		"Bearer user-token",
		"Ellrott_Lab",
		"test",
		Identity{Owner: "BForePC", Repo: "BForePC"},
		"main",
		"META/Missing.ndjson",
	)
	if err == nil {
		t.Fatal("expected not-found error")
	}
	var statusErr *domain.HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", statusErr.StatusCode)
	}
	if statusErr.Code != "integration_error" {
		t.Fatalf("code = %q, want integration_error", statusErr.Code)
	}
	if !strings.Contains(statusErr.Message, "GitHub file lookup failed") {
		t.Fatalf("unexpected message: %q", statusErr.Message)
	}
}
