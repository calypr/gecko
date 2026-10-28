package repo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	geckodb "github.com/calypr/gecko/internal/db"
	integrationfence "github.com/calypr/gecko/internal/integrations/fence"
)

func TestRequestInstallationTokenRejectsUnlistedRepositoryHost(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	service := NewService(ServiceConfig{
		AllowedGitHubHosts: []string{"github.com"},
		FenceClient:        integrationfence.NewClient(server.Client(), integrationfence.Config{BaseURL: server.URL}),
	})
	_, err := service.RequestInstallationToken(
		context.Background(),
		"Bearer user-token",
		"org",
		"project",
		Identity{Host: "attacker.example", Owner: "owner", Repo: "repo"},
		"read",
	)
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected host rejection, got %v", err)
	}
	if requests != 0 {
		t.Fatalf("expected no Fence request, got %d", requests)
	}
}

func TestRefreshProjectRejectsUnlistedRepositoryHostBeforeMetadata(t *testing.T) {
	service := NewService(ServiceConfig{AllowedGitHubHosts: []string{"github.com"}})
	_, _, err := service.RefreshProject(
		context.Background(),
		"org/project",
		Identity{Host: "attacker.example", Owner: "owner", Repo: "repo"},
		&geckodb.GitProjectState{ProjectID: "org/project"},
		"install-token",
	)
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected host rejection, got %v", err)
	}
}

func TestNewServiceDerivesEnterpriseHostAllowlist(t *testing.T) {
	service := NewService(ServiceConfig{GitHubAPIBase: "https://github.example.org/api/v3"})
	if !service.githubRepositoryHostAllowed("github.example.org") {
		t.Fatal("expected enterprise host to be allowed")
	}
	if !service.githubRepositoryHostAllowed("github.com") {
		t.Fatal("expected public host to remain allowed")
	}
	if service.githubRepositoryHostAllowed("attacker.example") {
		t.Fatal("did not expect unrelated host to be allowed")
	}
}
