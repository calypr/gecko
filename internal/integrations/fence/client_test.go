package fence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
)

func TestRequestOrganizationInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installed":            true,
			"organization":         "HTAN_INT",
			"installation_id":      42,
			"target":               "HTAN_INT",
			"target_type":          "Organization",
			"html_url":             "https://github.com/organizations/HTAN_INT/settings/installations/42",
			"repository_selection": "selected",
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	status, err := client.RequestOrganizationInstallationStatus(context.Background(), "Bearer user-token", "HTAN_INT", "htan-int-github")
	if err != nil {
		t.Fatalf("request organization installation status: %v", err)
	}
	if !status.Installed {
		t.Fatal("expected installed status")
	}
	if status.InstallationID == nil || *status.InstallationID != 42 {
		t.Fatalf("unexpected installation id: %+v", status.InstallationID)
	}
	if status.RepositorySelection != "selected" {
		t.Fatalf("unexpected repository selection: %q", status.RepositorySelection)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["action"] != "organization_installation" {
		t.Fatalf("expected organization_installation action, got %#v", receivedBody)
	}
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization payload, got %#v", receivedBody)
	}
	if receivedBody["owner"] != "htan-int-github" {
		t.Fatalf("expected owner payload, got %#v", receivedBody)
	}
}

func TestRequestOrganizationInstallationStatusStripsEmptyRepositoryIDsFromHTMLURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installed":            true,
			"organization":         "EllrottLab",
			"installation_id":      42,
			"target":               "EllrottLab",
			"target_type":          "Organization",
			"html_url":             "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=",
			"repository_selection": "selected",
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	status, err := client.RequestOrganizationInstallationStatus(context.Background(), "Bearer user-token", "TEST", "EllrottLab")
	if err != nil {
		t.Fatalf("request organization installation status: %v", err)
	}
	if status.HTMLURL != "https://github.com/organizations/EllrottLab/settings/installations/42" {
		t.Fatalf("unexpected html url: %q", status.HTMLURL)
	}
}

func TestRequestInstallationStatusForwardsAuthorizationAndParsesStatus(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installed":       true,
			"installation_id": 42,
			"target":          "HTAN_INT",
			"target_type":     "Organization",
			"html_url":        "https://github.com/organizations/HTAN_INT/settings/installations/42",
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	status, err := client.RequestInstallationStatus(context.Background(), "Bearer user-token", "HTAN_INT", domain.GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	})
	if err != nil {
		t.Fatalf("request installation status: %v", err)
	}
	if !status.Installed {
		t.Fatal("expected installed status")
	}
	if status.InstallationID == nil || *status.InstallationID != 42 {
		t.Fatalf("unexpected installation id: %+v", status.InstallationID)
	}
	if status.Target != "HTAN_INT" {
		t.Fatalf("unexpected target: %q", status.Target)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
}

func TestListInstallationRepositoriesForwardsAuthorizationAndParsesRepositories(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installation_id": 42,
			"repositories": []map[string]any{
				{
					"id":        101,
					"name":      "git_drs_test",
					"full_name": "Ellrott_Lab/git_drs_test",
					"html_url":  "https://github.com/EllrottLab/git_drs_test",
					"clone_url": "https://github.com/EllrottLab/git_drs_test.git",
				},
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	repositories, err := client.ListInstallationRepositories(context.Background(), "Bearer user-token", 42)
	if err != nil {
		t.Fatalf("list installation repositories: %v", err)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["action"] != "installation_repositories" {
		t.Fatalf("expected installation_repositories action, got %#v", receivedBody)
	}
	if receivedBody["installation_id"] != float64(42) {
		t.Fatalf("expected installation id in request body, got %#v", receivedBody)
	}
	if len(repositories) != 1 {
		t.Fatalf("expected one repository, got %+v", repositories)
	}
	if repositories[0].FullName != "Ellrott_Lab/git_drs_test" {
		t.Fatalf("unexpected repository: %+v", repositories[0])
	}
}

func TestRequestInstallationTokenForwardsAuthorizationAndParsesToken(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"token":      "ghs_test",
			"expires_at": "2026-05-20T18:00:00Z",
			"repository": map[string]string{"owner": "HTAN_INT", "repo": "BForePC"},
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	token, err := client.RequestInstallationToken(context.Background(), "Bearer user-token", "HTAN_INT", "BForePC", domain.GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	}, "write")
	if err != nil {
		t.Fatalf("request installation token: %v", err)
	}
	if token != "ghs_test" {
		t.Fatalf("expected token ghs_test, got %q", token)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["access"] != "write" {
		t.Fatalf("expected write access request, got %#v", receivedBody)
	}
	if receivedBody["action"] != "installation_token" {
		t.Fatalf("expected installation_token action, got %#v", receivedBody)
	}
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization HTAN_INT, got %#v", receivedBody)
	}
	if receivedBody["project"] != "BForePC" {
		t.Fatalf("expected project BForePC, got %#v", receivedBody)
	}
}

func TestRequestInstallationTokenReturnsFenceStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(writer).Encode(map[string]any{"message": "forbidden by fence"})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	_, err := client.RequestInstallationToken(context.Background(), "Bearer user-token", "HTAN_INT", "BForePC", domain.GitRepositoryIdentity{
		Owner: "HTAN_INT",
		Repo:  "BForePC",
	}, "read")
	if err == nil {
		t.Fatal("expected error")
	}
	statusErr, ok := err.(*domain.HTTPStatusError)
	if !ok {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", statusErr.StatusCode)
	}
	if statusErr.Message != "forbidden by fence" {
		t.Fatalf("unexpected status error message: %q", statusErr.Message)
	}
}

func TestRequestInstallationURLForwardsAuthorizationAndParsesInstallURL(t *testing.T) {
	var receivedAuth string
	var receivedBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		if request.URL.Path != "/credentials/github" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected request method: %s", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"install_url": "https://github.com/apps/calypr-github/installations/new?state=abc",
			"owner":       "HTAN_INT",
		})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	redirectURL, err := client.RequestInstallationURL(context.Background(), "Bearer user-token", "HTAN_INT", "/git/HTAN_INT")
	if err != nil {
		t.Fatalf("request installation URL: %v", err)
	}
	if redirectURL != "https://github.com/apps/calypr-github/installations/new?state=abc" {
		t.Fatalf("unexpected redirect URL: %q", redirectURL)
	}
	if receivedAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization header, got %q", receivedAuth)
	}
	if receivedBody["owner"] != "HTAN_INT" {
		t.Fatalf("expected owner HTAN_INT, got %q", receivedBody["owner"])
	}
	if receivedBody["organization"] != "HTAN_INT" {
		t.Fatalf("expected organization HTAN_INT, got %q", receivedBody["organization"])
	}
	if receivedBody["action"] != "install_url" {
		t.Fatalf("expected install_url action, got %#v", receivedBody)
	}
	if receivedBody["redirect_path"] != "/git/HTAN_INT" {
		t.Fatalf("expected redirect_path /git/HTAN_INT, got %q", receivedBody["redirect_path"])
	}
}

func TestRequestInstallationURLReturnsFenceStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(writer).Encode(map[string]any{"message": "forbidden by fence"})
	}))
	defer server.Close()

	client := NewClient(server.Client(), Config{BaseURL: server.URL})
	_, err := client.RequestInstallationURL(context.Background(), "Bearer user-token", "HTAN_INT", "/git/HTAN_INT")
	if err == nil {
		t.Fatal("expected error")
	}
	statusErr, ok := err.(*domain.HTTPStatusError)
	if !ok {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", statusErr.StatusCode)
	}
	if statusErr.Message != "forbidden by fence" {
		t.Fatalf("unexpected status error message: %q", statusErr.Message)
	}
}
