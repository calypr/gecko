package git

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/calypr/gecko/config"
	gitservice "github.com/calypr/gecko/internal/git"
	intfence "github.com/calypr/gecko/internal/integrations/fence"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/server/http/shared"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func newGitHandlerTestServer(t *testing.T, fenceServer *httptest.Server, githubTransport http.RoundTripper) (*Handler, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	logger := &geckologging.Handler{Logger: log.New(os.Stdout, "", 0)}
	githubClient := &http.Client{Timeout: 5 * time.Second}
	if githubTransport != nil {
		githubClient.Transport = githubTransport
	}
	gitSvc := gitservice.NewGitService(gitservice.GitServiceConfig{
		DataDir:       t.TempDir(),
		GitHubAPIBase: "https://api.github.com",
		HTTPClient:    githubClient,
		FenceClient:   intfence.NewClient(fenceServer.Client(), intfence.Config{BaseURL: fenceServer.URL}),
	})
	handler := &Handler{
		Handler:    &shared.Handler{},
		db:         sqlx.NewDb(db, "sqlmock"),
		logger:     logger,
		gitService: gitSvc,
	}
	return handler, mock, func() { _ = db.Close() }
}

func runGitRequest(t *testing.T, app *fiber.App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0, FailOnTimeout: false})
	if err != nil {
		t.Fatalf("fiber test request failed: %v", err)
	}
	return resp
}

func TestGitOrganizationInitConnectReturnsRedirectWithoutRepository(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"install_url": "https://github.com/apps/calypr-github/installations/new?state=%2Fgit",
		})
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", handler.handleGitOrganizationInitConnectPOST)
	req := httptest.NewRequest(http.MethodPost, "/git/organizations/TEST/init-connect", nil)
	req.Header.Set("Authorization", "Bearer test")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Mode != "redirect" {
		t.Fatalf("expected redirect mode, got %+v", payload)
	}
	if payload.RedirectURL != "https://github.com/apps/calypr-github/installations/new?state=%2Fgit" {
		t.Fatalf("unexpected redirect url: %q", payload.RedirectURL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitOrganizationInitConnectAppendsRepositorySuggestionWhenResolvable(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"install_url": "https://github.com/apps/calypr-github/installations/new?state=%2Fgit%2FTEST",
		})
	}))
	defer fenceServer.Close()

	githubTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host != "api.github.com" || request.URL.Path != "/repos/EllrottLab/git_drs_test" {
			t.Fatalf("unexpected github request: %s %s", request.Method, request.URL.String())
		}
		body := `{"id":456,"owner":{"id":123}}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, githubTransport)
	defer cleanup()

	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", handler.handleGitOrganizationInitConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/organizations/TEST/init-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, payload)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(payload.RedirectURL, "/installations/new/permissions") {
		t.Fatalf("expected permissions redirect, got %q", payload.RedirectURL)
	}
	if !strings.Contains(payload.RedirectURL, "suggested_target_id=123") || !strings.Contains(payload.RedirectURL, "repository_ids[]=456") {
		t.Fatalf("expected redirect optimization params, got %q", payload.RedirectURL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitOrganizationInitConnectFallsBackToPlainRedirectWhenRepositoryLookupFails(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"install_url": "https://github.com/apps/calypr-github/installations/new?state=%2Fgit%2FTEST",
		})
	}))
	defer fenceServer.Close()

	githubTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"Not Found"}`)),
		}, nil
	})

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, githubTransport)
	defer cleanup()

	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", handler.handleGitOrganizationInitConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/organizations/TEST/init-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, payload)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.RedirectURL != "https://github.com/apps/calypr-github/installations/new?state=%2Fgit%2FTEST" {
		t.Fatalf("expected plain redirect url, got %q", payload.RedirectURL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitOrganizationInitConnectFallsBackToCleanOrgSettingsURLWhenRepositoryLookupFails(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var receivedBody map[string]any
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode fence request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		switch receivedBody["action"] {
		case "install_url":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"install_url": "https://github.com/apps/calypr-github/installations/select_target?state=%2Fgit%2FTEST",
			})
		case "organization_installation":
			if receivedBody["owner"] != "EllrottLab" {
				t.Fatalf("expected organization installation lookup for EllrottLab, got %#v", receivedBody["owner"])
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"installed":            true,
				"organization":         "EllrottLab",
				"installation_id":      134470697,
				"target":               "EllrottLab",
				"target_type":          "Organization",
				"html_url":             "https://github.com/organizations/EllrottLab/settings/installations/134470697?repository_ids=",
				"repository_selection": "selected",
			})
		default:
			t.Fatalf("unexpected fence action: %#v", receivedBody["action"])
		}
	}))
	defer fenceServer.Close()

	githubTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "api.github.com" && request.URL.Path == "/repos/EllrottLab/git_drs_test" {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"message":"Not Found"}`)),
			}, nil
		}
		t.Fatalf("unexpected github request: %s %s", request.Method, request.URL.String())
		return nil, nil
	})

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, githubTransport)
	defer cleanup()

	app := fiber.New()
	app.Post("/git/organizations/:orgTitle/init-connect", handler.handleGitOrganizationInitConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/organizations/TEST/init-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, payload)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.RedirectURL != "https://github.com/organizations/EllrottLab/settings/installations/134470697" {
		t.Fatalf("expected clean organization settings redirect when repo lookup fails, got %q", payload.RedirectURL)
	}
	if strings.Contains(payload.RedirectURL, "suggested_target_id=") || strings.Contains(payload.RedirectURL, "repository_ids") {
		t.Fatalf("did not expect partial redirect optimization or empty repository_ids, got %q", payload.RedirectURL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectEditConnectUpdatesProjectConfigAndState(t *testing.T) {
	var receivedBody map[string]any
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("decode fence request body: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installation_id": 42,
			"repositories": []map[string]any{{
				"id":        101,
				"name":      "git_drs_test",
				"full_name": "EllrottLab/git_drs_test",
				"html_url":  "https://github.com/EllrottLab/git_drs_test",
				"clone_url": "https://github.com/EllrottLab/git_drs_test.git",
			}},
		})
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	projectCfg := appconfig.ProjectConfig{Title: "proj-a", OrgTitle: "TEST", ProjectTitle: "proj-a", SrcRepo: ""}
	projectContent, err := json.Marshal(projectCfg)
	if err != nil {
		t.Fatalf("marshal project config: %v", err)
	}
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("TEST/proj-a").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("TEST/proj-a", projectContent))
	mock.ExpectQuery(`SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema\.git_organization_state WHERE organization = \$1`).
		WithArgs("TEST").
		WillReturnRows(sqlmock.NewRows([]string{"organization", "installed", "installation_id", "installation_target_type", "installation_target", "html_url", "repository_selection", "configured_at", "last_seen_at", "updated_at", "last_error"}).
			AddRow("TEST", true, 42, "Organization", "EllrottLab", "", "selected", nil, nil, time.Now(), nil))
	updatedCfg := projectCfg
	updatedCfg.SrcRepo = "https://github.com/EllrottLab/git_drs_test"
	updatedContent, err := json.Marshal(&updatedCfg)
	if err != nil {
		t.Fatalf("marshal updated project config: %v", err)
	}
	mock.ExpectQuery(`SELECT project_id, repo_host, repo_owner, repo_name, installation_id, installation_target_type, installation_target, mirror_path, sync_state, default_branch, last_refreshed_at, last_error FROM config_schema\.git_project_state WHERE project_id = \$1`).
		WithArgs("TEST/proj-a").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO config_schema\.projects`).
		WithArgs("TEST/proj-a", updatedContent).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO config_schema\.git_project_state`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/edit-connect", handler.handleGitProjectEditConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/projects/TEST/proj-a/edit-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, payload)
	}
	if receivedBody["action"] != "installation_repositories" {
		t.Fatalf("expected installation_repositories action, got %#v", receivedBody)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Mode != "connected" {
		t.Fatalf("expected connected mode, got %+v", payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectEditConnectRejectsRepositoryOutsideInstallation(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"installation_id": 42,
			"repositories": []map[string]any{{
				"id":        101,
				"name":      "other-repo",
				"full_name": "EllrottLab/other-repo",
				"html_url":  "https://github.com/EllrottLab/other-repo",
				"clone_url": "https://github.com/EllrottLab/other-repo.git",
			}},
		})
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	projectCfg := appconfig.ProjectConfig{Title: "proj-a", OrgTitle: "TEST", ProjectTitle: "proj-a", SrcRepo: ""}
	projectContent, err := json.Marshal(projectCfg)
	if err != nil {
		t.Fatalf("marshal project config: %v", err)
	}
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("TEST/proj-a").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("TEST/proj-a", projectContent))
	mock.ExpectQuery(`SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema\.git_organization_state WHERE organization = \$1`).
		WithArgs("TEST").
		WillReturnRows(sqlmock.NewRows([]string{"organization", "installed", "installation_id", "installation_target_type", "installation_target", "html_url", "repository_selection", "configured_at", "last_seen_at", "updated_at", "last_error"}).
			AddRow("TEST", true, 42, "Organization", "EllrottLab", "", "selected", nil, nil, time.Now(), nil))

	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/edit-connect", handler.handleGitProjectEditConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/projects/TEST/proj-a/edit-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectEditConnectRequiresConnectedOrganization(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	projectCfg := appconfig.ProjectConfig{Title: "proj-a", OrgTitle: "TEST", ProjectTitle: "proj-a", SrcRepo: ""}
	projectContent, err := json.Marshal(projectCfg)
	if err != nil {
		t.Fatalf("marshal project config: %v", err)
	}
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("TEST/proj-a").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("TEST/proj-a", projectContent))
	mock.ExpectQuery(`SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema\.git_organization_state WHERE organization = \$1`).
		WithArgs("TEST").
		WillReturnRows(sqlmock.NewRows([]string{"organization", "installed", "installation_id", "installation_target_type", "installation_target", "html_url", "repository_selection", "configured_at", "last_seen_at", "updated_at", "last_error"}).
			AddRow("TEST", false, nil, nil, nil, nil, nil, nil, nil, time.Now(), nil))

	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/edit-connect", handler.handleGitProjectEditConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":"EllrottLab/git_drs_test"}`)
	req := httptest.NewRequest(http.MethodPost, "/git/projects/TEST/proj-a/edit-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectEditConnectClearsRepositoryBinding(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	projectCfg := appconfig.ProjectConfig{
		Title:        "proj-a",
		OrgTitle:     "TEST",
		ProjectTitle: "proj-a",
		SrcRepo:      "https://github.com/EllrottLab/git_drs_test",
	}
	projectContent, err := json.Marshal(projectCfg)
	if err != nil {
		t.Fatalf("marshal project config: %v", err)
	}
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("TEST/proj-a").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("TEST/proj-a", projectContent))
	updatedCfg := projectCfg
	updatedCfg.SrcRepo = ""
	updatedContent, err := json.Marshal(&updatedCfg)
	if err != nil {
		t.Fatalf("marshal updated project config: %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO config_schema\.projects`).
		WithArgs("TEST/proj-a", updatedContent).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM config_schema\.git_project_state WHERE project_id = \$1`).
		WithArgs("TEST/proj-a").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/edit-connect", handler.handleGitProjectEditConnectPOST)
	body := bytes.NewBufferString(`{"repository_full_name":""}`)
	req := httptest.NewRequest(http.MethodPost, "/git/projects/TEST/proj-a/edit-connect", body)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, payload)
	}
	var payload gitservice.GitOrganizationConnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Mode != "disconnected" {
		t.Fatalf("expected disconnected mode, got %+v", payload)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGitProjectUpdateRejectsSetupOnlyProjectUntilGitHubConnectCompletes(t *testing.T) {
	fenceServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer fenceServer.Close()

	handler, mock, cleanup := newGitHandlerTestServer(t, fenceServer, nil)
	defer cleanup()

	projectCfg := appconfig.ProjectConfig{
		Title:        "proj-a",
		OrgTitle:     "TEST",
		ProjectTitle: "proj-a",
		Description:  "setup only",
		ContactEmail: "test@example.org",
		SrcRepo:      "",
	}
	projectContent, err := json.Marshal(projectCfg)
	if err != nil {
		t.Fatalf("marshal project config: %v", err)
	}
	mock.ExpectQuery(`SELECT name, content FROM config_schema\.projects WHERE name=\$1`).
		WithArgs("TEST/proj-a").
		WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("TEST/proj-a", projectContent))

	app := fiber.New()
	app.Post("/git/projects/:orgTitle/:projectTitle/update", handler.handleGitProjectUpdatePOST)
	req := httptest.NewRequest(http.MethodPost, "/git/projects/TEST/proj-a/update", nil)
	req.Header.Set("Authorization", "Bearer test")

	resp := runGitRequest(t, app, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, payload)
	}
	var payload struct {
		Error struct {
			Message string         `json:"message"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(payload.Error.Message, "GitHub connection has not been completed") {
		t.Fatalf("unexpected error message: %q", payload.Error.Message)
	}
	if got := payload.Error.Details["workflow_stage"]; got != gitservice.GitWorkflowStageAwaitingGitHubConnect {
		t.Fatalf("expected workflow stage %q, got %#v", gitservice.GitWorkflowStageAwaitingGitHubConnect, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
