package project

import (
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/calypr/gecko/apierror"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
)

func TestMain(m *testing.M) {
	access.SetFenceTrust(access.FenceTrust{AllowedIssuers: []string{"https://fence.example.org"}})
	os.Exit(m.Run())
}

func TestOrganizationConfigurationState(t *testing.T) {
	tests := []struct {
		name               string
		appInstalled       bool
		configuredProjects int
		totalProjects      int
		want               string
	}{
		{name: "not installed", configuredProjects: 0, totalProjects: 0, want: "not_connected"},
		{name: "installed with no projects", appInstalled: true, want: "connected"},
		{name: "installed with no configured projects", appInstalled: true, totalProjects: 2, want: "installed_unconfigured"},
		{name: "partially configured", appInstalled: true, configuredProjects: 1, totalProjects: 2, want: "partially_configured"},
		{name: "fully configured", appInstalled: true, configuredProjects: 2, totalProjects: 2, want: "connected"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := OrganizationConfigurationState(test.appInstalled, test.configuredProjects, test.totalProjects)
			if got != test.want {
				t.Fatalf("OrganizationConfigurationState() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStatusFromState(t *testing.T) {
	t.Run("setup only project awaits github connect", func(t *testing.T) {
		response := StatusFromState("TEST/project", "TEST", "project", appconfig.ProjectConfig{}, domain.GitRepositoryIdentity{}, nil, nil)
		if response.WorkflowStage != GitWorkflowStageAwaitingGitHubConnect {
			t.Fatalf("workflow stage = %q, want %q", response.WorkflowStage, GitWorkflowStageAwaitingGitHubConnect)
		}
		if response.InstallationState != GitInstallationNotConnected {
			t.Fatalf("installation state = %q, want %q", response.InstallationState, GitInstallationNotConnected)
		}
	})

	t.Run("organization installation marks project connected without project state", func(t *testing.T) {
		cfg := appconfig.ProjectConfig{SrcRepo: "https://github.com/owner/repo"}
		orgState := &geckodb.GitOrganizationState{
			Installed:           true,
			HTMLURL:             sql.NullString{String: "https://github.com/owner", Valid: true},
			RepositorySelection: sql.NullString{String: "all", Valid: true},
		}
		response := StatusFromState("TEST/project", "TEST", "project", cfg, domain.GitRepositoryIdentity{}, nil, orgState)
		if response.WorkflowStage != GitWorkflowStageGitHubConnected {
			t.Fatalf("workflow stage = %q, want %q", response.WorkflowStage, GitWorkflowStageGitHubConnected)
		}
		if response.InstallationState != GitInstallationConnected {
			t.Fatalf("installation state = %q, want %q", response.InstallationState, GitInstallationConnected)
		}
		if response.OrganizationHTMLURL != "https://github.com/owner" || response.OrganizationRepositorySelection != "all" {
			t.Fatalf("unexpected organization state: %+v", response)
		}
	})

	t.Run("project state populates installation and mirror readiness", func(t *testing.T) {
		mirrorPath := t.TempDir()
		refreshedAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
		installationID := int64(42)
		state := &geckodb.GitProjectState{
			InstallationID:         sql.NullInt64{Int64: installationID, Valid: true},
			InstallationTarget:     sql.NullString{String: "owner", Valid: true},
			InstallationTargetType: sql.NullString{String: "Organization", Valid: true},
			MirrorPath:             mirrorPath,
			SyncState:              GitSyncReady,
			DefaultBranch:          sql.NullString{String: "main", Valid: true},
			LastRefreshedAt:        sql.NullTime{Time: refreshedAt, Valid: true},
			LastError:              sql.NullString{String: "previous failure", Valid: true},
		}
		response := StatusFromState("TEST/project", "TEST", "project", appconfig.ProjectConfig{SrcRepo: "https://github.com/owner/repo"}, domain.GitRepositoryIdentity{}, state, nil)
		if response.InstallationID == nil || *response.InstallationID != installationID {
			t.Fatalf("installation id = %v, want %d", response.InstallationID, installationID)
		}
		if response.InstallationTarget != "owner" || response.InstallationTargetType != "Organization" {
			t.Fatalf("unexpected installation target: %+v", response)
		}
		if response.SyncState != GitSyncReady || response.DefaultBranch != "main" || response.LastError != "previous failure" {
			t.Fatalf("unexpected sync state: %+v", response)
		}
		if response.LastRefreshedAt == nil || !response.LastRefreshedAt.Equal(refreshedAt) {
			t.Fatalf("last refreshed at = %v, want %v", response.LastRefreshedAt, refreshedAt)
		}
		if !response.MirrorReady {
			t.Fatal("expected mirror to be ready")
		}
	})
}

func TestApplyOrganizationStatusCapabilities(t *testing.T) {
	t.Run("nil response is ignored", func(t *testing.T) {
		ApplyOrganizationStatusCapabilities(nil, nil)
	})

	t.Run("maps arborist permissions to response capabilities", func(t *testing.T) {
		response := GitOrganizationStatusResponse{
			Organization: "TEST",
			Projects: []GitOrganizationProjectStatus{
				{ResourcePath: "/programs/TEST/projects/a"},
				{ResourcePath: "/programs/TEST/projects/b"},
			},
		}
		snapshot := access.ResourceAccessSnapshot{
			"/programs/TEST/projects":   {{Method: "create-descendant", Service: "arborist"}},
			"/programs/TEST":            {{Method: "delete", Service: "*"}},
			"/programs/TEST/projects/a": {{Method: "update", Service: "*"}},
		}
		ApplyOrganizationStatusCapabilities(&response, snapshot)
		if !response.CanAccessSettings {
			t.Fatal("expected settings access")
		}
		if !response.CanCreateProjects {
			t.Fatal("expected create project permission")
		}
		if response.CanManagePeople {
			t.Fatal("did not expect manage people permission")
		}
		if !response.CanDeleteOrg {
			t.Fatal("expected delete organization permission")
		}
		if !response.Projects[0].CanManageSettings {
			t.Fatal("expected project a settings permission")
		}
		if response.Projects[1].CanManageSettings {
			t.Fatal("did not expect project b settings permission")
		}
	})

	t.Run("manage people implies delete organization", func(t *testing.T) {
		response := GitOrganizationStatusResponse{Organization: "TEST"}
		snapshot := access.ResourceAccessSnapshot{
			"/programs/TEST": {{Method: "manage-owners", Service: "arborist"}},
		}
		ApplyOrganizationStatusCapabilities(&response, snapshot)
		if !response.CanManagePeople || !response.CanDeleteOrg {
			t.Fatalf("expected manage people and delete org, got %+v", response)
		}
	})
}

func TestSplitProjectID(t *testing.T) {
	tests := []struct {
		input            string
		wantOrganization string
		wantProject      string
	}{
		{input: "TEST/project", wantOrganization: "TEST", wantProject: "project"},
		{input: " TEST / project ", wantOrganization: "TEST", wantProject: "project"},
		{input: "TEST", wantOrganization: "TEST", wantProject: ""},
		{input: "", wantOrganization: "", wantProject: ""},
	}
	for _, test := range tests {
		organization, projectID := splitProjectID(test.input)
		if organization != test.wantOrganization || projectID != test.wantProject {
			t.Fatalf("splitProjectID(%q) = (%q, %q), want (%q, %q)", test.input, organization, projectID, test.wantOrganization, test.wantProject)
		}
	}
}

func TestStorageChecks(t *testing.T) {
	buckets := map[string]StorageBucket{
		"bucket-a": {Resources: []string{" /programs/TEST/projects/project "}},
		"bucket-b": {Resources: []string{"/organization/OTHER/project/other"}},
	}
	t.Run("setup check matches program resource", func(t *testing.T) {
		check := deriveStorageSetupCheck(buckets, "TEST", "project")
		if !check.Pass || check.Reason != "" || check.Details != "" {
			t.Fatalf("unexpected setup check: %+v", check)
		}
	})
	t.Run("setup check reports missing scope", func(t *testing.T) {
		check := deriveStorageSetupCheck(buckets, "TEST", "missing")
		if check.Pass || check.Reason != "missing_storage_scope" || check.Details == "" {
			t.Fatalf("unexpected setup check: %+v", check)
		}
	})
	t.Run("integration check matches organization resource", func(t *testing.T) {
		check := deriveStorageIntegrationCheck(map[string]StorageBucket{
			"bucket": {Resources: []string{"/organization/TEST/project/project"}},
		}, nil, "TEST", "project")
		if !check.Pass {
			t.Fatalf("unexpected integration check: %+v", check)
		}
	})
	t.Run("integration check preserves bucket errors", func(t *testing.T) {
		check := deriveStorageIntegrationCheck(nil, errors.New("syfon unavailable"), "TEST", "project")
		if check.Pass || check.Details != "syfon unavailable" {
			t.Fatalf("unexpected integration check: %+v", check)
		}
	})
}

func TestStorageConfigFromIntent(t *testing.T) {
	if got := storageConfigFromIntent(nil); got != (StorageConfig{}) {
		t.Fatalf("storageConfigFromIntent(nil) = %+v, want zero value", got)
	}
	intent := &CalyprProjectStorageIntent{
		Bucket:              " bucket ",
		Provider:            " s3 ",
		Endpoint:            " https://s3.example.org ",
		Region:              " us-west-2 ",
		AccessKey:           " access ",
		SecretKey:           " secret ",
		Organization:        " TEST ",
		ProjectID:           " project ",
		Path:                " data ",
		PathPrefix:          " prefix ",
		OrganizationSubPath: " org ",
		ProjectSubPath:      " proj ",
	}
	got := storageConfigFromIntent(intent)
	want := StorageConfig{
		Bucket:              "bucket",
		Provider:            "s3",
		Endpoint:            "https://s3.example.org",
		Region:              "us-west-2",
		AccessKey:           "access",
		SecretKey:           "secret",
		Organization:        "TEST",
		ProjectID:           "project",
		Path:                "data",
		PathPrefix:          "prefix",
		OrganizationSubPath: "org",
		ProjectSubPath:      "proj",
	}
	if got != want {
		t.Fatalf("storageConfigFromIntent() = %+v, want %+v", got, want)
	}
}

func TestFenceIssuerURLs(t *testing.T) {
	token := unsignedJWT(t, jwt.MapClaims{"iss": "https://fence.example.org/user"})
	baseURL, err := access.FenceIssuerBaseURL("Bearer " + token)
	if err != nil {
		t.Fatalf("FenceIssuerBaseURL() error = %v", err)
	}
	if baseURL != "https://fence.example.org" {
		t.Fatalf("FenceIssuerBaseURL() = %q", baseURL)
	}
	descendantURL, err := arboristOwnedDescendantURL("Bearer " + token)
	if err != nil {
		t.Fatalf("arboristOwnedDescendantURL() error = %v", err)
	}
	if descendantURL != "https://fence.example.org/authz/ownership/descendant" {
		t.Fatalf("arboristOwnedDescendantURL() = %q", descendantURL)
	}
	resourceURL, err := arboristOwnershipResourceURL("Bearer "+token, "/programs/TEST/projects/a b")
	if err != nil {
		t.Fatalf("arboristOwnershipResourceURL() error = %v", err)
	}
	parsed, err := url.Parse(resourceURL)
	if err != nil {
		t.Fatalf("parse resource URL: %v", err)
	}
	if parsed.Query().Get("resource_path") != "/programs/TEST/projects/a b" {
		t.Fatalf("resource_path = %q", parsed.Query().Get("resource_path"))
	}

	if _, err := access.FenceIssuerBaseURL(""); err == nil {
		t.Fatal("expected empty authorization header to fail")
	}
	if _, err := access.FenceIssuerBaseURL("Bearer not-a-jwt"); err == nil {
		t.Fatal("expected malformed token to fail")
	}
	missingIssuer := unsignedJWT(t, jwt.MapClaims{"sub": "someone"})
	if _, err := access.FenceIssuerBaseURL("Bearer " + missingIssuer); err == nil {
		t.Fatal("expected token without issuer to fail")
	}
	untrusted := unsignedJWT(t, jwt.MapClaims{"iss": "https://attacker.example"})
	if _, err := access.FenceIssuerBaseURL("Bearer " + untrusted); err == nil {
		t.Fatal("expected untrusted issuer to fail")
	}
}

func TestInstallationRepositoryHelpers(t *testing.T) {
	repositories := []domain.GitHubInstallationRepository{
		{FullName: "Owner/Repo", CloneURL: "https://github.com/Owner/Repo.git"},
		{FullName: "Owner/HTML", HTMLURL: "https://github.com/Owner/HTML"},
		{FullName: "Owner/Bare"},
	}
	t.Run("normalizes matching repository", func(t *testing.T) {
		identity, found, err := NormalizeInstallationRepository("owner/repo", repositories, "TEST", "project")
		if err != nil || !found {
			t.Fatalf("NormalizeInstallationRepository() = %+v, %v, %v", identity, found, err)
		}
		if identity.Host != "github.com" || identity.Owner != "Owner" || identity.Repo != "Repo" {
			t.Fatalf("unexpected identity: %+v", identity)
		}
	})
	t.Run("falls back to html url", func(t *testing.T) {
		identity, found, err := NormalizeInstallationRepository("owner/html", repositories, "TEST", "project")
		if err != nil || !found || identity.Repo != "HTML" {
			t.Fatalf("NormalizeInstallationRepository() = %+v, %v, %v", identity, found, err)
		}
	})
	t.Run("falls back to full name", func(t *testing.T) {
		identity, found, err := NormalizeInstallationRepository("owner/bare", repositories, "TEST", "project")
		if err != nil || !found || identity.Repo != "Bare" {
			t.Fatalf("NormalizeInstallationRepository() = %+v, %v, %v", identity, found, err)
		}
	})
	t.Run("unknown repository is not found", func(t *testing.T) {
		_, found, err := NormalizeInstallationRepository("owner/missing", repositories, "TEST", "project")
		if err != nil || found {
			t.Fatalf("NormalizeInstallationRepository() found = %v, err = %v", found, err)
		}
	})

	t.Run("parses bare repository names", func(t *testing.T) {
		identity, err := ParseRequestedRepositoryIdentity(" owner/repo ")
		if err != nil {
			t.Fatalf("ParseRequestedRepositoryIdentity() error = %v", err)
		}
		if identity.Host != "github.com" || identity.Owner != "owner" || identity.Repo != "repo" {
			t.Fatalf("unexpected identity: %+v", identity)
		}
	})
}

func TestInstallationRedirectURLs(t *testing.T) {
	tests := []struct {
		name        string
		redirectURL string
		targetID    int64
		repoID      int64
		want        string
	}{
		{
			name:        "new installation",
			redirectURL: "https://github.com/apps/gecko/installations/new",
			targetID:    7,
			repoID:      9,
			want:        "https://github.com/apps/gecko/installations/new/permissions?suggested_target_id=7&repository_ids[]=9",
		},
		{
			name:        "select target with existing query",
			redirectURL: "https://github.com/apps/gecko/installations/select_target?state=abc",
			targetID:    7,
			repoID:      9,
			want:        "https://github.com/apps/gecko/installations/new/permissions?state=abc&suggested_target_id=7&repository_ids[]=9",
		},
		{
			name:        "unrelated url is unchanged",
			redirectURL: "https://github.com/apps/gecko",
			want:        "https://github.com/apps/gecko",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DecorateInstallationRedirectURL(test.redirectURL, test.targetID, test.repoID); got != test.want {
				t.Fatalf("DecorateInstallationRedirectURL() = %q, want %q", got, test.want)
			}
		})
	}

	if got := AppInstallationRedirectURL("https://github.com/apps/gecko/installations/new", 42); got != "https://github.com/apps/gecko/installations/42" {
		t.Fatalf("AppInstallationRedirectURL() = %q", got)
	}
	if got := AppInstallationRedirectURL("https://github.com/apps/gecko/installations/new", 0); got != "" {
		t.Fatalf("AppInstallationRedirectURL() with zero id = %q, want empty", got)
	}
	if got := AppInstallationRedirectURL("https://github.com/owner/repo", 42); got != "" {
		t.Fatalf("AppInstallationRedirectURL() with non-app path = %q, want empty", got)
	}
}

func TestIntegrationErrors(t *testing.T) {
	statusErr := &domain.HTTPStatusError{StatusCode: http.StatusTooManyRequests, Code: "rate_limited", Message: "slow down"}
	err := integrationError(statusErr, map[string]any{"organization": "TEST"}, "github request failed")
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if domainErr.Kind != domain.ErrorKindIntegration || domainErr.StatusCode != http.StatusTooManyRequests || domainErr.Type != apierror.Type("rate_limited") {
		t.Fatalf("unexpected domain error: %+v", domainErr)
	}

	err = integrationError(errors.New("connection refused"), map[string]any{"organization": "TEST"}, "github request failed")
	if !errors.As(err, &domainErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if domainErr.StatusCode != http.StatusBadGateway || domainErr.Type != apierror.Type("integration_error") {
		t.Fatalf("unexpected fallback domain error: %+v", domainErr)
	}

	err = projectRepositoryError("TEST/project", "save project", errors.New("database down"))
	if !errors.As(err, &domainErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if domainErr.Kind != domain.ErrorKindDatabase || domainErr.StatusCode != http.StatusInternalServerError || domainErr.Type != apierror.TypeDatabaseError {
		t.Fatalf("unexpected repository error: %+v", domainErr)
	}
	if domainErr.Details["project_id"] != "TEST/project" {
		t.Fatalf("unexpected error details: %+v", domainErr.Details)
	}
}

func TestFromID(t *testing.T) {
	tests := []struct {
		input string
		want  Project
		ok    bool
	}{
		{input: "TEST/project", want: Project{ID: "TEST/project", Organization: "TEST", Name: "project"}, ok: true},
		{input: "TEST", ok: false},
		{input: "/project", ok: false},
		{input: "TEST/", ok: false},
		{input: "TEST/project/extra", ok: false},
	}
	for _, test := range tests {
		got, ok := FromID(test.input)
		if ok != test.ok {
			t.Fatalf("FromID(%q) ok = %v, want %v", test.input, ok, test.ok)
		}
		if ok && got != test.want {
			t.Fatalf("FromID(%q) = %+v, want %+v", test.input, got, test.want)
		}
	}
}

func unsignedJWT(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign test JWT: %v", err)
	}
	return token
}
