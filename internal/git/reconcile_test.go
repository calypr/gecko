package git

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/jmoiron/sqlx"
)

func TestBuildOrganizationStatusTreatsSetupOnlyProjectAsAwaitingGitHubConnect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

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

	service := &ReconcileService{db: sqlx.NewDb(db, "sqlmock")}
	organizationStates := map[string]geckodb.GitOrganizationState{
		"TEST": {
			Organization:        "TEST",
			Installed:           true,
			RepositorySelection: nullableString("selected"),
			UpdatedAt:           time.Now(),
		},
	}
	buckets := map[string]StorageBucket{
		"bucket-a": {
			Bucket:    "bucket-a",
			Resources: []string{"/programs/TEST/projects/proj-a"},
		},
	}

	status, err := service.BuildOrganizationStatus(
		context.Background(),
		"TEST",
		[]string{"TEST/proj-a"},
		map[string]geckodb.GitProjectState{},
		organizationStates,
		[]string{"/programs/TEST/projects/proj-a"},
		buckets,
		nil,
	)
	if err != nil {
		t.Fatalf("build organization status: %v", err)
	}
	if len(status.Projects) != 1 {
		t.Fatalf("expected one project, got %d", len(status.Projects))
	}
	project := status.Projects[0]
	if project.WorkflowStage != GitWorkflowStageAwaitingGitHubConnect {
		t.Fatalf("expected workflow stage %q, got %q", GitWorkflowStageAwaitingGitHubConnect, project.WorkflowStage)
	}
	if project.Integrations.GitHub.Reason != GitWorkflowStageAwaitingGitHubConnect {
		t.Fatalf("expected github reason %q, got %q", GitWorkflowStageAwaitingGitHubConnect, project.Integrations.GitHub.Reason)
	}
	if project.Integrations.GitHub.Pass {
		t.Fatal("expected github integration to be incomplete")
	}
	if !project.Integrations.Storage.Pass {
		t.Fatal("expected storage integration to pass")
	}
	if project.Configured {
		t.Fatal("expected setup-only project to remain unconfigured")
	}
	if project.Installation.Installed {
		t.Fatal("expected project installation to remain unbound before connect")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}
