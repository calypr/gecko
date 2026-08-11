package config

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/loom"
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

func TestExplorerOverlayRoundTripAndResolutionPreservesAllCustomization(t *testing.T) {
	sortable := true
	overlay := appconfig.ExplorerOverlay{
		SchemaVersion: 2,
		SharedFilters: appconfig.OverlaySharedFilters{Defined: map[string][]appconfig.OverlayFilterPair{"project": {{Index: "DocumentReference", SemanticPath: "DocumentReference.project"}}}},
		FileActions:   appconfig.OverlayFileActions{Extensions: map[string][]string{"tiff": {"download"}}, Actions: map[string]string{"download": "/download"}},
		ExplorerConfig: []appconfig.OverlayTab{{
			DataType: "DocumentReference", TabTitle: "Files", IncludeUnconfiguredFields: &sortable,
			GuppyConfig: appconfig.OverlayGuppyConfig{
				NodeCountTitle:           "File count",
				FieldMapping:             []appconfig.OverlayGuppyFieldMapping{{SemanticPath: "DocumentReference.id", Name: "ID"}},
				AccessibleFieldCheckList: []string{"DocumentReference.project"}, AccessibleValidationField: "DocumentReference.id",
				ManifestMapping: appconfig.OverlayManifestMapping{ResourceIndexType: "file", ResourceIDSemanticPath: "DocumentReference.id"},
			},
			Charts:     map[string]appconfig.OverlayChart{"DocumentReference.size": {ChartType: "histogram", Title: "Size"}},
			Filters:    appconfig.OverlayFiltersConfig{Tabs: []appconfig.OverlayFilterTab{{Title: "Basic", Fields: []appconfig.OverlayFilterField{{SemanticPath: "DocumentReference.project"}}}}},
			Table:      &appconfig.OverlayTableConfig{Enabled: &sortable, Fields: []string{"DocumentReference.id"}, Columns: map[string]appconfig.OverlayTableColumn{"DocumentReference.size": {Title: "Size", CellRenderFunction: "renderSize", Sortable: true}}, DetailsConfig: appconfig.OverlayTableDetailsConfig{IDSemanticPath: "DocumentReference.id"}},
			Dropdowns:  map[string]any{"group": []string{"project"}},
			Buttons:    []appconfig.OverlayButton{{Enabled: true, Type: "manifest", Action: "download", Title: "Manifest", ActionArgs: appconfig.OverlayButtonActionArgs{ResourceIndexType: "file", ResourceIDSemanticPath: "DocumentReference.id", FileSemanticPaths: []string{"DocumentReference.url"}}}},
			PreFilters: map[string]any{"DocumentReference.project": []any{"P1"}},
			Fields:     []appconfig.OverlayField{{SemanticPath: "DocumentReference.url", Renderer: "renderURL", Params: map[string]any{"target": "_blank"}}},
		}},
	}
	data, err := json.Marshal(overlay)
	if err != nil {
		t.Fatalf("marshal overlay: %v", err)
	}
	var roundTrip appconfig.ExplorerOverlay
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("unmarshal overlay: %v", err)
	}
	if roundTrip.ExplorerConfig[0].GuppyConfig.FieldMapping[0].SemanticPath != "DocumentReference.id" {
		t.Fatalf("semantic mapping did not round-trip: %s", string(data))
	}
	if err := validateOverlay(roundTrip); err != nil {
		t.Fatalf("round-trip overlay invalid: %v", err)
	}

	exec := loom.Execution{ID: "execution-1", Outputs: []loom.Output{{DataType: "DocumentReference", Columns: []loom.Column{
		{SemanticPath: "DocumentReference.id", Name: "doc_id", Filterable: true, Sortable: true},
		{SemanticPath: "DocumentReference.project", Name: "project_id", Filterable: true, Sortable: true},
		{SemanticPath: "DocumentReference.size", Name: "file_size", Aggregatable: true, Sortable: true},
		{SemanticPath: "DocumentReference.url", Name: "file_url", Filterable: true, Sortable: true},
	}}},
	}
	resolved := resolveOverlayConfig(roundTrip, exec)
	if got := resolved.SharedFilters.SharedFilter["project"][0].Field; got != "project_id" {
		t.Fatalf("shared filter field = %q", got)
	}
	item := resolved.ExplorerConfig[0]
	if item.GuppyConfig.FieldMapping[0].Field != "doc_id" || item.GuppyConfig.ManifestMapping.ResourceIdField != "doc_id" {
		t.Fatalf("guppy semantic refs not resolved: %+v", item.GuppyConfig)
	}
	if item.Charts["file_size"].ChartType != "histogram" {
		t.Fatalf("chart not resolved: %+v", item.Charts)
	}
	if item.Filters.Tabs[0].Fields[0] != "project_id" {
		t.Fatalf("filter not resolved: %+v", item.Filters)
	}
	if item.Table.Columns["file_size"].CellRenderFunction != "renderSize" {
		t.Fatalf("table column not preserved: %+v", item.Table.Columns)
	}
	if item.Table.Columns["file_url"].CellRenderFunction != "renderURL" {
		t.Fatalf("field renderer not preserved: %+v", item.Table.Columns)
	}
	if _, ok := item.Dropdowns["group"]; !ok || item.PreFilters["project_id"] == nil {
		t.Fatalf("dropdown/prefilter not preserved: %+v", item)
	}
	if item.Buttons[0].ActionArgs.FileFields[0] != "file_url" {
		t.Fatalf("button semantic ref not resolved: %+v", item.Buttons[0])
	}
	if resolved.FileActions.Actions["download"] != "/download" {
		t.Fatalf("file actions not preserved: %+v", resolved.FileActions)
	}
}

func TestValidateOverlayRequiresReasonForOptionalOmission(t *testing.T) {
	o := appconfig.ExplorerOverlay{SchemaVersion: 2, ExplorerConfig: []appconfig.OverlayTab{{DataType: "DocumentReference", Fields: []appconfig.OverlayField{{SemanticPath: "DocumentReference.foo", MissingPolicy: "OMIT"}}}}}
	if err := validateOverlay(o); err == nil {
		t.Fatal("expected omission reason validation error")
	}
	o.ExplorerConfig[0].Fields[0].OmissionReason = "not present in all projects"
	if err := validateOverlay(o); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestActiveDatasetMatchesPublishingRevisionExactly(t *testing.T) {
	r := &geckodb.ExplorerRevision{ProjectID: "project-1", TargetExecutionID: sql.NullString{String: "exec-1", Valid: true}, TargetGeneration: sql.NullString{String: "gen-2", Valid: true}}
	if !activeDatasetMatchesRevision(loom.ActiveDataset{ID: "exec-1:Patient", Revision: "exec-1", ProjectID: "project-1", DatasetGeneration: "gen-2", State: "READY"}, r) {
		t.Fatal("expected exact active execution to match")
	}
	if activeDatasetMatchesRevision(loom.ActiveDataset{ID: "exec-2:Patient", Revision: "exec-2", ProjectID: "project-1", DatasetGeneration: "gen-2", State: "READY"}, r) {
		t.Fatal("different execution must not reconcile")
	}
	if activeDatasetMatchesRevision(loom.ActiveDataset{ID: "exec-1:Patient", Revision: "exec-1", ProjectID: "project-1", DatasetGeneration: "gen-1", State: "READY"}, r) {
		t.Fatal("different generation must not reconcile")
	}
}

func TestVerifyActiveRevisionRejectsOutOfBandGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"projectDataframeDatasets": []any{map[string]any{"id": "exec-2:Patient", "revision": "exec-2", "projectId": "project-1", "datasetGeneration": "gen-new", "state": "READY", "columns": []any{}}}}})
	}))
	defer server.Close()
	t.Setenv("LOOM_BASE_URL", server.URL)
	r := &geckodb.ExplorerRevision{ID: "rev-1", ProjectID: "project-1", TargetExecutionID: sql.NullString{String: "exec-1", Valid: true}, TargetGeneration: sql.NullString{String: "gen-old", Valid: true}}
	err := (&Handler{}).verifyActiveRevision(context.Background(), r, "Bearer test")
	if _, ok := err.(*activeGenerationMismatchError); !ok {
		t.Fatalf("expected ACTIVE_GENERATION_MISMATCH, got %T: %v", err, err)
	}
}

func TestIncompatibleRevisionErrorUsesStableDiagnostic(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(ctx fiber.Ctx) error {
		return incompatibleRevisionError(ctx, &geckodb.ExplorerRevision{ID: "rev-1"}, "active generation changed")
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "ACTIVE_GENERATION_MISMATCH") || !strings.Contains(string(body), "INCOMPATIBLE") {
		t.Fatalf("body missing stable incompatibility diagnostics: %s", body)
	}
}

func TestActiveRevisionValidationRetryDoesNotDemoteRevision(t *testing.T) {
	database, mock, closeDB := activeRevisionDB(t)
	defer closeDB()
	handler := &Handler{db: database}
	app := fiber.New()
	app.Post("/:configId/revisions/:revisionId/validate", handler.handleExplorerRevisionValidate)

	req := httptest.NewRequest(http.MethodPost, "/project-1/revisions/rev-active/validate", strings.NewReader(`{"loomExecutionId":"exec-1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"VALID"`) {
		t.Fatalf("validation retry body = %s", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActiveRevisionPublishRetryIsIdempotent(t *testing.T) {
	database, mock, closeDB := activeRevisionDB(t)
	defer closeDB()
	handler := &Handler{db: database}
	app := fiber.New()
	app.Post("/:configId/revisions/:revisionId/publish", handler.handleExplorerRevisionPublish)

	req := httptest.NewRequest(http.MethodPost, "/project-1/revisions/rev-active/publish", strings.NewReader(`{"loomExecutionId":"exec-1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ACTIVE"`) {
		t.Fatalf("publish retry body = %s", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func activeRevisionDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	rawDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	columns := []string{"id", "config_id", "project_id", "parent_revision_id", "digest", "overlay", "status", "target_execution_id", "target_generation", "target_schema_digest", "diagnostics", "created_by", "created_at", "updated_at"}
	diagnostics := []byte(`{"revisionId":"rev-active","status":"VALID","errors":[],"warnings":[],"acknowledgedOmissions":[]}`)
	rows := sqlmock.NewRows(columns).AddRow("rev-active", "project-1", "project-1", "rev-parent", "digest", []byte(`{"schemaVersion":2}`), statusActive, "exec-1", "generation-1", "schema-1", diagnostics, nil, now, now)
	mock.ExpectQuery("SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=\\$1 AND id=\\$2").
		WithArgs("project-1", "rev-active").
		WillReturnRows(rows)
	return sqlx.NewDb(rawDB, "sqlmock"), mock, func() { _ = rawDB.Close() }
}

func TestReconcilePublishingPromotesAfterLoomActivation(t *testing.T) {
	loomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql/graph" {
			t.Fatalf("unexpected Loom path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"projectDataframeDatasets": []any{map[string]any{"id": "exec-1:Patient", "revision": "exec-1", "projectId": "project-1", "datasetGeneration": "gen-2", "state": "READY", "columns": []any{}}}}})
	}))
	defer loomServer.Close()
	t.Setenv("LOOM_BASE_URL", loomServer.URL)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	now := time.Now()
	cols := []string{"id", "config_id", "project_id", "parent_revision_id", "digest", "overlay", "status", "target_execution_id", "target_generation", "target_schema_digest", "diagnostics", "created_by", "created_at", "updated_at"}
	row := sqlmock.NewRows(cols).AddRow("rev-1", "project-1", "project-1", nil, "digest", []byte(`{"schemaVersion":2}`), "PUBLISHING", "exec-1", "gen-2", "schema", []byte(`{"status":"VALID"}`), nil, now, now)
	mock.ExpectQuery("SELECT id,config_id,project_id,parent_revision_id,digest,overlay,status,target_execution_id,target_generation,target_schema_digest,diagnostics,created_by,created_at,updated_at FROM config_schema.explorer_config_revision WHERE config_id=\\$1 AND status=\\$2").WithArgs("project-1", "PUBLISHING").WillReturnRows(row)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM config_schema.explorer_config_revision WHERE config_id=\\$1 AND status='ACTIVE' FOR UPDATE").WithArgs("project-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("UPDATE config_schema.explorer_config_revision SET status='SUPERSEDED'").WithArgs("project-1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE config_schema.explorer_config_revision SET status='ACTIVE'").WithArgs("rev-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	h := &Handler{db: sqlxDB}
	got, err := h.reconcilePublishing(context.Background(), "project-1", "Bearer test")
	if err != nil || got == nil || got.Status != statusActive {
		t.Fatalf("reconcile result = %#v, err=%v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateOverlayTreatsMissingRequiredFieldAsInvalid(t *testing.T) {
	o := appconfig.ExplorerOverlay{SchemaVersion: 2, ExplorerConfig: []appconfig.OverlayTab{{DataType: "DocumentReference", Fields: []appconfig.OverlayField{{SemanticPath: "DocumentReference.foo"}}}}}
	v := validateOverlayAgainstExecution(o, loom.Execution{ID: "e1", Outputs: []loom.Output{{Columns: []loom.Column{{SemanticPath: "DocumentReference.bar", Filterable: true}}}}})
	if v.Status != "INVALID" || len(v.Errors) != 1 || v.Errors[0].Code != "FIELD_NOT_FOUND" {
		t.Fatalf("unexpected result: %#v", v)
	}
}

func TestValidateOverlayAcknowledgesExplicitOptionalOmission(t *testing.T) {
	o := appconfig.ExplorerOverlay{SchemaVersion: 2, ExplorerConfig: []appconfig.OverlayTab{{DataType: "DocumentReference", Fields: []appconfig.OverlayField{{SemanticPath: "DocumentReference.foo", MissingPolicy: "OMIT", OmissionReason: "optional extension"}}}}}
	v := validateOverlayAgainstExecution(o, loom.Execution{ID: "e1", Outputs: []loom.Output{{Name: "DocumentReference", DataType: "DocumentReference"}}})
	if v.Status != "VALID_WITH_OMISSIONS" || len(v.Errors) != 0 || len(v.AcknowledgedOmissions) != 1 {
		t.Fatalf("unexpected result: %#v", v)
	}
}

func TestValidateOverlayChecksFieldCapabilities(t *testing.T) {
	sortable := true
	o := appconfig.ExplorerOverlay{SchemaVersion: 2, ExplorerConfig: []appconfig.OverlayTab{{DataType: "Patient", Fields: []appconfig.OverlayField{{SemanticPath: "Patient.birthDate", Chart: &appconfig.OverlayChart{ChartType: "histogram"}, Filters: []appconfig.OverlayFilter{{Type: "range"}}, Table: &appconfig.OverlayTable{Sortable: &sortable}}}}}}
	v := validateOverlayAgainstExecution(o, loom.Execution{ID: "e1", Outputs: []loom.Output{{Columns: []loom.Column{{SemanticPath: "Patient.birthDate", Filterable: false, Sortable: false, Aggregatable: false}}}}})
	if v.Status != "INVALID" || len(v.Errors) != 3 {
		t.Fatalf("unexpected result: %#v", v)
	}
}
