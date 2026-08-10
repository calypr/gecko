package config

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/calypr/gecko/internal/loom"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const (
	statusDraft      = "DRAFT"
	statusValidated  = "VALIDATED"
	statusActive     = "ACTIVE"
	statusRejected   = "REJECTED"
	statusPublishing = "PUBLISHING"
)

type revisionRequest struct {
	ParentRevisionID  string                    `json:"parentRevisionId,omitempty"`
	Overlay           appconfig.ExplorerOverlay `json:"overlay"`
	TargetExecutionID string                    `json:"targetExecutionId,omitempty"`
}
type executionRequest struct {
	LoomExecutionID string `json:"loomExecutionId"`
}
type diagnostic struct {
	Code         string `json:"code"`
	DataType     string `json:"dataType,omitempty"`
	SemanticPath string `json:"semanticPath,omitempty"`
	ConfigPath   string `json:"configPath,omitempty"`
	Message      string `json:"message"`
}
type validationResponse struct {
	RevisionID            string       `json:"revisionId"`
	Status                string       `json:"status"`
	Errors                []diagnostic `json:"errors"`
	Warnings              []diagnostic `json:"warnings"`
	AcknowledgedOmissions []diagnostic `json:"acknowledgedOmissions"`
	TargetExecutionID     string       `json:"targetExecutionId,omitempty"`
	TargetGeneration      string       `json:"targetGeneration,omitempty"`
	TargetSchemaDigest    string       `json:"targetSchemaDigest,omitempty"`
}

func (handler *Handler) registerExplorerRevisionRoutes(group fiber.Router, authzHandler servermw.ResourceAccessHandler) {
	auth := servermw.ConfigAuth(handler.Logger, authzHandler)
	group.Post("/:configId/revisions", auth, handler.handleExplorerRevisionCreate)
	group.Get("/:configId/revisions", auth, handler.handleExplorerRevisionList)
	group.Get("/:configId/revisions/:revisionId", auth, handler.handleExplorerRevisionGet)
	group.Post("/:configId/revisions/:revisionId/validate", auth, handler.handleExplorerRevisionValidate)
	group.Post("/:configId/revisions/:revisionId/publish", auth, handler.handleExplorerRevisionPublish)
	group.Get("/:configId/authored", auth, handler.handleExplorerAuthored)
	group.Get("/:configId/resolved", auth, handler.handleExplorerResolved)
	group.Get("/:configId/status", auth, handler.handleExplorerStatus)
}

func (handler *Handler) handleExplorerRevisionCreate(ctx fiber.Ctx) error {
	var req revisionRequest
	if err := parseRevisionRequest(ctx.Body(), &req); err != nil {
		return revisionError(ctx, http.StatusBadRequest, err.Error(), map[string]any{"config_id": ctx.Params("configId")})
	}
	if err := validateOverlay(req.Overlay); err != nil {
		return revisionError(ctx, http.StatusBadRequest, err.Error(), nil)
	}
	if req.Overlay.ProjectID != "" && req.Overlay.ProjectID != ctx.Params("configId") {
		return revisionError(ctx, http.StatusBadRequest, "overlay projectId must match configId", nil)
	}
	if req.Overlay.ProjectID == "" {
		req.Overlay.ProjectID = ctx.Params("configId")
	}
	if req.Overlay.SchemaVersion == 0 {
		req.Overlay.SchemaVersion = 2
	}
	data, _ := json.Marshal(req.Overlay)
	digest := digestJSON(data)
	diagnostics, _ := json.Marshal(map[string]any{"errors": []diagnostic{}, "warnings": []diagnostic{}})
	r := &geckodb.ExplorerRevision{ID: uuid.NewString(), ConfigID: ctx.Params("configId"), ProjectID: req.Overlay.ProjectID, Digest: digest, Overlay: data, Status: statusDraft, Diagnostics: diagnostics}
	if req.ParentRevisionID != "" {
		r.ParentRevisionID = sql.NullString{String: req.ParentRevisionID, Valid: true}
	} else {
		// Automated publishers normally omit parentRevisionId. Capture the
		// current active head at draft creation so an ordinary second release is
		// optimistic-concurrency safe instead of conflicting after Loom moves.
		active, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"))
		if err != nil {
			return revisionError(ctx, http.StatusInternalServerError, "could not resolve active explorer revision", err)
		}
		if active != nil {
			r.ParentRevisionID = sql.NullString{String: active.ID, Valid: true}
		}
	}
	if req.TargetExecutionID != "" {
		r.TargetExecutionID = sql.NullString{String: req.TargetExecutionID, Valid: true}
	}
	if err := geckodb.InsertExplorerRevision(ctx.Context(), handler.db, r); err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not persist explorer revision", err)
	}
	return httputil.JSON(revisionJSON(r), http.StatusCreated).Write(ctx)
}

// parseRevisionRequest accepts the v2 envelope and, during migration, a legacy
// expanded explorer Config as the request body. The adapter deliberately keeps
// every configured physical field as an explicit semantic reference; validation
// against Loom remains strict and will reject ambiguous/missing references.
func parseRevisionRequest(body []byte, req *revisionRequest) error {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("invalid JSON request: %w", err)
	}
	if _, ok := envelope["overlay"]; ok {
		if e := httputil.ParseJSONBody(body, req, nil); e != nil {
			return errors.New(e.Error.Message)
		}
		return nil
	}
	var legacy appconfig.Config
	if e := httputil.ParseJSONBody(body, &legacy, nil); e != nil {
		return errors.New(e.Error.Message)
	}
	req.Overlay = legacyConfigToOverlay(legacy)
	return nil
}

func legacyConfigToOverlay(legacy appconfig.Config) appconfig.ExplorerOverlay {
	legacyCopy := legacy
	o := appconfig.ExplorerOverlay{SchemaVersion: 2, ExplorerConfig: make([]appconfig.OverlayTab, 0, len(legacy.ExplorerConfig)), LegacyConfig: &legacyCopy}
	for i, tab := range legacy.ExplorerConfig {
		out := appconfig.OverlayTab{TabID: fmt.Sprintf("legacy-%d", i), DataType: tab.GuppyConfig.DataType, TabTitle: tab.TabTitle}
		seen := map[string]bool{}
		add := func(path string, field appconfig.OverlayField) {
			if path == "" || seen[path] {
				return
			}
			field.SemanticPath = path
			out.Fields = append(out.Fields, field)
			seen[path] = true
		}
		for path, chart := range tab.Charts {
			add(path, appconfig.OverlayField{Chart: &appconfig.OverlayChart{ChartType: chart.ChartType, Title: chart.Title}})
		}
		for _, path := range tab.Table.Fields {
			add(path, appconfig.OverlayField{Table: &appconfig.OverlayTable{}})
		}
		for path := range tab.Table.Columns {
			add(path, appconfig.OverlayField{Table: &appconfig.OverlayTable{}})
		}
		for _, filterTab := range tab.Filters.Tabs {
			for _, path := range filterTab.Fields {
				add(path, appconfig.OverlayField{Filters: []appconfig.OverlayFilter{{Type: "filter", Label: filterTab.Title}}})
			}
		}
		for _, mapping := range tab.GuppyConfig.FieldMapping {
			add(mapping.Field, appconfig.OverlayField{})
		}
		for _, path := range tab.GuppyConfig.AccessibleFieldCheckList {
			add(path, appconfig.OverlayField{})
		}
		add(tab.GuppyConfig.AccessibleValidationField, appconfig.OverlayField{})
		manifest := tab.GuppyConfig.ManifestMapping
		for _, path := range []string{manifest.ResourceIdField, manifest.ReferenceIdFieldInResourceIndex, manifest.ReferenceIdFieldInDataIndex} {
			add(path, appconfig.OverlayField{})
		}
		for _, button := range tab.Buttons {
			for _, path := range button.ActionArgs.FileFields {
				add(path, appconfig.OverlayField{Download: &appconfig.OverlayDownload{}})
			}
			for _, path := range []string{button.ActionArgs.ResourceIdField, button.ActionArgs.ReferenceIdFieldInResourceIndex, button.ActionArgs.ReferenceIdFieldInDataIndex} {
				add(path, appconfig.OverlayField{})
			}
		}
		for path := range tab.PreFilters {
			add(path, appconfig.OverlayField{})
		}
		o.ExplorerConfig = append(o.ExplorerConfig, out)
	}
	for _, legs := range legacy.SharedFilters.SharedFilter {
		for _, leg := range legs {
			for i := range o.ExplorerConfig {
				if o.ExplorerConfig[i].DataType == leg.Index {
					o.ExplorerConfig[i].Fields = append(o.ExplorerConfig[i].Fields, appconfig.OverlayField{SemanticPath: leg.Field})
				}
			}
		}
	}
	return o
}

func (handler *Handler) handleExplorerRevisionList(ctx fiber.Ctx) error {
	rows, err := geckodb.ListExplorerRevisions(ctx.Context(), handler.db, ctx.Params("configId"))
	if err != nil {
		return revisionError(ctx, 500, "could not list explorer revisions", err)
	}
	out := make([]any, 0, len(rows))
	for i := range rows {
		out = append(out, revisionJSON(&rows[i]))
	}
	return httputil.JSON(out, 200).Write(ctx)
}
func (handler *Handler) handleExplorerRevisionGet(ctx fiber.Ctx) error {
	r, err := geckodb.GetExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"), ctx.Params("revisionId"))
	if err != nil {
		return revisionError(ctx, 500, "could not load explorer revision", err)
	}
	if r == nil {
		return revisionError(ctx, 404, "explorer revision not found", nil)
	}
	return httputil.JSON(revisionJSON(r), 200).Write(ctx)
}

func (handler *Handler) handleExplorerRevisionValidate(ctx fiber.Ctx) error {
	r, err := geckodb.GetExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"), ctx.Params("revisionId"))
	if err != nil {
		return revisionError(ctx, 500, "could not load explorer revision", err)
	}
	if r == nil {
		return revisionError(ctx, 404, "explorer revision not found", nil)
	}
	var req executionRequest
	if e := httputil.ParseJSONBody(ctx.Body(), &req, nil); e != nil {
		return e.Write(ctx)
	}
	if strings.TrimSpace(req.LoomExecutionID) == "" {
		return revisionError(ctx, 400, "loomExecutionId is required", nil)
	}
	exec, err := handler.loom().GetExecution(ctx.Context(), req.LoomExecutionID, ctx.Get("Authorization"))
	if err != nil {
		return revisionValidationUnavailable(ctx, r, err)
	}
	if exec.ProjectID != "" && exec.ProjectID != r.ProjectID {
		result := validationResponse{RevisionID: r.ID, Status: "INVALID", TargetExecutionID: exec.ID, Errors: []diagnostic{{Code: "PROJECT_MISMATCH", Message: "candidate Loom execution belongs to a different project"}}}
		r.Status = statusRejected
		r.Diagnostics, _ = json.Marshal(result)
		_ = geckodb.UpdateExplorerRevision(ctx.Context(), handler.db, r)
		return httputil.JSON(result, http.StatusUnprocessableEntity).Write(ctx)
	}
	overlay := appconfig.ExplorerOverlay{}
	if err := json.Unmarshal(r.Overlay, &overlay); err != nil {
		return revisionError(ctx, 500, "stored overlay is invalid", err)
	}
	result := validateOverlayAgainstExecution(overlay, exec)
	result.RevisionID = r.ID
	result.TargetExecutionID = exec.ID
	result.TargetGeneration = exec.Generation
	result.TargetSchemaDigest = exec.SchemaDigest
	r.TargetExecutionID = sql.NullString{String: exec.ID, Valid: true}
	r.TargetGeneration = sql.NullString{String: exec.Generation, Valid: exec.Generation != ""}
	r.TargetSchemaDigest = sql.NullString{String: exec.SchemaDigest, Valid: exec.SchemaDigest != ""}
	r.Diagnostics, _ = json.Marshal(result)
	if result.Status == "INVALID" {
		r.Status = statusRejected
	} else {
		r.Status = statusValidated
	}
	if err := geckodb.UpdateExplorerRevision(ctx.Context(), handler.db, r); err != nil {
		return revisionError(ctx, 500, "could not save validation result", err)
	}
	code := 200
	if result.Status == "INVALID" {
		code = http.StatusUnprocessableEntity
	}
	return httputil.JSON(result, code).Write(ctx)
}

func (handler *Handler) handleExplorerRevisionPublish(ctx fiber.Ctx) error {
	r, err := geckodb.GetExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"), ctx.Params("revisionId"))
	if err != nil {
		return revisionError(ctx, 500, "could not load explorer revision", err)
	}
	if r == nil {
		return revisionError(ctx, 404, "explorer revision not found", nil)
	}
	var req executionRequest
	if e := httputil.ParseJSONBody(ctx.Body(), &req, nil); e != nil {
		return e.Write(ctx)
	}
	if req.LoomExecutionID == "" {
		return revisionError(ctx, 400, "loomExecutionId is required", nil)
	}
	if r.Status != statusValidated {
		return revisionError(ctx, http.StatusConflict, "revision must validate successfully before publish", map[string]any{"status": r.Status})
	}
	exec, err := handler.loom().GetExecution(ctx.Context(), req.LoomExecutionID, ctx.Get("Authorization"))
	if err != nil {
		return revisionError(ctx, http.StatusConflict, "candidate Loom execution is unavailable", err)
	}
	if r.TargetExecutionID.Valid && r.TargetExecutionID.String != exec.ID {
		return revisionError(ctx, http.StatusConflict, "target execution changed", map[string]any{"code": "TARGET_EXECUTION_CHANGED"})
	}
	if r.TargetSchemaDigest.Valid && exec.SchemaDigest != "" && r.TargetSchemaDigest.String != exec.SchemaDigest {
		return revisionError(ctx, http.StatusConflict, "candidate schema changed", map[string]any{"code": "TARGET_SCHEMA_CHANGED"})
	}
	if exec.State != "" && exec.State != "READY" {
		return revisionError(ctx, http.StatusConflict, "candidate Loom execution is not READY", map[string]any{"state": exec.State})
	}
	if exec.Generation == "" {
		return revisionError(ctx, http.StatusConflict, "candidate Loom execution has no dataset generation", map[string]any{"code": "GENERATION_MISSING"})
	}
	// Detect a stale editor/ETL draft before crossing the external Loom
	// activation boundary. ActivateExplorerRevision repeats this check inside
	// its transaction after Loom returns.
	active, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, r.ConfigID)
	if err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not verify publication parent", err)
	}
	expectedParent := ""
	if r.ParentRevisionID.Valid {
		expectedParent = r.ParentRevisionID.String
	}
	actualParent := ""
	if active != nil {
		actualParent = active.ID
	}
	if actualParent != expectedParent {
		return revisionError(ctx, http.StatusConflict, "publication parent revision conflict", map[string]any{"code": "PARENT_REVISION_CONFLICT", "expected": expectedParent, "actual": actualParent})
	}
	// Persist PUBLISHING before crossing the Loom activation boundary. A
	// reconciliation worker can safely identify this candidate after a crash.
	r.Status = statusPublishing
	if err := geckodb.UpdateExplorerRevision(ctx.Context(), handler.db, r); err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not begin publication", err)
	}
	if err := handler.loom().ActivateGeneration(ctx.Context(), r.ProjectID, exec.Generation, exec.ID, ctx.Get("Authorization")); err != nil {
		r.Status = statusValidated
		_ = geckodb.UpdateExplorerRevision(ctx.Context(), handler.db, r)
		return revisionError(ctx, http.StatusConflict, "Loom generation activation failed", map[string]any{"error": err.Error()})
	}
	if err := geckodb.ActivateExplorerRevision(ctx.Context(), handler.db, r, expectedParent); err != nil {
		r.Status = statusValidated
		_ = geckodb.UpdateExplorerRevision(ctx.Context(), handler.db, r)
		return revisionError(ctx, http.StatusConflict, "publication parent revision conflict", map[string]any{"code": "PARENT_REVISION_CONFLICT", "error": err.Error()})
	}
	r.Status = statusActive
	return httputil.JSON(revisionJSON(r), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleExplorerAuthored(ctx fiber.Ctx) error {
	if _, err := handler.reconcilePublishing(ctx.Context(), ctx.Params("configId"), ctx.Get("Authorization")); err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not reconcile explorer publication", err)
	}
	r, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"))
	if err != nil {
		return revisionError(ctx, 500, "could not load active explorer revision", err)
	}
	if r == nil {
		return httputil.JSON(map[string]any{"status": "UNAVAILABLE", "configId": ctx.Params("configId")}, 200).Write(ctx)
	}
	if err := handler.verifyActiveRevision(ctx.Context(), r, ctx.Get("Authorization")); err != nil {
		if mismatch, ok := err.(*activeGenerationMismatchError); ok {
			return incompatibleRevisionError(ctx, r, mismatch.Error())
		}
		return revisionError(ctx, http.StatusServiceUnavailable, "could not verify active Loom generation", err)
	}
	return httputil.JSON(map[string]any{"status": r.Status, "revisionId": r.ID, "digest": r.Digest, "projectId": r.ProjectID, "overlay": json.RawMessage(r.Overlay)}, 200).Write(ctx)
}
func (handler *Handler) handleExplorerResolved(ctx fiber.Ctx) error {
	if _, err := handler.reconcilePublishing(ctx.Context(), ctx.Params("configId"), ctx.Get("Authorization")); err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not reconcile explorer publication", err)
	}
	r, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"))
	if err != nil {
		return revisionError(ctx, 500, "could not resolve explorer revision", err)
	}
	if r == nil {
		return httputil.JSON(map[string]any{"status": "UNAVAILABLE", "configId": ctx.Params("configId")}, 200).Write(ctx)
	}
	if err := handler.verifyActiveRevision(ctx.Context(), r, ctx.Get("Authorization")); err != nil {
		if mismatch, ok := err.(*activeGenerationMismatchError); ok {
			return incompatibleRevisionError(ctx, r, mismatch.Error())
		}
		return revisionError(ctx, http.StatusServiceUnavailable, "could not verify active Loom generation", err)
	}
	if !r.TargetExecutionID.Valid {
		return httputil.JSON(map[string]any{"status": "UNAVAILABLE", "configRevisionId": r.ID, "configId": r.ConfigID}, 200).Write(ctx)
	}
	exec, err := handler.loom().GetExecution(ctx.Context(), r.TargetExecutionID.String, ctx.Get("Authorization"))
	if err != nil {
		return httputil.JSON(map[string]any{"status": "UNAVAILABLE", "configRevisionId": r.ID, "configId": r.ConfigID, "error": err.Error()}, 200).Write(ctx)
	}
	var diagnostics validationResponse
	_ = json.Unmarshal(r.Diagnostics, &diagnostics)
	resolvedStatus := "VALID"
	if diagnostics.Status != "" {
		resolvedStatus = diagnostics.Status
	}
	var overlay appconfig.ExplorerOverlay
	if err := json.Unmarshal(r.Overlay, &overlay); err != nil {
		return revisionError(ctx, 500, "stored overlay is invalid", err)
	}
	resolved := resolveOverlayConfig(overlay, exec)
	return httputil.JSON(map[string]any{
		"status": resolvedStatus, "configRevisionId": r.ID, "projectId": r.ProjectID,
		"datasetGeneration": r.TargetGeneration.String, "config": resolved,
		"errors": diagnostics.Errors, "warnings": diagnostics.Warnings,
		"acknowledgedOmissions": diagnostics.AcknowledgedOmissions,
	}, 200).Write(ctx)
}

// handlePublishedExplorerConfigGET keeps the legacy expanded-config endpoint
// aligned with the active revision. Existing frontends still call
// GET /config/explorer/:configId; once a revision is active, returning the
// mutable legacy row would expose a different configuration than /resolved.
func (handler *Handler) handlePublishedExplorerConfigGET(ctx fiber.Ctx, configID string) (bool, error) {
	if _, err := handler.reconcilePublishing(ctx.Context(), configID, ctx.Get("Authorization")); err != nil {
		return true, revisionError(ctx, http.StatusInternalServerError, "could not reconcile explorer publication", err)
	}
	revision, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, configID)
	if err != nil {
		return true, revisionError(ctx, http.StatusInternalServerError, "could not load active explorer revision", err)
	}
	if revision == nil {
		return false, nil
	}
	if err := handler.verifyActiveRevision(ctx.Context(), revision, ctx.Get("Authorization")); err != nil {
		if mismatch, ok := err.(*activeGenerationMismatchError); ok {
			return true, incompatibleRevisionError(ctx, revision, mismatch.Error())
		}
		return true, revisionError(ctx, http.StatusServiceUnavailable, "could not verify active Loom generation", err)
	}
	if !revision.TargetExecutionID.Valid {
		return true, revisionError(ctx, http.StatusConflict, "active explorer revision has no Loom execution", nil)
	}
	exec, err := handler.loom().GetExecution(ctx.Context(), revision.TargetExecutionID.String, ctx.Get("Authorization"))
	if err != nil {
		return true, revisionError(ctx, http.StatusServiceUnavailable, "could not load active Loom execution", err)
	}
	var overlay appconfig.ExplorerOverlay
	if err := json.Unmarshal(revision.Overlay, &overlay); err != nil {
		return true, revisionError(ctx, http.StatusInternalServerError, "stored overlay is invalid", err)
	}
	return true, httputil.JSON(resolveOverlayConfig(overlay, exec), http.StatusOK).Write(ctx)
}

type activeGenerationMismatchError struct{ ProjectID, ExpectedExecution, ExpectedGeneration string }

func (e *activeGenerationMismatchError) Error() string {
	return fmt.Sprintf("active Loom datasets do not match Gecko revision: project=%s execution=%s generation=%s", e.ProjectID, e.ExpectedExecution, e.ExpectedGeneration)
}

func (handler *Handler) verifyActiveRevision(ctx context.Context, revision *geckodb.ExplorerRevision, bearer string) error {
	if !revision.TargetExecutionID.Valid || !revision.TargetGeneration.Valid {
		return &activeGenerationMismatchError{ProjectID: revision.ProjectID, ExpectedExecution: revision.TargetExecutionID.String, ExpectedGeneration: revision.TargetGeneration.String}
	}
	datasets, err := handler.loom().GetProjectDatasets(ctx, revision.ProjectID, bearer)
	if err != nil {
		return err
	}
	for _, dataset := range datasets {
		if activeDatasetMatchesRevision(dataset, revision) {
			return nil
		}
	}
	return &activeGenerationMismatchError{ProjectID: revision.ProjectID, ExpectedExecution: revision.TargetExecutionID.String, ExpectedGeneration: revision.TargetGeneration.String}
}

func incompatibleRevisionError(ctx fiber.Ctx, revision *geckodb.ExplorerRevision, message string) error {
	mismatchDiagnostic := diagnostic{Code: "ACTIVE_GENERATION_MISMATCH", DataType: "", ConfigPath: "", Message: message}
	return httputil.JSON(map[string]any{
		"status": "INCOMPATIBLE", "configRevisionId": revision.ID,
		"message": message, "errors": []diagnostic{mismatchDiagnostic},
	}, http.StatusConflict).Write(ctx)
}

func resolveOverlayConfig(overlay appconfig.ExplorerOverlay, exec loom.Execution) appconfig.Config {
	if overlay.LegacyConfig != nil {
		return *overlay.LegacyConfig
	}
	resolved := appconfig.Config{
		ExplorerConfig: []appconfig.ConfigItem{},
		SharedFilters:  appconfig.SharedFiltersConfig{SharedFilter: map[string][]appconfig.FilterPair{}},
		FileActions:    appconfig.FileActionsConfig{Extensions: overlay.FileActions.Extensions, Actions: overlay.FileActions.Actions},
	}
	// Resolve a semantic reference against the candidate execution. A missing
	// reference is intentionally returned unchanged: validation blocks publish,
	// while the resolved response remains useful for displaying diagnostics.
	resolve := func(path string, columns map[string]loom.Column) string {
		if c, ok := columns[path]; ok && c.Name != "" {
			return c.Name
		}
		return path
	}
	outputColumns := func(dataType string) map[string]loom.Column {
		out := map[string]loom.Column{}
		for _, output := range exec.Outputs {
			if output.DataType != "" && output.DataType != dataType {
				continue
			}
			for _, col := range output.Columns {
				out[col.SemanticPath] = col
			}
		}
		return out
	}
	byDataType := map[string]map[string]loom.Column{}
	for _, tab := range overlay.ExplorerConfig {
		byDataType[tab.DataType] = outputColumns(tab.DataType)
	}
	for name, pairs := range overlay.SharedFilters.Defined {
		for _, pair := range pairs {
			columns := byDataType[pair.Index]
			resolved.SharedFilters.SharedFilter[name] = append(resolved.SharedFilters.SharedFilter[name], appconfig.FilterPair{Index: pair.Index, Field: resolve(pair.SemanticPath, columns)})
		}
	}
	for _, tab := range overlay.ExplorerConfig {
		columns := outputColumns(tab.DataType)
		item := appconfig.ConfigItem{
			TabTitle:    tab.TabTitle,
			GuppyConfig: appconfig.GuppyConfig{DataType: tab.DataType, NodeCountTitle: tab.GuppyConfig.NodeCountTitle},
			Charts:      map[string]appconfig.Chart{},
			Filters:     appconfig.FiltersConfig{Tabs: []appconfig.FilterTab{}},
			Table:       appconfig.TableConfig{Enabled: true, Fields: []string{}},
			Dropdowns:   tab.Dropdowns, LoginForDownload: tab.LoginForDownload, PreFilters: map[string]any{},
		}
		if tab.Table != nil {
			if tab.Table.Enabled != nil {
				item.Table.Enabled = *tab.Table.Enabled
			}
			item.Table.DetailsConfig = appconfig.TableDetailsConfig{Panel: tab.Table.DetailsConfig.Panel, Mode: tab.Table.DetailsConfig.Mode, Title: tab.Table.DetailsConfig.Title, NodeType: tab.Table.DetailsConfig.NodeType, NodeFields: map[string]string{}}
			item.Table.DetailsConfig.IDField = resolve(tab.Table.DetailsConfig.IDSemanticPath, columns)
			item.Table.DetailsConfig.FilterField = resolve(tab.Table.DetailsConfig.FilterSemanticPath, columns)
			for path, value := range tab.Table.DetailsConfig.NodeFields {
				item.Table.DetailsConfig.NodeFields[resolve(path, columns)] = resolve(value, columns)
			}
			for _, path := range tab.Table.Fields {
				item.Table.Fields = append(item.Table.Fields, resolve(path, columns))
			}
			for path, column := range tab.Table.Columns {
				physical := resolve(path, columns)
				item.Table.Columns = ensureTableColumns(item.Table.Columns)
				item.Table.Columns[physical] = appconfig.TableColumnsConfig{Field: physical, Title: column.Title, AccessorPath: column.AccessorPath, Type: appconfig.SummaryTableColumnType(column.Type), CellRenderFunction: column.CellRenderFunction, Params: column.Params, Width: column.Width, Sortable: column.Sortable, Visable: column.Visible}
				if !containsString(item.Table.Fields, physical) {
					item.Table.Fields = append(item.Table.Fields, physical)
				}
			}
		}
		for _, filterTab := range tab.Filters.Tabs {
			out := appconfig.FilterTab{Title: filterTab.Title, Fields: []string{}, FieldsConfig: map[string]appconfig.FieldConfig{}}
			for _, field := range filterTab.Fields {
				out.Fields = append(out.Fields, resolve(field.SemanticPath, columns))
			}
			for path, cfg := range filterTab.FieldsConfig {
				physical := resolve(path, columns)
				out.FieldsConfig[physical] = appconfig.FieldConfig{Field: physical, DataField: resolve(cfg.DataFieldSemanticPath, columns), Index: resolve(cfg.IndexSemanticPath, columns), Label: cfg.Label, Type: cfg.Type}
			}
			item.Filters.Tabs = append(item.Filters.Tabs, out)
		}
		for _, mapping := range tab.GuppyConfig.FieldMapping {
			item.GuppyConfig.FieldMapping = append(item.GuppyConfig.FieldMapping, appconfig.GuppyFieldMapping{Field: resolve(mapping.SemanticPath, columns), Name: mapping.Name})
		}
		for _, path := range tab.GuppyConfig.AccessibleFieldCheckList {
			item.GuppyConfig.AccessibleFieldCheckList = append(item.GuppyConfig.AccessibleFieldCheckList, resolve(path, columns))
		}
		item.GuppyConfig.AccessibleValidationField = resolve(tab.GuppyConfig.AccessibleValidationField, columns)
		item.GuppyConfig.ManifestMapping = appconfig.ManifestMapping{ResourceIndexType: tab.GuppyConfig.ManifestMapping.ResourceIndexType, ResourceIdField: resolve(tab.GuppyConfig.ManifestMapping.ResourceIDSemanticPath, columns), ReferenceIdFieldInResourceIndex: resolve(tab.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInResourceIndex, columns), ReferenceIdFieldInDataIndex: resolve(tab.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInDataIndex, columns)}
		for path, chart := range tab.Charts {
			item.Charts[resolve(path, columns)] = appconfig.Chart{ChartType: chart.ChartType, Title: chart.Title}
		}
		var outputColumnsList []loom.Column
		for _, out := range exec.Outputs {
			if out.DataType == "" || out.DataType == tab.DataType {
				outputColumnsList = append(outputColumnsList, out.Columns...)
			}
		}
		byPath := map[string]loom.Column{}
		for _, col := range outputColumnsList {
			byPath[col.SemanticPath] = col
			// Legacy v1 imports carry physical field names in semanticPath until
			// they are migrated. Exact-name fallback is intentionally one-way and
			// does not pretend to preserve customization across a rename.
			if col.Name != "" {
				if _, exists := byPath[col.Name]; !exists {
					byPath[col.Name] = col
				}
			}
		}
		excluded := map[string]bool{}
		for _, field := range tab.Fields {
			if field.Table != nil && field.Table.Include != nil && !*field.Table.Include {
				excluded[physicalColumnName(field.SemanticPath, byPath)] = true
			}
			if field.Download != nil && field.Download.Include != nil && !*field.Download.Include {
				excluded[physicalColumnName(field.SemanticPath, byPath)] = true
			}
		}
		include := true
		if tab.IncludeUnconfiguredFields != nil {
			include = *tab.IncludeUnconfiguredFields
		}
		seen := map[string]bool{}
		physical := func(path string) string {
			if c, ok := byPath[path]; ok && c.Name != "" {
				return c.Name
			}
			return path
		}
		for _, col := range outputColumnsList {
			if include && col.Name != "" && !excluded[col.Name] && !seen[col.Name] {
				item.Table.Fields = append(item.Table.Fields, col.Name)
				seen[col.Name] = true
			}
		}
		for _, field := range tab.Fields {
			name := physical(field.SemanticPath)
			if name == "" {
				continue
			}
			if field.Table != nil && (field.Table.Include == nil || *field.Table.Include) && !seen[name] {
				item.Table.Fields = append(item.Table.Fields, name)
				seen[name] = true
			}
			if field.Chart != nil {
				item.Charts[name] = appconfig.Chart{ChartType: field.Chart.ChartType, Title: field.Chart.Title}
			}
			if field.Table != nil {
				item.Table.Columns = ensureTableColumns(item.Table.Columns)
				column := item.Table.Columns[name]
				column.Field = name
				column.Title = field.Table.Title
				if field.Table.Sortable != nil {
					column.Sortable = *field.Table.Sortable
				}
				item.Table.Columns[name] = column
			}
			if field.Renderer != "" {
				item.Table.Columns = ensureTableColumns(item.Table.Columns)
				item.Table.Columns[name] = appconfig.TableColumnsConfig{Field: name, CellRenderFunction: field.Renderer, Params: field.Params}
			}
			for _, filter := range field.Filters {
				item.Filters.Tabs = append(item.Filters.Tabs, appconfig.FilterTab{Title: filter.Label, Fields: []string{name}})
			}
		}
		for path, values := range tab.PreFilters {
			item.PreFilters[physical(path)] = values
		}
		for _, button := range tab.Buttons {
			args := appconfig.ButtonActionArgs{ResourceIndexType: button.ActionArgs.ResourceIndexType, ResourceIdField: physical(button.ActionArgs.ResourceIDSemanticPath), ReferenceIdFieldInDataIndex: physical(button.ActionArgs.ReferenceIDSemanticPathInDataIndex), ReferenceIdFieldInResourceIndex: physical(button.ActionArgs.ReferenceIDSemanticPathInResourceIndex)}
			for _, path := range button.ActionArgs.FileSemanticPaths {
				args.FileFields = append(args.FileFields, physical(path))
			}
			item.Buttons = append(item.Buttons, appconfig.ButtonConfig{Enabled: button.Enabled, Type: button.Type, Action: button.Action, Title: button.Title, LeftIcon: button.LeftIcon, RightIcon: button.RightIcon, FileName: button.FileName, ActionArgs: args})
		}
		resolved.ExplorerConfig = append(resolved.ExplorerConfig, item)
	}
	return resolved
}

func physicalColumnName(path string, columns map[string]loom.Column) string {
	if c, ok := columns[path]; ok && c.Name != "" {
		return c.Name
	}
	return path
}

func ensureTableColumns(columns map[string]appconfig.TableColumnsConfig) map[string]appconfig.TableColumnsConfig {
	if columns == nil {
		return map[string]appconfig.TableColumnsConfig{}
	}
	return columns
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (handler *Handler) handleExplorerStatus(ctx fiber.Ctx) error {
	if _, err := handler.reconcilePublishing(ctx.Context(), ctx.Params("configId"), ctx.Get("Authorization")); err != nil {
		return revisionError(ctx, http.StatusInternalServerError, "could not reconcile explorer publication", err)
	}
	r, err := geckodb.ActiveExplorerRevision(ctx.Context(), handler.db, ctx.Params("configId"))
	if err != nil {
		return revisionError(ctx, 500, "could not load explorer status", err)
	}
	if r == nil {
		pending, pendingErr := geckodb.ExplorerRevisionByStatus(ctx.Context(), handler.db, ctx.Params("configId"), statusPublishing)
		if pendingErr != nil {
			return revisionError(ctx, http.StatusInternalServerError, "could not load pending explorer publication", pendingErr)
		}
		if pending != nil {
			return httputil.JSON(map[string]any{"status": pending.Status, "revisionId": pending.ID, "targetExecutionId": pending.TargetExecutionID.String, "targetGeneration": pending.TargetGeneration.String, "targetSchemaDigest": pending.TargetSchemaDigest.String, "diagnostics": json.RawMessage(pending.Diagnostics)}, 200).Write(ctx)
		}
		return httputil.JSON(map[string]any{"status": "UNAVAILABLE", "configId": ctx.Params("configId")}, 200).Write(ctx)
	}
	if err := handler.verifyActiveRevision(ctx.Context(), r, ctx.Get("Authorization")); err != nil {
		if mismatch, ok := err.(*activeGenerationMismatchError); ok {
			return incompatibleRevisionError(ctx, r, mismatch.Error())
		}
		return revisionError(ctx, http.StatusServiceUnavailable, "could not verify active Loom generation", err)
	}
	return httputil.JSON(map[string]any{"status": r.Status, "revisionId": r.ID, "targetExecutionId": r.TargetExecutionID.String, "targetGeneration": r.TargetGeneration.String, "targetSchemaDigest": r.TargetSchemaDigest.String, "diagnostics": json.RawMessage(r.Diagnostics)}, 200).Write(ctx)
}

// reconcilePublishing repairs the narrow crash window between Loom activation
// and the Gecko activation transaction. It only promotes a candidate when Loom
// reports the exact candidate execution/revision and generation as active.
func (handler *Handler) reconcilePublishing(ctx context.Context, configID, bearer string) (*geckodb.ExplorerRevision, error) {
	pending, err := geckodb.ExplorerRevisionByStatus(ctx, handler.db, configID, statusPublishing)
	if err != nil || pending == nil {
		return nil, err
	}
	if !pending.TargetExecutionID.Valid || !pending.TargetGeneration.Valid {
		return nil, nil
	}
	datasets, err := handler.loom().GetProjectDatasets(ctx, pending.ProjectID, bearer)
	if err != nil {
		return nil, nil
	} // Loom outages do not hide the last active config.
	for _, dataset := range datasets {
		if !activeDatasetMatchesRevision(dataset, pending) {
			continue
		}
		expectedParent := ""
		if pending.ParentRevisionID.Valid {
			expectedParent = pending.ParentRevisionID.String
		}
		if err := geckodb.ActivateExplorerRevision(ctx, handler.db, pending, expectedParent); err != nil {
			// Another request may have repaired it concurrently. The transaction's
			// parent check makes this safe to retry on the next request.
			return nil, nil
		}
		pending.Status = statusActive
		return pending, nil
	}
	return nil, nil
}

func activeDatasetMatchesRevision(dataset loom.ActiveDataset, revision *geckodb.ExplorerRevision) bool {
	if dataset.State != "" && dataset.State != "READY" {
		return false
	}
	if dataset.ProjectID != "" && dataset.ProjectID != revision.ProjectID {
		return false
	}
	if dataset.DatasetGeneration != revision.TargetGeneration.String {
		return false
	}
	return dataset.Revision == revision.TargetExecutionID.String || dataset.ID == revision.TargetExecutionID.String
}

func (handler *Handler) loom() loom.Client {
	u := os.Getenv("LOOM_BASE_URL")
	if u == "" {
		u = os.Getenv("LOOM_URL")
	}
	return loom.Client{BaseURL: u}
}
func revisionJSON(r *geckodb.ExplorerRevision) map[string]any {
	out := map[string]any{"revisionId": r.ID, "configId": r.ConfigID, "projectId": r.ProjectID, "digest": r.Digest, "status": r.Status, "overlay": json.RawMessage(r.Overlay), "diagnostics": json.RawMessage(r.Diagnostics), "createdAt": r.CreatedAt, "updatedAt": r.UpdatedAt}
	if r.ParentRevisionID.Valid {
		out["parentRevisionId"] = r.ParentRevisionID.String
	}
	if r.TargetExecutionID.Valid {
		out["targetExecutionId"] = r.TargetExecutionID.String
	}
	return out
}
func digestJSON(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func revisionError(ctx fiber.Ctx, code int, msg string, details any) error {
	d := map[string]any(nil)
	if details != nil {
		d = map[string]any{"details": details}
	}
	return httputil.NewError("explorer_revision_error", msg, code, d, nil).Write(ctx)
}
func revisionValidationUnavailable(ctx fiber.Ctx, r *geckodb.ExplorerRevision, err error) error {
	v := validationResponse{RevisionID: r.ID, Status: "UNAVAILABLE", Errors: []diagnostic{{Code: "LOOM_UNAVAILABLE", Message: err.Error()}}}
	return httputil.JSON(v, http.StatusServiceUnavailable).Write(ctx)
}

func validateOverlay(o appconfig.ExplorerOverlay) error {
	if o.Version() != 2 {
		return fmt.Errorf("schemaVersion must be 2")
	}
	for i, t := range o.ExplorerConfig {
		if t.DataType == "" {
			return fmt.Errorf("explorerConfig[%d].dataType is required", i)
		}
		for j, f := range t.Fields {
			if strings.TrimSpace(f.SemanticPath) == "" {
				return fmt.Errorf("explorerConfig[%d].fields[%d].semanticPath is required", i, j)
			}
			if strings.EqualFold(f.MissingPolicy, "OMIT") && strings.TrimSpace(f.OmissionReason) == "" {
				return fmt.Errorf("explorerConfig[%d].fields[%d].omissionReason is required for OMIT", i, j)
			}
			if f.MissingPolicy != "" && f.MissingPolicy != "ERROR" && f.MissingPolicy != "OMIT" {
				return fmt.Errorf("invalid missingPolicy for %s", f.SemanticPath)
			}
		}
		requirePath := func(path, name string) error {
			if strings.TrimSpace(path) == "" {
				return fmt.Errorf("explorerConfig[%d].%s semanticPath is required", i, name)
			}
			return nil
		}
		for path := range t.Charts {
			if err := requirePath(path, "charts"); err != nil {
				return err
			}
		}
		for fi, filterTab := range t.Filters.Tabs {
			for fj, field := range filterTab.Fields {
				if err := requirePath(field.SemanticPath, fmt.Sprintf("filters.tabs[%d].fields[%d]", fi, fj)); err != nil {
					return err
				}
			}
			for path, cfg := range filterTab.FieldsConfig {
				if err := requirePath(path, fmt.Sprintf("filters.tabs[%d].fieldsConfig", fi)); err != nil {
					return err
				}
				if cfg.DataFieldSemanticPath != "" {
					if err := requirePath(cfg.DataFieldSemanticPath, "dataFieldSemanticPath"); err != nil {
						return err
					}
				}
				if cfg.IndexSemanticPath != "" {
					if err := requirePath(cfg.IndexSemanticPath, "indexSemanticPath"); err != nil {
						return err
					}
				}
			}
		}
		for fi, mapping := range t.GuppyConfig.FieldMapping {
			if err := requirePath(mapping.SemanticPath, fmt.Sprintf("guppyConfig.fieldMapping[%d]", fi)); err != nil {
				return err
			}
		}
		for fi, path := range t.GuppyConfig.AccessibleFieldCheckList {
			if err := requirePath(path, fmt.Sprintf("guppyConfig.accessibleFieldCheckList[%d]", fi)); err != nil {
				return err
			}
		}
		for name, path := range map[string]string{"guppyConfig.accessibleValidationField": t.GuppyConfig.AccessibleValidationField, "guppyConfig.manifestMapping.resourceIdSemanticPath": t.GuppyConfig.ManifestMapping.ResourceIDSemanticPath, "guppyConfig.manifestMapping.referenceIdSemanticPathInResourceIndex": t.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInResourceIndex, "guppyConfig.manifestMapping.referenceIdSemanticPathInDataIndex": t.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInDataIndex} {
			if path != "" {
				if err := requirePath(path, name); err != nil {
					return err
				}
			}
		}
		if t.Table != nil {
			for fi, path := range t.Table.Fields {
				if err := requirePath(path, fmt.Sprintf("table.fields[%d]", fi)); err != nil {
					return err
				}
			}
			for path := range t.Table.Columns {
				if err := requirePath(path, "table.columns"); err != nil {
					return err
				}
			}
			for name, path := range map[string]string{"table.detailsConfig.idSemanticPath": t.Table.DetailsConfig.IDSemanticPath, "table.detailsConfig.filterSemanticPath": t.Table.DetailsConfig.FilterSemanticPath} {
				if path != "" {
					if err := requirePath(path, name); err != nil {
						return err
					}
				}
			}
			for path, value := range t.Table.DetailsConfig.NodeFields {
				if err := requirePath(path, "table.detailsConfig.nodeFields"); err != nil {
					return err
				}
				if err := requirePath(value, "table.detailsConfig.nodeFields"); err != nil {
					return err
				}
			}
		}
		for path := range t.PreFilters {
			if err := requirePath(path, "preFilters"); err != nil {
				return err
			}
		}
		for bi, button := range t.Buttons {
			for name, path := range map[string]string{"resourceIdSemanticPath": button.ActionArgs.ResourceIDSemanticPath, "referenceIdSemanticPathInDataIndex": button.ActionArgs.ReferenceIDSemanticPathInDataIndex, "referenceIdSemanticPathInResourceIndex": button.ActionArgs.ReferenceIDSemanticPathInResourceIndex} {
				if path != "" {
					if err := requirePath(path, fmt.Sprintf("buttons[%d].actionArgs.%s", bi, name)); err != nil {
						return err
					}
				}
			}
			for fi, path := range button.ActionArgs.FileSemanticPaths {
				if err := requirePath(path, fmt.Sprintf("buttons[%d].actionArgs.fileSemanticPaths[%d]", bi, fi)); err != nil {
					return err
				}
			}
		}
	}
	for name, pairs := range o.SharedFilters.Defined {
		for pi, pair := range pairs {
			if strings.TrimSpace(pair.SemanticPath) == "" {
				return fmt.Errorf("sharedFilters.defined[%q][%d].semanticPath is required", name, pi)
			}
		}
	}
	return nil
}

func validateOverlayAgainstExecution(o appconfig.ExplorerOverlay, e loom.Execution) validationResponse {
	v := validationResponse{Status: "VALID", Errors: []diagnostic{}, Warnings: []diagnostic{}, AcknowledgedOmissions: []diagnostic{}}
	for ti, t := range o.ExplorerConfig {
		idx := map[string]loom.Column{}
		matchedOutput := false
		for _, out := range e.Outputs {
			// Prefer the output whose dataType matches the tab. Older Loom
			// responses omitted dataType, so those remain compatible as a
			// fallback; a mismatched typed output is never consulted.
			outputType := out.DataType
			if outputType == "" {
				outputType = out.Name
			}
			if outputType == "" && len(e.Outputs) == 1 {
				outputType = t.DataType
			}
			if outputType != t.DataType {
				continue
			}
			matchedOutput = true
			for _, c := range out.Columns {
				idx[c.SemanticPath] = c
				if c.Name != "" {
					if _, exists := idx[c.Name]; !exists {
						idx[c.Name] = c
					}
				}
			}
		}
		if !matchedOutput {
			v.Errors = append(v.Errors, diagnostic{Code: "DATASET_NOT_FOUND", DataType: t.DataType, ConfigPath: fmt.Sprintf("explorerConfig[%d].dataType", ti), Message: "configured dataframe output is absent from candidate execution"})
			continue
		}
		for fi, f := range t.Fields {
			c, ok := idx[f.SemanticPath]
			path := fmt.Sprintf("explorerConfig[%d].fields[%d]", ti, fi)
			if !ok {
				d := diagnostic{Code: "FIELD_NOT_FOUND", DataType: t.DataType, SemanticPath: f.SemanticPath, ConfigPath: path, Message: "configured field is absent from candidate dataframe"}
				if f.MissingPolicy == "OMIT" {
					v.AcknowledgedOmissions = append(v.AcknowledgedOmissions, d)
				} else {
					v.Errors = append(v.Errors, d)
				}
				continue
			}
			if f.Chart != nil && !c.Aggregatable {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_AGGREGATABLE", DataType: t.DataType, SemanticPath: f.SemanticPath, ConfigPath: path + ".chart", Message: "chart field is not aggregatable"})
			}
			if len(f.Filters) > 0 && !c.Filterable {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_FILTERABLE", DataType: t.DataType, SemanticPath: f.SemanticPath, ConfigPath: path + ".filters", Message: "configured filter field is not filterable"})
			}
			if f.Table != nil && f.Table.Sortable != nil && *f.Table.Sortable && !c.Sortable {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_SORTABLE", DataType: t.DataType, SemanticPath: f.SemanticPath, ConfigPath: path + ".table", Message: "configured table field is not sortable"})
			}
		}
		// V2 keeps all of the legacy Explorer surfaces, but uses semantic paths
		// for every reference. Validate those references here as well so a
		// resolver can never silently emit a partially configured document.
		check := func(semanticPath, configPath string) loom.Column {
			if strings.TrimSpace(semanticPath) == "" {
				return loom.Column{}
			}
			c, ok := idx[semanticPath]
			if !ok {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_FOUND", DataType: t.DataType, SemanticPath: semanticPath, ConfigPath: configPath, Message: "configured field is absent from candidate dataframe"})
				return loom.Column{}
			}
			return c
		}
		for path := range t.Charts {
			c := check(path, fmt.Sprintf("explorerConfig[%d].charts[%q]", ti, path))
			if path != "" && c.SemanticPath != "" && !c.Aggregatable {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_AGGREGATABLE", DataType: t.DataType, SemanticPath: path, ConfigPath: fmt.Sprintf("explorerConfig[%d].charts[%q]", ti, path), Message: "chart field is not aggregatable"})
			}
		}
		for fti, filterTab := range t.Filters.Tabs {
			for ffi, field := range filterTab.Fields {
				c := check(field.SemanticPath, fmt.Sprintf("explorerConfig[%d].filters.tabs[%d].fields[%d]", ti, fti, ffi))
				if c.SemanticPath != "" && !c.Filterable {
					v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_FILTERABLE", DataType: t.DataType, SemanticPath: field.SemanticPath, ConfigPath: fmt.Sprintf("explorerConfig[%d].filters.tabs[%d].fields[%d]", ti, fti, ffi), Message: "configured filter field is not filterable"})
				}
			}
			for path, cfg := range filterTab.FieldsConfig {
				check(path, fmt.Sprintf("explorerConfig[%d].filters.tabs[%d].fieldsConfig[%q]", ti, fti, path))
				if cfg.DataFieldSemanticPath != "" {
					check(cfg.DataFieldSemanticPath, fmt.Sprintf("explorerConfig[%d].filters.tabs[%d].fieldsConfig[%q].dataFieldSemanticPath", ti, fti, path))
				}
				if cfg.IndexSemanticPath != "" {
					check(cfg.IndexSemanticPath, fmt.Sprintf("explorerConfig[%d].filters.tabs[%d].fieldsConfig[%q].indexSemanticPath", ti, fti, path))
				}
			}
		}
		for mi, mapping := range t.GuppyConfig.FieldMapping {
			check(mapping.SemanticPath, fmt.Sprintf("explorerConfig[%d].guppyConfig.fieldMapping[%d].semanticPath", ti, mi))
		}
		for fi, path := range t.GuppyConfig.AccessibleFieldCheckList {
			check(path, fmt.Sprintf("explorerConfig[%d].guppyConfig.accessibleFieldCheckList[%d]", ti, fi))
		}
		check(t.GuppyConfig.AccessibleValidationField, fmt.Sprintf("explorerConfig[%d].guppyConfig.accessibleValidationField", ti))
		check(t.GuppyConfig.ManifestMapping.ResourceIDSemanticPath, fmt.Sprintf("explorerConfig[%d].guppyConfig.manifestMapping.resourceIdSemanticPath", ti))
		check(t.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInResourceIndex, fmt.Sprintf("explorerConfig[%d].guppyConfig.manifestMapping.referenceIdSemanticPathInResourceIndex", ti))
		check(t.GuppyConfig.ManifestMapping.ReferenceIDSemanticPathInDataIndex, fmt.Sprintf("explorerConfig[%d].guppyConfig.manifestMapping.referenceIdSemanticPathInDataIndex", ti))
		if t.Table != nil {
			for fi, path := range t.Table.Fields {
				check(path, fmt.Sprintf("explorerConfig[%d].table.fields[%d]", ti, fi))
			}
			for path, column := range t.Table.Columns {
				c := check(path, fmt.Sprintf("explorerConfig[%d].table.columns[%q]", ti, path))
				if c.SemanticPath != "" && column.Sortable && !c.Sortable {
					v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_SORTABLE", DataType: t.DataType, SemanticPath: path, ConfigPath: fmt.Sprintf("explorerConfig[%d].table.columns[%q]", ti, path), Message: "table column is not sortable"})
				}
			}
			check(t.Table.DetailsConfig.IDSemanticPath, fmt.Sprintf("explorerConfig[%d].table.detailsConfig.idSemanticPath", ti))
			check(t.Table.DetailsConfig.FilterSemanticPath, fmt.Sprintf("explorerConfig[%d].table.detailsConfig.filterSemanticPath", ti))
			for path, value := range t.Table.DetailsConfig.NodeFields {
				check(path, fmt.Sprintf("explorerConfig[%d].table.detailsConfig.nodeFields[%q]", ti, path))
				check(value, fmt.Sprintf("explorerConfig[%d].table.detailsConfig.nodeFields[%q]", ti, path))
			}
		}
		for path := range t.PreFilters {
			check(path, fmt.Sprintf("explorerConfig[%d].preFilters[%q]", ti, path))
		}
		for bi, button := range t.Buttons {
			check(button.ActionArgs.ResourceIDSemanticPath, fmt.Sprintf("explorerConfig[%d].buttons[%d].actionArgs.resourceIdSemanticPath", ti, bi))
			check(button.ActionArgs.ReferenceIDSemanticPathInDataIndex, fmt.Sprintf("explorerConfig[%d].buttons[%d].actionArgs.referenceIdSemanticPathInDataIndex", ti, bi))
			check(button.ActionArgs.ReferenceIDSemanticPathInResourceIndex, fmt.Sprintf("explorerConfig[%d].buttons[%d].actionArgs.referenceIdSemanticPathInResourceIndex", ti, bi))
			for fi, path := range button.ActionArgs.FileSemanticPaths {
				check(path, fmt.Sprintf("explorerConfig[%d].buttons[%d].actionArgs.fileSemanticPaths[%d]", ti, bi, fi))
			}
		}
	}
	for name, pairs := range o.SharedFilters.Defined {
		for pi, pair := range pairs {
			matched := false
			for _, out := range e.Outputs {
				if out.DataType != "" && out.DataType != pair.Index {
					continue
				}
				matched = true
				break
			}
			if !matched {
				v.Errors = append(v.Errors, diagnostic{Code: "DATASET_NOT_FOUND", DataType: pair.Index, ConfigPath: fmt.Sprintf("sharedFilters.defined[%q][%d].index", name, pi), Message: "shared filter dataframe output is absent from candidate execution"})
				continue
			}
			found := false
			for _, out := range e.Outputs {
				if out.DataType != "" && out.DataType != pair.Index {
					continue
				}
				for _, c := range out.Columns {
					if c.SemanticPath == pair.SemanticPath {
						found = true
					}
				}
			}
			if !found {
				v.Errors = append(v.Errors, diagnostic{Code: "FIELD_NOT_FOUND", DataType: pair.Index, SemanticPath: pair.SemanticPath, ConfigPath: fmt.Sprintf("sharedFilters.defined[%q][%d].semanticPath", name, pi), Message: "configured shared filter field is absent from candidate dataframe"})
			}
		}
	}
	if len(v.Errors) > 0 {
		v.Status = "INVALID"
	} else if len(v.AcknowledgedOmissions) > 0 {
		v.Status = "VALID_WITH_OMISSIONS"
	}
	return v
}
