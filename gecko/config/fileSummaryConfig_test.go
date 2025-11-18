package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const filesummaryJSON = `{
  "config": {
    "document_reference_title": {
      "title": "Title",
      "field": "document_reference_title"
    },
    "document_reference_size": {
      "cellRenderFunction": "HumanReadableString",
      "type": "string",
      "title": "File Size",
      "field": "document_reference_size"
    },
    "document_reference_source_path": {
      "title": "Source Path",
      "field": "document_reference_source_path"
    }
  },
  "binslicePoints": [
    0, 1048576, 524288000, 1073741824, 107374182400, 9007199254740991
  ],
  "barChartColor": "#e9724d",
  "defaultProject": "gdc-esca",
  "idField": "document_reference_id",
  "index": "document_reference"
}`

// ---------------------------------------------------------------------
// Round-trip Test (Order & Whitespace Insensitive)
// ---------------------------------------------------------------------

func TestFilesummaryConfig_RoundTrip(t *testing.T) {
	var cfg FilesummaryConfig
	if err := json.Unmarshal([]byte(filesummaryJSON), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Use Marshal, not MarshalIndent, for tighter JSON and easier comparison
	marshaled, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Unmarshal both the original and the marshaled JSON into generic maps
	// to enable a deep comparison that ignores field order (which reflect.DeepEqual
	// does correctly for map[string]any but is sensitive to whitespace/indentation
	// if comparing raw strings).

	var originalMap any
	if err := json.Unmarshal([]byte(filesummaryJSON), &originalMap); err != nil {
		t.Fatalf("Unmarshal original map failed: %v", err)
	}

	var marshaledMap any
	if err := json.Unmarshal(marshaled, &marshaledMap); err != nil {
		t.Fatalf("Unmarshal marshaled map failed: %v", err)
	}

	// The DeepEqual check on maps is the "order-insensitive" check
	if !reflect.DeepEqual(originalMap, marshaledMap) {
		// Use cmp.Diff on the unmarshaled structs for better debug output
		// if the map comparison fails (optional, as the string output is often enough)
		t.Errorf("Round-trip mismatch (Content differs - likely an omitted field or type issue)")

		// Marshal the original struct back out with indentation for a clearer visual comparison
		wantJSON, _ := json.MarshalIndent(originalMap, "", "  ")
		gotJSON, _ := json.MarshalIndent(marshaledMap, "", "  ")

		t.Errorf("--- want (Normalized) ---\n%s\n--- got (Normalized) ---\n%s\n", string(wantJSON), string(gotJSON))
	} else {
		t.Log("Round-trip successful (order-insensitive, content preserved)")
	}
}

// ---------------------------------------------------------------------
// Field Validation Test
// ---------------------------------------------------------------------

func TestFilesummaryConfig_Fields(t *testing.T) {
	var cfg FilesummaryConfig
	if err := json.Unmarshal([]byte(filesummaryJSON), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// --- Top-level Fields ---
	if cfg.BarChartColor != "#e9724d" {
		t.Errorf("BarChartColor = %q, want %q", cfg.BarChartColor, "#e9724d")
	}
	if cfg.DefaultProject != "gdc-esca" {
		t.Errorf("DefaultProject = %q, want %q", cfg.DefaultProject, "gdc-esca")
	}
	if cfg.IdField != "document_reference_id" {
		t.Errorf("IdField = %q, want %q", cfg.IdField, "document_reference_id")
	}
	if cfg.Index != "document_reference" {
		t.Errorf("Index = %q, want %q", cfg.Index, "document_reference")
	}

	// --- BinslicePoints Array ---
	expectedBins := []int{0, 1048576, 524288000, 1073741824, 107374182400, 9007199254740991}
	if !reflect.DeepEqual(cfg.BinslicePoints, expectedBins) {
		t.Errorf("BinslicePoints mismatch: \nGot: %+v\nWant: %+v", cfg.BinslicePoints, expectedBins)
	}

	// --- Config Map ---
	if len(cfg.Config) != 3 {
		t.Fatalf("expected 3 config entries, got %d", len(cfg.Config))
	}

	// Title column check
	titleCol, ok := cfg.Config["document_reference_title"]
	if !ok {
		t.Fatal("missing document_reference_title")
	}
	wantTitleCol := TableColumnsConfig{
		Title: "Title",
		Field: "document_reference_title",
		// CellRenderFunction and Type should be zero values
	}
	if !reflect.DeepEqual(titleCol, wantTitleCol) {
		t.Errorf("Title column mismatch:\nGot: %+v\nWant: %+v", titleCol, wantTitleCol)
	}

	// Size column check
	sizeCol, ok := cfg.Config["document_reference_size"]
	if !ok {
		t.Fatal("missing document_reference_size")
	}
	wantSizeCol := TableColumnsConfig{
		CellRenderFunction: "HumanReadableString",
		Type:               SummaryTableColumnTypeString, // Assumes this constant is 'string'
		Title:              "File Size",
		Field:              "document_reference_size",
	}
	if !reflect.DeepEqual(sizeCol, wantSizeCol) {
		t.Errorf("Size column mismatch:\nGot: %+v\nWant: %+v", sizeCol, wantSizeCol)
	}

	// Path column check
	pathCol, ok := cfg.Config["document_reference_source_path"]
	if !ok {
		t.Fatal("missing document_reference_source_path")
	}
	wantPathCol := TableColumnsConfig{
		Title: "Source Path",
		Field: "document_reference_source_path",
	}
	if !reflect.DeepEqual(pathCol, wantPathCol) {
		t.Errorf("Path column mismatch:\nGot: %+v\nWant: %+v", pathCol, wantPathCol)
	}
}

// ---------------------------------------------------------------------
// Test omitempty behavior on Type (Example from original, adapted)
// ---------------------------------------------------------------------

func TestFilesummaryConfig_TypeOmitEmpty(t *testing.T) {
	input := `{
		"config": {
			"test": {
				"field": "test",
				"title": "Test"
			}
		},
		"barChartColor": "#fff",
		"defaultProject": "test",
		"idField": "id",
		"index": "test",
		"binslicePoints": [1,2,3]
	}`

	var cfg FilesummaryConfig
	if err := json.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	col := cfg.Config["test"]
	// Verify that the zero value is correctly unmarshaled (empty string)
	if col.Type != "" {
		t.Errorf("expected Type to be zero value, got %q", col.Type)
	}

	out, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Test that the JSON tag 'omitempty' is working for the Type field
	if strings.Contains(string(out), `"type":`) {
		t.Error("empty type should be omitted in JSON but was found")
	}
}
