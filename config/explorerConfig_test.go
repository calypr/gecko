package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

const explorerJSON = `{
  "sharedFilters": {
    "defined": {
      "proj": [
        {
          "index": "file",
          "field": "project_id"
        }
      ]
    }
  },
  "explorerConfig": [
    {
      "tabTitle": "test",
      "guppyConfig": {
        "dataType": "file",
        "nodeCountTitle": "file Count",
        "fieldMapping": [
          {
            "field": "file_id",
            "name": "ID"
          }
        ],
        "manifestMapping": {
          "resourceIndexType": "file",
          "resourceIdField": "file_id"
        }
      },
      "charts": {
        "a": {
          "chartType": "bar",
          "title": "a"
        }
      },
      "filters": {
        "tabs": [
          {
            "title": "Filters",
            "fields": [
              "project_id"
            ]
          }
        ]
      },
      "table": {
        "enabled": true,
        "fields": [
          "project_id"
        ],
        "detailsConfig": {
          "panel": "details",
          "title": "Details"
        }
      },
      "buttons": [
        {
          "enabled": true,
          "type": "manifest",
          "action": "download",
          "title": "Download Manifest",
          "actionArgs": {
            "resourceIndexType": "file"
          }
        }
      ],
      "preFilters": {
        "project_id": ["proj1"]
      }
    }
  ],
  "fileActions": {
    "action1": ["field1"]
  }
}`

func TestExplorerConfig_RoundTrip(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(explorerJSON), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	marshaled, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var originalMap any
	if err := json.Unmarshal([]byte(explorerJSON), &originalMap); err != nil {
		t.Fatalf("Unmarshal original map failed: %v", err)
	}

	var marshaledMap any
	if err := json.Unmarshal(marshaled, &marshaledMap); err != nil {
		t.Fatalf("Unmarshal marshaled map failed: %v", err)
	}

	if !reflect.DeepEqual(originalMap, marshaledMap) {
		t.Errorf("Round-trip mismatch")
		wantJSON, _ := json.MarshalIndent(originalMap, "", "  ")
		gotJSON, _ := json.MarshalIndent(marshaledMap, "", "  ")
		t.Errorf("--- want ---\n%s\n--- got ---\n%s\n", string(wantJSON), string(gotJSON))
	}
}

func TestExplorerConfig_Fields(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(explorerJSON), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(cfg.ExplorerConfig) != 1 {
		t.Fatalf("expected 1 explorerConfig, got %d", len(cfg.ExplorerConfig))
	}

	item := cfg.ExplorerConfig[0]
	if item.TabTitle != "test" {
		t.Errorf("TabTitle = %q, want %q", item.TabTitle, "test")
	}

	if len(item.Buttons) != 1 {
		t.Fatalf("expected 1 button, got %d", len(item.Buttons))
	}

	btn := item.Buttons[0]
	if btn.Type != "manifest" || btn.Title != "Download Manifest" {
		t.Errorf("button mismatch: %+v", btn)
	}

	if btn.ActionArgs.ResourceIndexType != "file" {
		t.Errorf("ActionArgs.ResourceIndexType = %q, want %q", btn.ActionArgs.ResourceIndexType, "file")
	}

	if val, ok := item.PreFilters["project_id"]; !ok {
		t.Error("missing preFilters project_id")
	} else {
		// val will be []any since item.PreFilters is map[string]any
		slice, ok := val.([]any)
		if !ok || len(slice) != 1 || slice[0] != "proj1" {
			t.Errorf("preFilters project_id = %v, want [proj1]", val)
		}
	}
}
