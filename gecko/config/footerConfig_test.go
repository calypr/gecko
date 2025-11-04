package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

const rawConfig = `{
  "classNames": {
    "root": "",
    "layout": "flex items-center justify-center"
  },
  "columnLinks": [
    {
      "heading": "Resources",
      "items": [
        { "text": "About", "href": "/about" },
        { "text": "Contact", "href": "/contact", "linkType": "portal" }
      ]
    }
  ],
  "rightSection": {
    "columns": [
      {
        "rows": [
          {
            "Icon": {
              "logo": "/icons/knight.svg",
              "logolight": "/icons/knight_white.svg",
              "width": 100,
              "height": 47,
              "description": "Knight Cancer Institute"
            }
          }
        ]
      }
    ]
  }
}`

func TestFooterConfig_RoundTrip(t *testing.T) {
	var cfg FooterProps
	if err := json.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		t.Fatalf("Unmarshal original failed: %v", err)
	}

	// Use Marshal, not MarshalIndent, for tighter JSON and easier debugging of the resulting string
	marshaled, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var originalMap any
	if err := json.Unmarshal([]byte(rawConfig), &originalMap); err != nil {
		t.Fatalf("Unmarshal original map failed: %v", err)
	}

	var marshaledMap any
	if err := json.Unmarshal(marshaled, &marshaledMap); err != nil {
		t.Fatalf("Unmarshal marshaled map failed: %v", err)
	}

	if !reflect.DeepEqual(originalMap, marshaledMap) {
		t.Errorf("Round-trip mismatch (Content differs):\nOriginal:\n%s\n\nGot:\n%s\n", rawConfig, string(marshaled))
		t.Errorf("Normalized Original:\n%s\nNormalized Got:\n%s\n", rawConfig, string(marshaled))
	} else {
		t.Log("Round-trip successful!")
	}
}

func TestFooterConfig_ColumnLinks(t *testing.T) {
	var cfg FooterProps
	if err := json.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(cfg.ColumnLinks) != 1 {
		t.Fatalf("Expected 1 ColumnLinks, got %d", len(cfg.ColumnLinks))
	}

	col := cfg.ColumnLinks[0]
	if col.Heading != "Resources" {
		t.Errorf("Expected heading 'Resources', got %q", col.Heading)
	}
}

func TestFooterConfig_FooterRow_Icon(t *testing.T) {
	var cfg FooterProps
	if err := json.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if cfg.RightSection == nil || len(cfg.RightSection.Columns) == 0 {
		t.Fatal("rightSection or columns missing")
	}

	iconRow := cfg.RightSection.Columns[0].Rows[0]

	if iconRow.Kind != "Icon" || iconRow.Icon == nil {
		t.Fatal("Expected Icon with Logo")
	}

	logo := *iconRow.Icon
	expected := FooterLogo{
		Logo:        "/icons/knight.svg",
		LogoLight:   "/icons/knight_white.svg",
		Width:       100,
		Height:      47,
		Description: "Knight Cancer Institute",
	}

	if logo != expected {
		t.Errorf("Logo mismatch:\nGot: %+v\nWant: %+v", logo, expected)
	}
}
