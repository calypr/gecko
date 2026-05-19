package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

const exampleConfig = `{
  "headerProps": {
    "topBar": {
      "items": [
        {
          "href": "https://www.ohsu.edu/knight-cancer-institute",
          "name": "CBDS",
          "classNames": {
            "root": "",
            "label": "",
            "button": ""
          }
        }
      ],
      "loginButtonVisibility": "hidden"
    },
    "navigation": {
      "classNames": {
        "root": "bg-base-max text-primary opacity-100 hover:opacity-100",
        "item": "py-2 px-4 hover:bg-base-lightest hover:text-base-contrast",
        "navigationPanel": "bg-base-max text-primary"
      },
      "logo": {
        "src": "/icons/ohsu.svg",
        "width": 52.5,
        "height": 40,
        "href": "/Apps",
        "title": "CALYPR"
      },
      "items": [
        {
          "icon": "gen3:exploration",
          "href": "/Explorer",
          "name": "Exploration",
          "tooltip": "The Exploration Page enables discovery of the data at the subject level and features a cohort builder.",
          "title": "Explorer"
        },
        {
          "icon": "gen3:profile",
          "href": "/Profile",
          "name": "Profile",
          "tooltip": "Create API keys for programmatic data access, and review your authorization privileges to datasets and services.",
          "title": "Profile"
        }
      ]
    },
    "leftnav": [
      {
        "title": "Home",
        "description": "Home Apps page",
        "icon": "/icons/home.svg",
        "href": "/Apps",
        "perms": null
      },
      {
        "title": "Directory Structure",
        "description": "Search for files via a tree based interactive search",
        "icon": "/icons/binary-tree.svg",
        "href": "/Miller",
        "perms": null
      },
      {
        "title": "File Summary",
        "description": "Overview of file system usage",
        "icon": "/icons/file.svg",
        "href": "/Filesummary",
        "perms": null
      },
      {
        "title": "Project Discovery",
        "description": "Explore project summaries of every project in CALYPR",
        "icon": "/icons/compass.svg",
        "href": "/Discovery",
        "perms": null
      },
      {
        "title": "Image Viewer",
        "description": "View available .ome.tif images using Avivator",
        "icon": "/icons/layers-intersect.svg",
        "href": "/AvailableImages",
        "perms": null
      },
      {
        "title": "My Projects",
        "description": "Identify the list of projects in which you have access",
        "icon": "/icons/key.svg",
        "href": "/MyProjects",
        "perms": null
      }
    ]
  },
  "footerProps": {
    "classNames": {
      "root": "",
      "layout": "flex items-center justify-center"
    },
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
  },
  "headerMetadata": {
    "title": "CALYPR",
    "content": "Cancer Analytics Platform",
    "key": "calypr-main"
  }
}`

// ---------------------------------------------------------------------
// Round-trip Test (Order & Whitespace Insensitive)
// ---------------------------------------------------------------------

func TestNavPageLayout_RoundTrip(t *testing.T) {
	var cfg NavPageLayoutProps
	if err := json.Unmarshal([]byte(exampleConfig), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Marshal the struct back into JSON (without indentation)
	marshaled, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Unmarshal both the original and the marshaled JSON into generic maps
	// to perform an order-insensitive DeepEqual comparison.
	var originalMap any
	if err := json.Unmarshal([]byte(exampleConfig), &originalMap); err != nil {
		t.Fatalf("Unmarshal original map failed: %v", err)
	}

	var marshaledMap any
	if err := json.Unmarshal(marshaled, &marshaledMap); err != nil {
		t.Fatalf("Unmarshal marshaled map failed: %v", err)
	}

	// Use DeepEqual on maps to ignore key ordering and whitespace/indentation differences
	if !reflect.DeepEqual(originalMap, marshaledMap) {
		t.Errorf("Round-trip mismatch (Content differs - likely an omitted field or type issue)")

		// Remarshal with indentation for clear visual debugging output
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

func TestNavPageLayout_Fields(t *testing.T) {
	var cfg NavPageLayoutProps
	if err := json.Unmarshal([]byte(exampleConfig), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// --- headerMetadata check ---
	if cfg.HeaderMetadata.Title != "CALYPR" {
		t.Errorf("headerMetadata title = %q, want %q", cfg.HeaderMetadata.Title, "CALYPR")
	}
	if cfg.HeaderMetadata.Content != "Cancer Analytics Platform" {
		t.Errorf("headerMetadata content = %q, want %q", cfg.HeaderMetadata.Content, "Cancer Analytics Platform")
	}

	// --- topBar check ---
	if cfg.HeaderProps.Top.LoginButtonVisibility != "hidden" {
		t.Errorf("loginButtonVisibility = %q, want %q", cfg.HeaderProps.Top.LoginButtonVisibility, "hidden")
	}
	if len(cfg.HeaderProps.Top.Items) != 1 {
		t.Fatalf("expected 1 topBar item, got %d", len(cfg.HeaderProps.Top.Items))
	}
	if cfg.HeaderProps.Top.Items[0].Name != "CBDS" {
		t.Errorf("topBar item name = %q, want %q", cfg.HeaderProps.Top.Items[0].Name, "CBDS")
	}

	// --- navigation logo check ---
	logo := cfg.HeaderProps.Navigation.Logo
	if logo == nil {
		t.Fatal("navigation logo missing")
	}
	if logo.Src != "/icons/ohsu.svg" || logo.Title != "CALYPR" {
		t.Errorf("navigation logo mismatch:\nGot: %+v\nWant: Src:/icons/ohsu.svg, Title:CALYPR", logo)
	}

	// --- navigation items check ---
	if len(cfg.HeaderProps.Navigation.Items) != 2 {
		t.Fatalf("expected 2 navigation items, got %d", len(cfg.HeaderProps.Navigation.Items))
	}
	if cfg.HeaderProps.Navigation.Items[0].Name != "Exploration" {
		t.Errorf("first navigation name = %q, want %q", cfg.HeaderProps.Navigation.Items[0].Name, "Exploration")
	}
	if cfg.HeaderProps.Navigation.Items[1].Tooltip != "Create API keys for programmatic data access, and review your authorization privileges to datasets and services." {
		t.Errorf("second navigation tooltip mismatch")
	}

	// --- leftnav check ---
	if len(cfg.HeaderProps.LeftNav) != 6 {
		t.Fatalf("expected 6 leftnav items, got %d", len(cfg.HeaderProps.LeftNav))
	}
	if cfg.HeaderProps.LeftNav[2].Title != "File Summary" {
		t.Errorf("third leftnav title = %q, want %q", cfg.HeaderProps.LeftNav[2].Title, "File Summary")
	}
	// Check a null value for perms
	if cfg.HeaderProps.LeftNav[2].Perms != nil {
		t.Errorf("leftnav item perms expected to be nil, got %v", cfg.HeaderProps.LeftNav[2].Perms)
	}

	// --- footer section check (re-using your original logic structure) ---
	if cfg.FooterProps.RightSection == nil || len(cfg.FooterProps.RightSection.Columns) == 0 {
		t.Fatal("footer rightSection or columns missing")
	}

	rows := cfg.FooterProps.RightSection.Columns[0].Rows
	if len(rows) != 1 {
		t.Fatalf("expected 1 footer row, got %d", len(rows))
	}
	iconRow := rows[0].Icon
	if iconRow == nil {
		t.Fatal("footer Icon row missing")
	}
	if iconRow.Description != "Knight Cancer Institute" {
		t.Errorf("footer description = %q, want %q", iconRow.Description, "Knight Cancer Institute")
	}
	if iconRow.Width != 100 {
		t.Errorf("footer icon width = %d, want %d", iconRow.Width, 100)
	}
}
