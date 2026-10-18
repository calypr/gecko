package git

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
)

func TestParseStorageChildrenLimit(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		want      int
		wantError bool
	}{
		{name: "default", raw: "", want: defaultStorageChildrenLimit},
		{name: "explicit", raw: "25", want: 25},
		{name: "cap", raw: "5000", want: maxStorageChildrenLimit},
		{name: "invalid", raw: "nope", wantError: true},
		{name: "zero", raw: "0", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseStorageChildrenLimit(test.raw)
			if test.wantError {
				if err == nil {
					t.Fatalf("expected error for limit %q", test.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse limit: %v", err)
			}
			if got != test.want {
				t.Fatalf("expected limit %d, got %d", test.want, got)
			}
		})
	}
}

func TestParseStorageChildrenRequestOptionsReadsCursorAndSort(t *testing.T) {
	app := fiber.New()
	app.Get("/storage/children", func(ctx fiber.Ctx) error {
		options, errResponse := parseStorageChildrenRequestOptions(ctx, "org/proj")
		if errResponse != nil {
			return errResponse.Write(ctx)
		}
		return ctx.JSON(map[string]any{
			"cursor":     options.cursor,
			"limit":      options.limit,
			"sort_by":    options.sortBy,
			"sort_order": options.sortOrder,
		})
	})
	req := httptest.NewRequest(http.MethodGet, "/storage/children?limit=15&cursor=abc123&sort_by=name&sort_order=asc", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("run request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["cursor"] != "abc123" || body["sort_by"] != "name" || body["sort_order"] != "asc" || body["limit"].(float64) != 15 {
		t.Fatalf("unexpected parsed options: %+v", body)
	}
}
