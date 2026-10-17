package access

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestResourceListAllowsProjectMatchesProgramResource(t *testing.T) {
	resources := []string{
		"/programs/Ellrott_Lab/projects/embedding_rotation",
		"/programs/Ellrott_Lab/projects/git_drs_test",
	}
	if !ResourceListAllowsProject(resources, "Ellrott_Lab", "git_drs_test") {
		t.Fatal("expected project access to be allowed")
	}
	if ResourceListAllowsProject(resources, "Ellrott_Lab", "missing") {
		t.Fatal("expected unrelated project access to be denied")
	}
}

func TestResourceListAllowsOrganizationMatchesProgramResources(t *testing.T) {
	resources := []string{
		"/programs/Ellrott_Lab/projects",
		"/programs/Ellrott_Lab/projects/git_drs_test",
		"/organization/HTAN_INT/project/BForePC",
	}

	if !ResourceListAllowsOrganization(resources, "Ellrott_Lab") {
		t.Fatal("expected program-scoped organization access to be allowed")
	}
	if ResourceListAllowsOrganization(resources, "HTAN_INT") {
		t.Fatal("expected legacy organization access to be denied")
	}
	if ResourceListAllowsOrganization(resources, "gdc_mirror") {
		t.Fatal("expected unrelated organization access to be denied")
	}
}

func TestParseResourceAccessSnapshotPrefersAuthzBlock(t *testing.T) {
	snapshot, err := parseResourceAccessSnapshot(map[string]any{
		"authz": map[string]any{
			"/programs/org-a": []any{
				map[string]any{"service": "arborist", "method": "manage-owners"},
			},
			"/programs/org-a/projects/proj-a": []any{
				map[string]any{"service": "*", "method": "read"},
				map[string]any{"service": "*", "method": "update"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a", "manage-owners", "arborist") {
		t.Fatal("expected org manage-owners access to be present")
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "update", "*") {
		t.Fatal("expected project update access to be present")
	}
	if ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-b", "update", "*") {
		t.Fatal("did not expect unrelated project access")
	}
}

func TestFenceUserAccessHandlerGetResourceAccessUsesProjectAccessFallback(t *testing.T) {
	responseBody, err := json.Marshal(map[string]any{
		"project_access": map[string]any{
			"/programs/org-a/projects/proj-a": []any{
				map[string]any{"service": "*", "method": "read"},
				map[string]any{"service": "*", "method": "update"},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal response body: %v", err)
	}
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(responseBody)),
			}, nil
		}),
	}

	token := "Bearer " + buildUnverifiedJWT(t, map[string]any{"iss": "https://example.test"})
	handler := NewFenceUserAccessHandler(client)
	snapshot, err := handler.GetResourceAccess(context.Background(), token)
	if err != nil {
		t.Fatalf("unexpected access lookup error: %v", err)
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "read", "*") {
		t.Fatal("expected read access from project_access fallback")
	}
	if !ResourceAccessAllows(snapshot, "/programs/org-a/projects/proj-a", "update", "*") {
		t.Fatal("expected update access from project_access fallback")
	}
}

func TestParseResourceAccessSnapshotRejectsMissingBlocks(t *testing.T) {
	_, err := parseResourceAccessSnapshot(map[string]any{})
	var accessErr *AccessError
	if !errors.As(err, &accessErr) {
		t.Fatalf("expected AccessError, got %T", err)
	}
	if accessErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502-style access error, got %d", accessErr.StatusCode)
	}
}

func TestFenceUserEndpoint(t *testing.T) {
	token := "Bearer eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2V4YW1wbGUub3JnL3VzZXIifQ."
	endpoint, err := FenceUserEndpoint(token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint != "https://example.org/user/user" {
		t.Fatalf("unexpected endpoint %q", endpoint)
	}
}

func buildUnverifiedJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	headerBody, err := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(headerBody) + "." + base64.RawURLEncoding.EncodeToString(body) + ".sig"
}
