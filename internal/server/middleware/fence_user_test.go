package middleware

import "testing"

func TestFenceUserEndpoint(t *testing.T) {
	token := "Bearer eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2V4YW1wbGUub3JnL3VzZXIifQ."
	endpoint, err := fenceUserEndpoint(token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint != "https://example.org/user/user" {
		t.Fatalf("unexpected endpoint %q", endpoint)
	}
}
