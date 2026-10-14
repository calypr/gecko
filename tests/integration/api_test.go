package main

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeRequest(t *testing.T, method, url string, payload []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	if err != nil {
		t.Fatalf("makeRequest Err: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestHealthCheck(t *testing.T) {
	baseURL := requireIntegrationServer(t)
	resp, err := http.DefaultClient.Do(makeRequest(t, "GET", baseURL+"/health", nil))
	assert.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			t.Fatalf("read body: %v", err)
		}
		body := buf.String()
		assert.Contains(t, body, "Healthy")
	}
}
