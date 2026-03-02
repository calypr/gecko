package main

import (
	"bytes"
	"log"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeRequest(method, url string, payload []byte) *http.Request {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	if err != nil {
		log.Fatal("makeRequest Err: ", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestHealthCheck(t *testing.T) {
	resp, err := http.DefaultClient.Do(makeRequest("GET", "http://localhost:8080/health", nil))
	assert.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		body := buf.String()
		assert.Contains(t, body, "Healthy")
	}
}
