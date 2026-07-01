package syfon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestBulkGetProjectRecordsByChecksumReadsResultsMap(t *testing.T) {
	t.Helper()

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/index/bulk/hashes" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Hashes []string `json:"hashes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if !reflect.DeepEqual(req.Hashes, []string{
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}) {
			t.Fatalf("unexpected hashes payload: %#v", req.Hashes)
		}

		org := "org"
		project := "proj"
		sizeA := int64(100)
		sizeB := int64(200)
		hashesA := map[string]string{"sha256": "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
		hashesB := map[string]string{"sha256": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}
		accessURLA := "s3://bucket/a"
		accessURLB := "s3://bucket/b"

		response := map[string]any{
			"results": map[string]any{
				req.Hashes[0]: []any{
					map[string]any{
						"did":          "obj-a",
						"organization": org,
						"project":      project,
						"size":         sizeA,
						"hashes":       hashesA,
						"access_methods": []any{
							map[string]any{
								"access_url": map[string]any{"url": accessURLA},
							},
						},
					},
				},
				req.Hashes[1]: []any{
					map[string]any{
						"did":          "obj-b",
						"organization": org,
						"project":      project,
						"size":         sizeB,
						"hashes":       hashesB,
						"access_methods": []any{
							map[string]any{
								"access_url": map[string]any{"url": accessURLB},
							},
						},
					},
				},
			},
		}
		body, err := json.Marshal(response)
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	manager := NewManager("http://syfon.example", client)
	records, err := manager.BulkGetProjectRecordsByChecksum(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
	)
	if err != nil {
		t.Fatalf("BulkGetProjectRecordsByChecksum returned error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 checksum groups, got %#v", records)
	}
	if len(records["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]) != 1 {
		t.Fatalf("expected first checksum match, got %#v", records)
	}
	if len(records["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]) != 1 {
		t.Fatalf("expected second checksum match, got %#v", records)
	}
	if got := records["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"][0].ObjectID; got != "obj-a" {
		t.Fatalf("expected obj-a, got %q", got)
	}
	if got := records["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"][0].AccessURLs; !reflect.DeepEqual(got, []string{"s3://bucket/b"}) {
		t.Fatalf("unexpected access urls: %#v", got)
	}
}

func TestBulkGetProjectRecordsByChecksumAllowsLegacyUnscopedResults(t *testing.T) {
	t.Helper()

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response := map[string]any{
			"results": map[string]any{
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": []any{
					map[string]any{
						"did":    "obj-a",
						"size":   100,
						"hashes": map[string]string{"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
					},
				},
			},
		}
		body, err := json.Marshal(response)
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	manager := NewManager("http://syfon.example", client)
	records, err := manager.BulkGetProjectRecordsByChecksum(
		context.Background(),
		"Bearer token",
		"org",
		"proj",
		[]string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	)
	if err != nil {
		t.Fatalf("BulkGetProjectRecordsByChecksum returned error: %v", err)
	}
	if len(records["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]) != 1 {
		t.Fatalf("expected fallback match for legacy unscoped result, got %#v", records)
	}
}

func TestBulkProbeStorageObjectsBatchesRequests(t *testing.T) {
	t.Helper()

	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/bulk" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		requests++
		var req struct {
			Items []struct {
				ID                string `json:"id"`
				ObjectURL         string `json:"object_url"`
				ExpectedSizeBytes *int64 `json:"expected_size_bytes"`
				ExpectedSHA256    string `json:"expected_sha256"`
			} `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(req.Items) == 0 || len(req.Items) > bulkStorageProbeBatchSize {
			t.Fatalf("unexpected batch size %d", len(req.Items))
		}
		respItems := make([]map[string]any, 0, len(req.Items))
		for _, item := range req.Items {
			respItems = append(respItems, map[string]any{
				"id":                item.ID,
				"object_url":        item.ObjectURL,
				"exists":            true,
				"status":            "present",
				"validation_status": "matched",
			})
		}
		body, err := json.Marshal(map[string]any{"items": respItems})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	manager := NewManager("http://syfon.example", client)
	items := make([]BulkStorageProbeItem, 0, bulkStorageProbeBatchSize+1)
	for i := 0; i < bulkStorageProbeBatchSize+1; i++ {
		size := int64(i + 1)
		items = append(items, BulkStorageProbeItem{
			ID:                "item-" + strconv.Itoa(i),
			ObjectURL:         "s3://bucket/object-" + strconv.Itoa(i),
			ExpectedSizeBytes: &size,
			ExpectedSHA256:    strings.Repeat("a", 64),
		})
	}

	results, err := manager.BulkProbeStorageObjects(context.Background(), "Bearer token", items)
	if err != nil {
		t.Fatalf("BulkProbeStorageObjects returned error: %v", err)
	}
	if requests != 2 {
		t.Fatalf("expected 2 bulk probe requests, got %d", requests)
	}
	if len(results) != len(items) {
		t.Fatalf("expected %d results, got %d", len(items), len(results))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
