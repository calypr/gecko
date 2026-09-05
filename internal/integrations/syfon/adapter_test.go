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

func TestGetProjectMetricsSummaryReadsRecordValidator(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/index/v1/metrics/summary" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("organization"); got != "org" {
			t.Fatalf("expected organization query, got %q", got)
		}
		if got := r.URL.Query().Get("project"); got != "proj" {
			t.Fatalf("expected project query, got %q", got)
		}
		body := []byte(`{"total_files":99,"record_count":42,"record_latest_updated_time":"2026-07-02T00:00:00Z","record_revision":"rev-1"}`)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	manager := NewManager("http://syfon.example", client)
	summary, err := manager.GetProjectMetricsSummary(context.Background(), "Bearer token", "org", "proj")
	if err != nil {
		t.Fatalf("GetProjectMetricsSummary returned error: %v", err)
	}
	if summary == nil {
		t.Fatal("expected metrics summary")
	}
	if summary.RecordCount != 42 || summary.RecordLatestUpdatedTime != "2026-07-02T00:00:00Z" || summary.RecordRevision != "rev-1" {
		t.Fatalf("unexpected metrics summary: %+v", summary)
	}
}

func TestListProjectFileUsageByObjectIDsUsesBulkEndpoint(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/index/v1/metrics/files/bulk" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("organization"); got != "org" {
			t.Fatalf("expected organization query, got %q", got)
		}
		if got := r.URL.Query().Get("project"); got != "proj" {
			t.Fatalf("expected project query, got %q", got)
		}
		var req struct {
			ObjectIDs    []string `json:"object_ids"`
			InactiveDays *int     `json:"inactive_days"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if !reflect.DeepEqual(req.ObjectIDs, []string{"obj-a", "obj-b"}) {
			t.Fatalf("unexpected object ids: %+v", req.ObjectIDs)
		}
		if req.InactiveDays == nil || *req.InactiveDays != 30 {
			t.Fatalf("unexpected inactive days: %+v", req.InactiveDays)
		}
		body := []byte(`{"data":[{"object_id":"obj-a","name":"a.txt","size":100,"download_count":3,"upload_count":1}]}`)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	manager := NewManager("http://syfon.example", client)
	usage, err := manager.ListProjectFileUsageByObjectIDs(context.Background(), "Bearer token", "org", "proj", []string{"obj-a", "obj-b", "obj-a", ""}, 30)
	if err != nil {
		t.Fatalf("ListProjectFileUsageByObjectIDs returned error: %v", err)
	}
	if len(usage) != 1 || usage["obj-a"].DownloadCount != 3 || usage["obj-a"].UploadCount != 1 {
		t.Fatalf("unexpected usage response: %+v", usage)
	}
}

func TestListProjectBucketInventoryUsesInventoryEndpoint(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/project-bucket/inventory" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Organization string `json:"organization"`
			Project      string `json:"project"`
			Mode         string `json:"mode"`
			PathPrefix   string `json:"path_prefix"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Organization != "org" || req.Project != "proj" || req.Mode != "items" || req.PathPrefix != "CONFIG" {
			t.Fatalf("unexpected request body: %+v", req)
		}
		body, err := json.Marshal(map[string]any{
			"summary": map[string]any{
				"inventory_complete": false,
				"inventory_warning":  "terminal replay returned different page content",
				"computed_at":        "2026-07-07T18:00:01Z",
			},
			"items": []map[string]any{{
				"object_url":    "s3://bucket/root/CONFIG/a.bin",
				"provider":      "s3",
				"bucket":        "bucket",
				"key":           "root/CONFIG/a.bin",
				"path":          "CONFIG/a.bin",
				"size_bytes":    123,
				"etag":          "etag-1",
				"last_modified": "2026-07-07T18:00:00Z",
			}}})
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	manager := NewManager("http://syfon", client)

	inventory, err := manager.ListProjectBucketInventory(context.Background(), "Bearer token", "org", "proj", "CONFIG")
	if err != nil {
		t.Fatalf("ListProjectBucketInventory returned error: %v", err)
	}
	if len(inventory.Objects) != 1 || inventory.Objects[0].ObjectURL != "s3://bucket/root/CONFIG/a.bin" || inventory.Objects[0].SizeBytes != 123 {
		t.Fatalf("unexpected inventory items: %+v", inventory.Objects)
	}
	if inventory.Complete || !strings.Contains(inventory.Warning, "terminal replay") || inventory.ObservedAt != "2026-07-07T18:00:01Z" {
		t.Fatalf("unexpected inventory metadata: %+v", inventory)
	}
}

func TestListProjectBucketObjectsPreservesIncompleteSummary(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/project-bucket" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		body := []byte(`{"summary":{"inventory_complete":false,"inventory_warning":"listing incomplete","computed_at":"2026-07-07T18:00:01Z"},"items":[{"object_url":"s3://bucket/a.bin","bucket":"bucket","key":"a.bin"}]}`)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	inventory, err := NewManager("http://syfon", client).ListProjectBucketObjects(context.Background(), "Bearer token", "org", "proj", "")
	if err != nil {
		t.Fatalf("ListProjectBucketObjects returned error: %v", err)
	}
	if inventory.Complete || inventory.Warning != "listing incomplete" || inventory.ObservedAt != "2026-07-07T18:00:01Z" || len(inventory.Objects) != 1 {
		t.Fatalf("unexpected partial project bucket response: %+v", inventory)
	}
}

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

func TestBulkDeleteObjectsUsesBulkDRSDeleteEndpoint(t *testing.T) {
	t.Helper()

	var requests []string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/ga4gh/drs/v1/objects/delete" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}

		var req struct {
			BulkObjectIDs        []string `json:"bulk_object_ids"`
			DeleteObjectMetadata *bool    `json:"delete_object_metadata"`
			DeleteStorageData    *bool    `json:"delete_storage_data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if !reflect.DeepEqual(req.BulkObjectIDs, []string{"obj-a", "obj-b"}) {
			t.Fatalf("unexpected bulk object ids: %#v", req.BulkObjectIDs)
		}
		if req.DeleteObjectMetadata == nil || !*req.DeleteObjectMetadata {
			t.Fatalf("expected delete_object_metadata=true, got %#v", req.DeleteObjectMetadata)
		}
		if req.DeleteStorageData == nil || !*req.DeleteStorageData {
			t.Fatalf("expected delete_storage_data=true, got %#v", req.DeleteStorageData)
		}

		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})}

	manager := NewManager("http://syfon.example/data", client)
	if err := manager.BulkDeleteObjects(
		context.Background(),
		"Bearer token",
		[]string{"obj-a", "obj-b", "obj-a"},
		true,
	); err != nil {
		t.Fatalf("BulkDeleteObjects returned error: %v", err)
	}

	expected := []string{"PUT /ga4gh/drs/v1/objects/delete"}
	if !reflect.DeepEqual(requests, expected) {
		t.Fatalf("unexpected delete requests: %#v", requests)
	}
}

func TestRegisterProjectObjectsUsesDRSRegistrationEndpoint(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/ga4gh/drs/v1/objects/register" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("expected authorization header")
		}
		var request struct {
			Candidates []struct {
				Name             string   `json:"name"`
				Size             int64    `json:"size"`
				ControlledAccess []string `json:"controlled_access"`
				Checksums        []struct {
					Type     string `json:"type"`
					Checksum string `json:"checksum"`
				} `json:"checksums"`
				AccessMethods []struct {
					AccessURL struct {
						URL string `json:"url"`
					} `json:"access_url"`
				} `json:"access_methods"`
			} `json:"candidates"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(request.Candidates) != 1 || request.Candidates[0].Name != "a.bin" || request.Candidates[0].Size != 100 || len(request.Candidates[0].Checksums) != 1 || request.Candidates[0].Checksums[0].Type != "sha256" || request.Candidates[0].Checksums[0].Checksum != strings.Repeat("a", 64) || len(request.Candidates[0].AccessMethods) != 1 || request.Candidates[0].AccessMethods[0].AccessURL.URL != "s3://bucket/root/a.bin" {
			t.Fatalf("unexpected registration request: %+v", request)
		}
		return &http.Response{
			StatusCode: http.StatusCreated,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"objects":[{"id":"obj-created"}]}`)),
		}, nil
	})}
	manager := NewManager("http://syfon.example", client)
	results, err := manager.RegisterProjectObjects(context.Background(), "Bearer token", []ProjectObjectRegistration{{
		Name:             "a.bin",
		Checksum:         strings.Repeat("a", 64),
		Size:             100,
		ControlledAccess: []string{"/organization/org/project/proj"},
		AccessURLs:       []string{"s3://bucket/root/a.bin"},
	}})
	if err != nil {
		t.Fatalf("RegisterProjectObjects returned error: %v", err)
	}
	if !reflect.DeepEqual(results, []ProjectObjectRegistrationResult{{ObjectID: "obj-created"}}) {
		t.Fatalf("unexpected registration result: %+v", results)
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

func TestBulkListStorageObjectsSendsExpectedName(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/bulk-list" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Items []struct {
				ID                string `json:"id"`
				ObjectURL         string `json:"object_url"`
				ExpectedSizeBytes *int64 `json:"expected_size_bytes"`
				ExpectedName      string `json:"expected_name"`
			} `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(req.Items) != 1 || req.Items[0].ExpectedName != "file.bin" {
			t.Fatalf("expected request to include expected_name, got %+v", req.Items)
		}
		sizeMatch := true
		nameMatch := true
		body, err := json.Marshal(map[string]any{"items": []map[string]any{{
			"id":                req.Items[0].ID,
			"object_url":        req.Items[0].ObjectURL,
			"exists":            true,
			"status":            "present",
			"validation_status": "matched",
			"size_match":        sizeMatch,
			"name_match":        nameMatch,
		}}})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	size := int64(17)
	manager := NewManager("http://syfon.example", client)
	results, err := manager.BulkListStorageObjects(context.Background(), "Bearer token", []BulkStorageProbeItem{{
		ID:                "item-1",
		ObjectURL:         "s3://bucket/file.bin",
		ExpectedSizeBytes: &size,
		ExpectedName:      "file.bin",
	}})
	if err != nil {
		t.Fatalf("BulkListStorageObjects returned error: %v", err)
	}
	if len(results) != 1 || results[0].NameMatch == nil || !*results[0].NameMatch {
		t.Fatalf("expected parsed name_match result, got %+v", results)
	}
}

func TestBulkListStorageObjectsDeduplicatesIdenticalRequests(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/bulk-list" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		requests++
		var req struct {
			Items []struct {
				ID                string `json:"id"`
				ObjectURL         string `json:"object_url"`
				ExpectedSizeBytes *int64 `json:"expected_size_bytes"`
				ExpectedName      string `json:"expected_name"`
			} `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(req.Items) != 1 {
			t.Fatalf("expected identical LIST items to be deduplicated, got %+v", req.Items)
		}
		sizeMatch := true
		nameMatch := true
		body, err := json.Marshal(map[string]any{"items": []map[string]any{{
			"id":                req.Items[0].ID,
			"object_url":        req.Items[0].ObjectURL,
			"exists":            true,
			"status":            "present",
			"validation_status": "matched",
			"size_match":        sizeMatch,
			"name_match":        nameMatch,
		}}})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}

	size := int64(17)
	manager := NewManager("http://syfon.example", client)
	results, err := manager.BulkListStorageObjects(context.Background(), "Bearer token", []BulkStorageProbeItem{
		{ID: "item-1", ObjectURL: "s3://bucket/file.bin", ExpectedSizeBytes: &size, ExpectedName: "file.bin"},
		{ID: "item-2", ObjectURL: "s3://bucket/file.bin", ExpectedSizeBytes: &size, ExpectedName: "file.bin"},
	})
	if err != nil {
		t.Fatalf("BulkListStorageObjects returned error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected one bulk-list request, got %d", requests)
	}
	if len(results) != 2 || results[0].ObjectURL != "s3://bucket/file.bin" || results[1].ObjectURL != "s3://bucket/file.bin" {
		t.Fatalf("expected deduplicated response to fan out to all original items, got %+v", results)
	}
}

func TestListProjectAuditRecordsIncludesPathPrefix(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/project-records" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Organization string `json:"organization"`
			Project      string `json:"project"`
			PathPrefix   string `json:"path_prefix"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Organization != "org" || req.Project != "proj" || req.PathPrefix != "CONFIG" {
			t.Fatalf("unexpected request payload: %+v", req)
		}
		body, err := json.Marshal(map[string]any{"items": []map[string]any{}})
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
	if _, err := manager.ListProjectAuditRecords(context.Background(), "Bearer token", "org", "proj", "/CONFIG/"); err != nil {
		t.Fatalf("ListProjectAuditRecords returned error: %v", err)
	}
}

func TestListProjectAuditRecordsFoldsAccessMethodURLsIntoAccessURLs(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/inspect/project-records" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		body, err := json.Marshal(map[string]any{"items": []map[string]any{{
			"object_id": "obj-1",
			"name":      "file.txt",
			"checksum":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"size":      10,
			"access_methods": []map[string]any{
				{"access_id": "s3", "type": "s3", "url": "s3://bucket/path/file.txt"},
				{"access_id": "s3", "type": "s3", "url": "s3://bucket/path/file.txt"},
			},
		}}})
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
	records, err := manager.ListProjectAuditRecords(context.Background(), "Bearer token", "org", "proj", "")
	if err != nil {
		t.Fatalf("ListProjectAuditRecords returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one record, got %+v", records)
	}
	if !reflect.DeepEqual(records[0].AccessURLs, []string{"s3://bucket/path/file.txt"}) {
		t.Fatalf("expected access method URL folded into access URLs, got %+v", records[0].AccessURLs)
	}
	if len(records[0].AccessMethods) != 2 {
		t.Fatalf("expected access methods preserved, got %+v", records[0].AccessMethods)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
