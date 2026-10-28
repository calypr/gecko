package syfon

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (manager *Manager) BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequest(ctx, authorizationHeader, "/data/inspect/bulk", items, false, false)
}

func (manager *Manager) BulkProbeStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem, onBatch func([]BulkStorageProbeItem, []BulkStorageProbeResult) error) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequestWithProgress(ctx, authorizationHeader, "/data/inspect/bulk", items, false, false, onBatch)
}

func (manager *Manager) BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequest(ctx, authorizationHeader, "/data/inspect/bulk", items, false, true)
}

func (manager *Manager) bulkStorageObjectRequest(ctx context.Context, authorizationHeader string, requestPath string, items []BulkStorageProbeItem, includeExpectedName bool, confirmMissing bool) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequestWithProgress(ctx, authorizationHeader, requestPath, items, includeExpectedName, confirmMissing, nil)
}

func (manager *Manager) bulkStorageObjectRequestWithProgress(ctx context.Context, authorizationHeader string, requestPath string, items []BulkStorageProbeItem, includeExpectedName bool, confirmMissing bool, onBatch func([]BulkStorageProbeItem, []BulkStorageProbeResult) error) ([]BulkStorageProbeResult, error) {
	if len(items) == 0 {
		return []BulkStorageProbeResult{}, nil
	}
	started := time.Now()
	requestItems := append([]BulkStorageProbeItem(nil), items...)
	duplicateCount := 0
	resultKeyByOriginalID := make(map[string]string, len(requestItems))
	for _, item := range requestItems {
		resultKeyByOriginalID[strings.TrimSpace(item.ID)] = strings.TrimSpace(item.ID)
	}
	if includeExpectedName {
		var deduped map[string]string
		items, deduped = dedupeBulkStorageProbeItems(items, includeExpectedName)
		duplicateCount = len(requestItems) - len(items)
		resultKeyByOriginalID = deduped
	}
	batchCount := (len(items) + bulkStorageProbeBatchSize - 1) / bulkStorageProbeBatchSize
	log.Printf("INFO: syfon_bulk_storage_request_start path=%s items=%d unique_items=%d duplicate_items=%d batch_size=%d batches=%d concurrency=%d include_expected_name=%t", requestPath, len(requestItems), len(items), duplicateCount, bulkStorageProbeBatchSize, batchCount, bulkStorageProbeConcurrency, includeExpectedName)
	resultsByID := make(map[string]BulkStorageProbeResult, len(items))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, bulkStorageProbeConcurrency)
	var firstErr error
	var launchErr error
	for start := 0; start < len(items); start += bulkStorageProbeBatchSize {
		if err := ctx.Err(); err != nil {
			launchErr = err
			break
		}
		end := start + bulkStorageProbeBatchSize
		if end > len(items) {
			end = len(items)
		}
		batchStart := start
		batch := append([]BulkStorageProbeItem(nil), items[start:end]...)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			launchErr = ctx.Err()
			break
		}
		if launchErr != nil {
			break
		}
		if err := ctx.Err(); err != nil {
			<-sem
			launchErr = err
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := ctx.Err(); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			batchStarted := time.Now()
			results, err := manager.probeStorageObjectBatch(ctx, authorizationHeader, requestPath, batch, includeExpectedName, confirmMissing)
			batchMs := time.Since(batchStarted).Milliseconds()
			if err == nil && onBatch != nil {
				if err = validateBulkStorageObservationBatch(batch, results); err == nil {
					observedAt := time.Now().UTC().Format(time.RFC3339Nano)
					for index := range results {
						results[index].ObservedAt = observedAt
					}
					err = onBatch(append([]BulkStorageProbeItem(nil), batch...), append([]BulkStorageProbeResult(nil), results...))
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Printf("INFO: syfon_bulk_storage_request_batch path=%s batch_start=%d batch_items=%d duration_ms=%d error=%q", requestPath, batchStart, len(batch), batchMs, err.Error())
				if firstErr == nil {
					firstErr = fmt.Errorf("bulk syfon storage object request %s batch starting at %d: %w", requestPath, batchStart, err)
				}
				return
			}
			for _, result := range results {
				resultsByID[strings.TrimSpace(result.ID)] = result
			}
			log.Printf("INFO: syfon_bulk_storage_request_batch path=%s batch_start=%d batch_items=%d result_items=%d duration_ms=%d", requestPath, batchStart, len(batch), len(results), batchMs)
		}()
	}
	wg.Wait()
	if firstErr != nil {
		log.Printf("INFO: syfon_bulk_storage_request_done path=%s items=%d results=%d batches=%d duration_ms=%d error=%q", requestPath, len(items), len(resultsByID), batchCount, time.Since(started).Milliseconds(), firstErr.Error())
		return nil, firstErr
	}
	if launchErr != nil {
		log.Printf("INFO: syfon_bulk_storage_request_done path=%s items=%d results=%d batches=%d duration_ms=%d error=%q", requestPath, len(items), len(resultsByID), batchCount, time.Since(started).Milliseconds(), launchErr.Error())
		return nil, launchErr
	}
	out := make([]BulkStorageProbeResult, 0, len(requestItems))
	for _, item := range requestItems {
		resultKey := resultKeyByOriginalID[strings.TrimSpace(item.ID)]
		if result, ok := resultsByID[resultKey]; ok {
			result.ID = strings.TrimSpace(item.ID)
			out = append(out, result)
		}
	}
	log.Printf("INFO: syfon_bulk_storage_request_done path=%s items=%d unique_items=%d results=%d batches=%d duration_ms=%d", requestPath, len(requestItems), len(items), len(out), batchCount, time.Since(started).Milliseconds())
	return out, nil
}

func validateBulkStorageObservationBatch(items []BulkStorageProbeItem, results []BulkStorageProbeResult) error {
	expectedByID := make(map[string]string, len(items))
	for _, item := range items {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			return fmt.Errorf("bulk storage observation request contains an empty item ID")
		}
		expectedByID[id] = canonicalStorageProbeURL(item.ObjectURL)
	}
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		id := strings.TrimSpace(result.ID)
		expectedURL, ok := expectedByID[id]
		if !ok {
			return fmt.Errorf("bulk storage observation returned unrequested item ID %q", id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("bulk storage observation returned duplicate item ID %q", id)
		}
		seen[id] = struct{}{}
		actualURL := canonicalStorageProbeURL(result.ObjectURL)
		if actualURL == "" && strings.TrimSpace(result.Bucket) != "" && strings.TrimSpace(result.Key) != "" {
			actualURL = canonicalStorageProbeURL("s3://" + strings.TrimSpace(result.Bucket) + "/" + strings.Trim(strings.TrimSpace(result.Key), "/"))
		}
		if expectedURL == "" || actualURL != expectedURL {
			return fmt.Errorf("bulk storage observation for item %q returned URL %q, expected %q", id, actualURL, expectedURL)
		}
	}
	return nil
}

func canonicalStorageProbeURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return ""
	}
	rest := strings.TrimLeft(trimmed[len("s3://"):], "/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.Trim(strings.TrimSpace(parts[1]), "/") == "" {
		return ""
	}
	return "s3://" + strings.TrimSpace(parts[0]) + "/" + strings.Trim(strings.TrimSpace(parts[1]), "/")
}

func dedupeBulkStorageProbeItems(items []BulkStorageProbeItem, includeExpectedName bool) ([]BulkStorageProbeItem, map[string]string) {
	seen := make(map[string]BulkStorageProbeItem, len(items))
	keyByOriginalID := make(map[string]string, len(items))
	out := make([]BulkStorageProbeItem, 0, len(items))
	for _, item := range items {
		key := bulkStorageProbeDedupKey(item, includeExpectedName)
		originalID := strings.TrimSpace(item.ID)
		if existing, ok := seen[key]; ok {
			keyByOriginalID[originalID] = strings.TrimSpace(existing.ID)
			continue
		}
		seen[key] = item
		keyByOriginalID[originalID] = originalID
		out = append(out, item)
	}
	return out, keyByOriginalID
}

func bulkStorageProbeDedupKey(item BulkStorageProbeItem, includeExpectedName bool) string {
	parts := []string{strings.TrimSpace(item.ObjectURL)}
	if item.ExpectedSizeBytes == nil {
		parts = append(parts, "")
	} else {
		parts = append(parts, strconv.FormatInt(*item.ExpectedSizeBytes, 10))
	}
	if includeExpectedName {
		parts = append(parts, strings.TrimSpace(item.ExpectedName))
	} else {
		parts = append(parts, strings.TrimSpace(item.ExpectedSHA256))
	}
	return strings.Join(parts, "\x00")
}

func (manager *Manager) probeStorageObjectBatch(ctx context.Context, authorizationHeader string, requestPath string, items []BulkStorageProbeItem, includeExpectedName bool, confirmMissing bool) ([]BulkStorageProbeResult, error) {
	requestBody := struct {
		ConfirmMissing bool `json:"confirm_missing,omitempty"`
		Items          []struct {
			ID                string `json:"id,omitempty"`
			ObjectURL         string `json:"object_url,omitempty"`
			ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
			ExpectedSHA256    string `json:"expected_sha256,omitempty"`
			ExpectedName      string `json:"expected_name,omitempty"`
		} `json:"items"`
	}{ConfirmMissing: confirmMissing, Items: make([]struct {
		ID                string `json:"id,omitempty"`
		ObjectURL         string `json:"object_url,omitempty"`
		ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
		ExpectedSHA256    string `json:"expected_sha256,omitempty"`
		ExpectedName      string `json:"expected_name,omitempty"`
	}, 0, len(items))}
	for _, item := range items {
		row := struct {
			ID                string `json:"id,omitempty"`
			ObjectURL         string `json:"object_url,omitempty"`
			ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
			ExpectedSHA256    string `json:"expected_sha256,omitempty"`
			ExpectedName      string `json:"expected_name,omitempty"`
		}{
			ID:                strings.TrimSpace(item.ID),
			ObjectURL:         strings.TrimSpace(item.ObjectURL),
			ExpectedSizeBytes: item.ExpectedSizeBytes,
			ExpectedSHA256:    strings.TrimSpace(item.ExpectedSHA256),
		}
		if includeExpectedName {
			row.ExpectedName = strings.TrimSpace(item.ExpectedName)
		}
		requestBody.Items = append(requestBody.Items, row)
	}
	var response struct {
		Items []struct {
			ID                   string   `json:"id"`
			ObjectURL            string   `json:"object_url"`
			Provider             string   `json:"provider"`
			Bucket               string   `json:"bucket"`
			Key                  string   `json:"key"`
			Path                 string   `json:"path"`
			Exists               bool     `json:"exists"`
			Status               string   `json:"status"`
			Error                string   `json:"error"`
			ErrorKind            string   `json:"error_kind"`
			VerificationMethod   string   `json:"verification_method"`
			SizeBytes            *int64   `json:"size_bytes"`
			MetaSHA256           string   `json:"meta_sha256"`
			ETag                 string   `json:"etag"`
			LastModified         string   `json:"last_modified"`
			ValidationStatus     string   `json:"validation_status"`
			SizeMatch            *bool    `json:"size_match"`
			NameMatch            *bool    `json:"name_match"`
			SHA256Match          *bool    `json:"sha256_match"`
			ValidationMismatches []string `json:"validation_mismatches"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, requestPath, nil, requestBody, &response); err != nil {
		return nil, fmt.Errorf("bulk syfon storage object request %s: %w", requestPath, err)
	}
	out := make([]BulkStorageProbeResult, 0, len(response.Items))
	for _, item := range response.Items {
		out = append(out, BulkStorageProbeResult{
			ID:                   strings.TrimSpace(item.ID),
			ObjectURL:            strings.TrimSpace(item.ObjectURL),
			Provider:             strings.TrimSpace(item.Provider),
			Bucket:               strings.TrimSpace(item.Bucket),
			Key:                  strings.TrimSpace(item.Key),
			Path:                 strings.TrimSpace(item.Path),
			Exists:               item.Exists,
			Status:               strings.TrimSpace(item.Status),
			Error:                strings.TrimSpace(item.Error),
			ErrorKind:            strings.TrimSpace(item.ErrorKind),
			VerificationMethod:   strings.TrimSpace(item.VerificationMethod),
			SizeBytes:            item.SizeBytes,
			MetaSHA256:           strings.TrimSpace(item.MetaSHA256),
			ETag:                 strings.TrimSpace(item.ETag),
			LastModified:         strings.TrimSpace(item.LastModified),
			ValidationStatus:     strings.TrimSpace(item.ValidationStatus),
			SizeMatch:            item.SizeMatch,
			NameMatch:            item.NameMatch,
			SHA256Match:          item.SHA256Match,
			ValidationMismatches: append([]string(nil), item.ValidationMismatches...),
		})
	}
	return out, nil
}

func (manager *Manager) BulkListStorageObjects(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequest(ctx, authorizationHeader, "/data/inspect/bulk-list", items, true, false)
}

func (manager *Manager) BulkListStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem, onBatch func([]BulkStorageProbeItem, []BulkStorageProbeResult) error) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequestWithProgress(ctx, authorizationHeader, "/data/inspect/bulk-list", items, true, false, onBatch)
}

func (manager *Manager) ListProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*ProjectBucketSummary, error) {
	started := time.Now()
	trimmedOrg := strings.TrimSpace(organization)
	trimmedProject := strings.TrimSpace(project)
	trimmedMode := strings.TrimSpace(mode)
	log.Printf("INFO: syfon_project_bucket_summary_request_start organization=%s project=%s mode=%s", trimmedOrg, trimmedProject, trimmedMode)
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
		Mode         string `json:"mode,omitempty"`
	}{
		Organization: trimmedOrg,
		Project:      trimmedProject,
		Mode:         trimmedMode,
	}
	var response struct {
		Summary *struct {
			ObjectURL   string `json:"object_url"`
			Provider    string `json:"provider"`
			Bucket      string `json:"bucket"`
			Prefix      string `json:"prefix"`
			Exists      bool   `json:"exists"`
			ObjectCount int    `json:"object_count"`
			TotalBytes  int64  `json:"total_bytes"`
			ComputedAt  string `json:"computed_at"`
			Mode        string `json:"mode"`
		} `json:"summary"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-bucket", nil, requestBody, &response); err != nil {
		log.Printf("INFO: syfon_project_bucket_summary_request_done organization=%s project=%s mode=%s duration_ms=%d error=%q", trimmedOrg, trimmedProject, trimmedMode, time.Since(started).Milliseconds(), err.Error())
		return nil, fmt.Errorf("inspect syfon project bucket summary: %w", err)
	}
	if response.Summary == nil {
		log.Printf("INFO: syfon_project_bucket_summary_request_done organization=%s project=%s mode=%s duration_ms=%d error=%q", trimmedOrg, trimmedProject, trimmedMode, time.Since(started).Milliseconds(), "response summary is missing")
		return nil, fmt.Errorf("inspect syfon project bucket summary: response summary is missing")
	}
	log.Printf("INFO: syfon_project_bucket_summary_request_done organization=%s project=%s mode=%s exists=%t object_count=%d total_bytes=%d duration_ms=%d", trimmedOrg, trimmedProject, trimmedMode, response.Summary.Exists, response.Summary.ObjectCount, response.Summary.TotalBytes, time.Since(started).Milliseconds())
	return &ProjectBucketSummary{
		ObjectURL:   strings.TrimSpace(response.Summary.ObjectURL),
		Provider:    strings.TrimSpace(response.Summary.Provider),
		Bucket:      strings.TrimSpace(response.Summary.Bucket),
		Prefix:      strings.TrimSpace(response.Summary.Prefix),
		Exists:      response.Summary.Exists,
		ObjectCount: response.Summary.ObjectCount,
		TotalBytes:  response.Summary.TotalBytes,
		ComputedAt:  strings.TrimSpace(response.Summary.ComputedAt),
		Mode:        strings.TrimSpace(response.Summary.Mode),
	}, nil
}

func (manager *Manager) ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (ProjectBucketInventory, error) {
	return manager.listProjectBucketObjects(ctx, authorizationHeader, "/data/inspect/project-bucket", organization, project, pathPrefix)
}

func (manager *Manager) ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (ProjectBucketInventory, error) {
	return manager.listProjectBucketObjects(ctx, authorizationHeader, "/data/inspect/project-bucket/inventory", organization, project, pathPrefix)
}

func (manager *Manager) listProjectBucketObjects(ctx context.Context, authorizationHeader string, requestPath string, organization string, project string, pathPrefix string) (ProjectBucketInventory, error) {
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
		Mode         string `json:"mode,omitempty"`
		PathPrefix   string `json:"path_prefix,omitempty"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
		Mode:         "items",
		PathPrefix:   strings.Trim(strings.TrimSpace(pathPrefix), "/"),
	}
	var response struct {
		Summary *struct {
			InventoryComplete *bool  `json:"inventory_complete"`
			InventoryWarning  string `json:"inventory_warning"`
			ComputedAt        string `json:"computed_at"`
		} `json:"summary"`
		Items []struct {
			ObjectURL    string `json:"object_url"`
			Provider     string `json:"provider"`
			Bucket       string `json:"bucket"`
			Key          string `json:"key"`
			Path         string `json:"path"`
			SizeBytes    int64  `json:"size_bytes"`
			MetaSHA256   string `json:"meta_sha256"`
			ETag         string `json:"etag"`
			LastModified string `json:"last_modified"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, requestPath, nil, requestBody, &response); err != nil {
		return ProjectBucketInventory{}, fmt.Errorf("list syfon project bucket objects: %w", err)
	}
	complete := true
	warning := ""
	observedAt := ""
	if response.Summary != nil {
		if response.Summary.InventoryComplete != nil {
			complete = *response.Summary.InventoryComplete
		}
		warning = strings.TrimSpace(response.Summary.InventoryWarning)
		observedAt = strings.TrimSpace(response.Summary.ComputedAt)
	}
	out := make([]ProjectBucketObject, 0, len(response.Items))
	for _, item := range response.Items {
		out = append(out, ProjectBucketObject{
			ObjectURL:    strings.TrimSpace(item.ObjectURL),
			Provider:     strings.TrimSpace(item.Provider),
			Bucket:       strings.TrimSpace(item.Bucket),
			Key:          strings.TrimSpace(item.Key),
			Path:         strings.TrimSpace(item.Path),
			SizeBytes:    item.SizeBytes,
			MetaSHA256:   strings.TrimSpace(item.MetaSHA256),
			ETag:         strings.TrimSpace(item.ETag),
			LastModified: strings.TrimSpace(item.LastModified),
		})
	}
	return ProjectBucketInventory{Objects: out, Complete: complete, Warning: warning, ObservedAt: observedAt}, nil
}
