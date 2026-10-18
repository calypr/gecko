package syfon

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
)

func (manager *Manager) CleanupProject(ctx context.Context, authorizationHeader string, organization string, project string) error {
	dataBaseURL, err := manager.dataAPIBaseURL()
	if err != nil {
		return err
	}
	cleanupURL := dataBaseURL +
		"/projects/" +
		url.PathEscape(strings.TrimSpace(organization)) +
		"/" +
		url.PathEscape(strings.TrimSpace(project))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, cleanupURL, nil)
	if err != nil {
		return fmt.Errorf("build syfon project cleanup request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set(refreshAuthzHeader, "true")
	resp, err := manager.client.Do(req)
	if err != nil {
		return fmt.Errorf("request syfon project cleanup: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("syfon project cleanup failed with status %d", resp.StatusCode)
	}
	return nil
}

func (manager *Manager) ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]ProjectRecord, error) {
	params := url.Values{}
	params.Set("organization", strings.TrimSpace(organization))
	params.Set("project", strings.TrimSpace(project))
	page := 1
	out := make([]ProjectRecord, 0)
	for {
		params.Set("limit", "1000")
		params.Set("page", strconv.Itoa(page))
		var response internalapi.ListRecordsResponse
		if err := manager.requestJSON(ctx, authorizationHeader, http.MethodGet, "/index", params, nil, &response); err != nil {
			return nil, fmt.Errorf("list syfon project records page %d: %w", page, err)
		}
		records := []internalapi.InternalRecord{}
		if response.Records != nil {
			records = *response.Records
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			projectRecord, ok := projectRecordFromInternal(record)
			if ok {
				out = append(out, projectRecord)
			}
		}
		if len(records) < 1000 {
			break
		}
		page++
	}
	return out, nil
}

func (manager *Manager) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]ProjectRecord, error) {
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
		PathPrefix   string `json:"path_prefix,omitempty"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
		PathPrefix:   strings.Trim(strings.TrimSpace(pathPrefix), "/"),
	}
	var response struct {
		Items []struct {
			ObjectID      string   `json:"object_id"`
			Name          string   `json:"name"`
			Checksum      string   `json:"checksum"`
			Organization  string   `json:"organization"`
			Project       string   `json:"project"`
			Size          int64    `json:"size"`
			CreatedTime   string   `json:"created_time"`
			UpdatedTime   string   `json:"updated_time"`
			AccessURLs    []string `json:"access_urls"`
			AccessMethods []struct {
				AccessID string   `json:"access_id"`
				Type     string   `json:"type"`
				URL      string   `json:"url"`
				Headers  []string `json:"headers"`
			} `json:"access_methods"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-records", nil, requestBody, &response); err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	out := make([]ProjectRecord, 0, len(response.Items))
	for _, item := range response.Items {
		checksum := normalizeSHA256(item.Checksum)
		if checksum == "" {
			continue
		}
		accessURLs := make([]string, 0, len(item.AccessURLs))
		for _, raw := range item.AccessURLs {
			if trimmed := strings.TrimSpace(raw); trimmed != "" {
				accessURLs = append(accessURLs, trimmed)
			}
		}
		accessMethods := make([]ProjectAccessMethod, 0, len(item.AccessMethods))
		for _, method := range item.AccessMethods {
			methodURL := strings.TrimSpace(method.URL)
			accessMethods = append(accessMethods, ProjectAccessMethod{
				AccessID: strings.TrimSpace(method.AccessID),
				Type:     strings.TrimSpace(method.Type),
				URL:      methodURL,
				Headers:  append([]string(nil), method.Headers...),
			})
			if methodURL != "" {
				accessURLs = append(accessURLs, methodURL)
			}
		}
		out = append(out, ProjectRecord{
			ObjectID:      strings.TrimSpace(item.ObjectID),
			Name:          strings.TrimSpace(item.Name),
			Checksum:      checksum,
			Organization:  strings.TrimSpace(item.Organization),
			Project:       strings.TrimSpace(item.Project),
			Size:          item.Size,
			CreatedAt:     parseOptionalTime(optionalString(item.CreatedTime)),
			UpdatedAt:     parseOptionalTime(optionalString(item.UpdatedTime)),
			AccessURLs:    uniqueNonEmptyStrings(accessURLs),
			AccessMethods: accessMethods,
		})
	}
	return out, nil
}

func (manager *Manager) GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*ProjectMetricsSummary, error) {
	params := url.Values{}
	params.Set("organization", strings.TrimSpace(organization))
	params.Set("project", strings.TrimSpace(project))
	var response struct {
		RecordCount             *int   `json:"record_count"`
		RecordLatestUpdatedTime string `json:"record_latest_updated_time"`
		RecordRevision          string `json:"record_revision"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodGet, "/index/v1/metrics/summary", params, nil, &response); err != nil {
		return nil, fmt.Errorf("get syfon metrics summary: %w", err)
	}
	if response.RecordCount == nil {
		return nil, nil
	}
	return &ProjectMetricsSummary{
		RecordCount:             *response.RecordCount,
		RecordLatestUpdatedTime: strings.TrimSpace(response.RecordLatestUpdatedTime),
		RecordRevision:          strings.TrimSpace(response.RecordRevision),
	}, nil
}

func (manager *Manager) ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	params := url.Values{}
	params.Set("organization", strings.TrimSpace(organization))
	params.Set("project", strings.TrimSpace(project))
	var response struct {
		Items []struct {
			Bucket       string `json:"bucket"`
			Organization string `json:"organization"`
			ProjectID    string `json:"project_id"`
			Path         string `json:"path"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodGet, "/data/inspect/project-scopes", params, nil, &response); err != nil {
		return nil, fmt.Errorf("list syfon project scopes: %w", err)
	}
	out := make([]domain.StorageBucketScope, 0, len(response.Items))
	for _, item := range response.Items {
		out = append(out, domain.StorageBucketScope{
			Bucket:       strings.TrimSpace(item.Bucket),
			Organization: strings.TrimSpace(item.Organization),
			ProjectID:    strings.TrimSpace(item.ProjectID),
			Path:         strings.TrimSpace(item.Path),
		})
	}
	return out, nil
}

func (manager *Manager) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]ProjectRecord, error) {
	normalized := dedupeChecksums(checksums)
	out := make(map[string][]ProjectRecord, len(normalized))
	if len(normalized) == 0 {
		return out, nil
	}
	const batchSize = 200
	for start := 0; start < len(normalized); start += batchSize {
		end := start + batchSize
		if end > len(normalized) {
			end = len(normalized)
		}
		requestBody := internalapi.BulkHashesRequest{Hashes: normalized[start:end]}
		var response struct {
			Results map[string][]internalapi.InternalRecord `json:"results"`
			Records *[]internalapi.InternalRecord           `json:"records"`
		}
		if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/index/bulk/hashes", nil, requestBody, &response); err != nil {
			return nil, fmt.Errorf("bulk lookup syfon checksums: %w", err)
		}
		if len(response.Results) > 0 {
			for _, records := range response.Results {
				for _, record := range records {
					projectRecord, ok := projectRecordFromInternal(record)
					if !ok {
						continue
					}
					if !recordMatchesScope(projectRecord, organization, project) {
						continue
					}
					out[projectRecord.Checksum] = append(out[projectRecord.Checksum], projectRecord)
				}
			}
			continue
		}
		if response.Records != nil {
			for _, record := range *response.Records {
				projectRecord, ok := projectRecordFromInternal(record)
				if !ok {
					continue
				}
				if !recordMatchesScope(projectRecord, organization, project) {
					continue
				}
				out[projectRecord.Checksum] = append(out[projectRecord.Checksum], projectRecord)
			}
		}
	}
	return out, nil
}
