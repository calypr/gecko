package syfon

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	drsapi "github.com/calypr/syfon/apigen/client/drs"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	metricsapi "github.com/calypr/syfon/apigen/client/metricsapi"
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

func (manager *Manager) ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]FileUsage, error) {
	out := make(map[string]FileUsage)
	offset := 0
	for {
		params := url.Values{}
		params.Set("organization", strings.TrimSpace(organization))
		params.Set("project", strings.TrimSpace(project))
		params.Set("limit", "1000")
		params.Set("offset", strconv.Itoa(offset))
		if inactiveDays > 0 {
			params.Set("inactive_days", strconv.Itoa(inactiveDays))
		}
		var response metricsapi.MetricsListResponse
		if err := manager.requestJSON(ctx, authorizationHeader, http.MethodGet, "/index/v1/metrics/files", params, nil, &response); err != nil {
			return nil, fmt.Errorf("list syfon metrics files offset %d: %w", offset, err)
		}
		items := []metricsapi.FileUsage{}
		if response.Data != nil {
			items = *response.Data
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			objectID := strings.TrimSpace(stringValue(item.ObjectId))
			if objectID == "" {
				continue
			}
			out[objectID] = FileUsage{
				ObjectID:         objectID,
				Name:             stringValue(item.Name),
				Size:             int64Value(item.Size),
				DownloadCount:    int64Value(item.DownloadCount),
				UploadCount:      int64Value(item.UploadCount),
				LastAccessTime:   item.LastAccessTime,
				LastDownloadTime: item.LastDownloadTime,
				LastUploadTime:   item.LastUploadTime,
			}
		}
		if len(items) < 1000 {
			break
		}
		offset += len(items)
	}
	return out, nil
}

func (manager *Manager) ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]FileUsage, error) {
	out := make(map[string]FileUsage)
	ids := uniqueNonEmptyStrings(objectIDs)
	if len(ids) == 0 {
		return out, nil
	}
	params := url.Values{}
	params.Set("organization", strings.TrimSpace(organization))
	params.Set("project", strings.TrimSpace(project))
	request := struct {
		ObjectIDs    []string `json:"object_ids"`
		InactiveDays *int     `json:"inactive_days,omitempty"`
	}{
		ObjectIDs: ids,
	}
	if inactiveDays > 0 {
		request.InactiveDays = &inactiveDays
	}
	var response metricsapi.MetricsListResponse
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/index/v1/metrics/files/bulk", params, request, &response); err != nil {
		return nil, fmt.Errorf("bulk list syfon metrics files: %w", err)
	}
	if response.Data == nil {
		return out, nil
	}
	for _, item := range *response.Data {
		objectID := strings.TrimSpace(stringValue(item.ObjectId))
		if objectID == "" {
			continue
		}
		out[objectID] = FileUsage{
			ObjectID:         objectID,
			Name:             stringValue(item.Name),
			Size:             int64Value(item.Size),
			DownloadCount:    int64Value(item.DownloadCount),
			UploadCount:      int64Value(item.UploadCount),
			LastAccessTime:   item.LastAccessTime,
			LastDownloadTime: item.LastDownloadTime,
			LastUploadTime:   item.LastUploadTime,
		}
	}
	return out, nil
}

func (manager *Manager) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	normalized := dedupeStrings(objectIDs)
	if len(normalized) == 0 {
		return nil
	}
	resp, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return err
	}
	deleteMetadata := true
	requestBody := drsapi.BulkDeleteObjectsJSONRequestBody{
		BulkObjectIds:        normalized,
		DeleteObjectMetadata: &deleteMetadata,
		DeleteStorageData:    &deleteStorageData,
	}
	response, err := resp.BulkDeleteObjectsWithResponse(ctx, requestBody)
	if err != nil {
		return fmt.Errorf("bulk delete syfon objects: %w", err)
	}
	if response.StatusCode() != http.StatusOK && response.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("bulk delete syfon objects failed with status %d", response.StatusCode())
	}
	return nil
}

func (manager *Manager) RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []ProjectObjectRegistration) ([]ProjectObjectRegistrationResult, error) {
	if len(candidates) == 0 {
		return []ProjectObjectRegistrationResult{}, nil
	}
	client, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return nil, err
	}
	request := drsapi.RegisterObjectsJSONRequestBody{Candidates: make([]drsapi.DrsObjectCandidate, 0, len(candidates))}
	for _, candidate := range candidates {
		name := strings.TrimSpace(candidate.Name)
		checksum := normalizeSHA256(candidate.Checksum)
		accessURLs := uniqueNonEmptyStrings(candidate.AccessURLs)
		if name == "" || checksum == "" || candidate.Size < 0 || len(accessURLs) == 0 {
			return nil, fmt.Errorf("register Syfon object: candidate is missing a name, SHA-256, size, or access URL")
		}
		accessMethods := make([]drsapi.AccessMethod, 0, len(accessURLs))
		for index, accessURL := range accessURLs {
			accessID := fmt.Sprintf("s3-%d", index+1)
			accessURLCopy := accessURL
			accessMethods = append(accessMethods, drsapi.AccessMethod{
				AccessId: &accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: accessURLCopy},
				Type: drsapi.AccessMethodType("s3"),
			})
		}
		controlledAccess := uniqueNonEmptyStrings(candidate.ControlledAccess)
		request.Candidates = append(request.Candidates, drsapi.DrsObjectCandidate{
			Name:             &name,
			Size:             candidate.Size,
			Checksums:        []drsapi.Checksum{{Type: "sha256", Checksum: checksum}},
			ControlledAccess: &controlledAccess,
			AccessMethods:    &accessMethods,
		})
	}
	response, err := client.RegisterObjectsWithResponse(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("register Syfon objects: %w", err)
	}
	if response.StatusCode() != http.StatusCreated || response.JSON201 == nil {
		return nil, fmt.Errorf("register Syfon objects failed with status %d: %s", response.StatusCode(), strings.TrimSpace(string(response.Body)))
	}
	if len(response.JSON201.Objects) != len(candidates) {
		return nil, fmt.Errorf("register Syfon objects: expected %d objects, received %d", len(candidates), len(response.JSON201.Objects))
	}
	results := make([]ProjectObjectRegistrationResult, len(response.JSON201.Objects))
	for index, object := range response.JSON201.Objects {
		results[index] = ProjectObjectRegistrationResult{ObjectID: strings.TrimSpace(object.Id)}
	}
	return results, nil
}

func (manager *Manager) DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]ProjectBucketDeleteResult, error) {
	requestBody := struct {
		Organization string   `json:"organization,omitempty"`
		Project      string   `json:"project,omitempty"`
		ObjectURLs   []string `json:"object_urls"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
		ObjectURLs:   dedupeStrings(objectURLs),
	}
	if len(requestBody.ObjectURLs) == 0 {
		return []ProjectBucketDeleteResult{}, nil
	}
	var response struct {
		Items []struct {
			ObjectURL string `json:"object_url"`
			Status    string `json:"status"`
			Error     string `json:"error,omitempty"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-bucket/delete", nil, requestBody, &response); err != nil {
		return nil, fmt.Errorf("delete syfon project bucket objects: %w", err)
	}
	out := make([]ProjectBucketDeleteResult, 0, len(response.Items))
	for _, item := range response.Items {
		out = append(out, ProjectBucketDeleteResult{
			ObjectURL: strings.TrimSpace(item.ObjectURL),
			Status:    strings.TrimSpace(item.Status),
			Error:     strings.TrimSpace(item.Error),
		})
	}
	return out, nil
}

func (manager *Manager) BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]ProjectAccessMethod) error {
	if len(updates) == 0 {
		return nil
	}
	resp, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return err
	}
	const batchSize = 200
	objectIDs := make([]string, 0, len(updates))
	for objectID := range updates {
		if trimmed := strings.TrimSpace(objectID); trimmed != "" {
			objectIDs = append(objectIDs, trimmed)
		}
	}
	sort.Strings(objectIDs)
	for start := 0; start < len(objectIDs); start += batchSize {
		end := start + batchSize
		if end > len(objectIDs) {
			end = len(objectIDs)
		}
		body := drsapi.BulkUpdateAccessMethodsJSONRequestBody{
			Updates: make([]struct {
				AccessMethods []drsapi.AccessMethod `json:"access_methods"`
				ObjectId      string                `json:"object_id"`
			}, 0, end-start),
		}
		for _, objectID := range objectIDs[start:end] {
			methods := projectAccessMethodsToDRS(updates[objectID])
			if len(methods) == 0 {
				continue
			}
			body.Updates = append(body.Updates, struct {
				AccessMethods []drsapi.AccessMethod `json:"access_methods"`
				ObjectId      string                `json:"object_id"`
			}{
				ObjectId:      objectID,
				AccessMethods: methods,
			})
		}
		if len(body.Updates) == 0 {
			continue
		}
		response, err := resp.BulkUpdateAccessMethodsWithResponse(ctx, body)
		if err != nil {
			return fmt.Errorf("bulk update syfon access methods: %w", err)
		}
		if response.StatusCode() != http.StatusOK {
			return fmt.Errorf("bulk update syfon access methods failed with status %d", response.StatusCode())
		}
	}
	return nil
}

func projectRecordFromInternal(record internalapi.InternalRecord) (ProjectRecord, bool) {
	checksum := ""
	if record.Hashes != nil {
		checksum = normalizeSHA256((*record.Hashes)["sha256"])
	}
	if checksum == "" {
		return ProjectRecord{}, false
	}
	accessURLs := make([]string, 0)
	accessMethods := make([]ProjectAccessMethod, 0)
	if record.AccessMethods != nil {
		for _, method := range *record.AccessMethods {
			projectMethod := ProjectAccessMethod{
				Type:    strings.TrimSpace(string(method.Type)),
				Headers: []string{},
			}
			if method.AccessId != nil {
				projectMethod.AccessID = strings.TrimSpace(*method.AccessId)
			}
			if method.AccessUrl != nil {
				projectMethod.URL = strings.TrimSpace(method.AccessUrl.Url)
				accessURLs = append(accessURLs, projectMethod.URL)
				if method.AccessUrl.Headers != nil {
					projectMethod.Headers = append([]string(nil), (*method.AccessUrl.Headers)...)
				}
			}
			accessMethods = append(accessMethods, projectMethod)
		}
	}
	return ProjectRecord{
		ObjectID:      strings.TrimSpace(record.Did),
		Checksum:      checksum,
		Organization:  stringValue(record.Organization),
		Project:       stringValue(record.Project),
		Size:          int64Value(record.Size),
		UpdatedAt:     parseOptionalTime(record.UpdatedTime),
		CreatedAt:     parseOptionalTime(record.CreatedTime),
		AccessURLs:    accessURLs,
		AccessMethods: accessMethods,
	}, true
}

func projectAccessMethodsToDRS(methods []ProjectAccessMethod) []drsapi.AccessMethod {
	out := make([]drsapi.AccessMethod, 0, len(methods))
	for _, method := range methods {
		accessMethod := drsapi.AccessMethod{}
		if trimmed := strings.TrimSpace(method.AccessID); trimmed != "" {
			accessMethod.AccessId = &trimmed
		}
		if trimmed := strings.TrimSpace(method.Type); trimmed != "" {
			accessMethod.Type = drsapi.AccessMethodType(trimmed)
		}
		if trimmed := strings.TrimSpace(method.URL); trimmed != "" {
			accessURL := struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: trimmed}
			if len(method.Headers) > 0 {
				headers := append([]string(nil), method.Headers...)
				accessURL.Headers = &headers
			}
			accessMethod.AccessUrl = &accessURL
		}
		out = append(out, accessMethod)
	}
	return out
}

func recordMatchesScope(record ProjectRecord, organization string, project string) bool {
	recordOrg := strings.TrimSpace(record.Organization)
	recordProject := strings.TrimSpace(record.Project)
	if recordOrg == "" && recordProject == "" {
		return true
	}
	return strings.EqualFold(recordOrg, organization) && strings.EqualFold(recordProject, project)
}
