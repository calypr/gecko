package syfon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	"github.com/calypr/gecko/internal/httpclient"
	"github.com/calypr/syfon/apigen/client/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/client/drs"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	metricsapi "github.com/calypr/syfon/apigen/client/metricsapi"
	syfonservices "github.com/calypr/syfon/client/services"
)

const refreshAuthzHeader = "X-Syfon-Refresh-Authz"
const bulkStorageProbeBatchSize = 200
const bulkStorageProbeConcurrency = 4

type HTTPError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("syfon %s %s failed with status %d: %s", err.Method, err.Path, err.Status, strings.TrimSpace(err.Body))
}

func IsHTTPStatus(err error, statuses ...int) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	for _, status := range statuses {
		if httpErr.Status == status {
			return true
		}
	}
	return false
}

type Manager struct {
	baseURL string
	client  *http.Client
}

type ProjectRecord struct {
	ObjectID      string
	Name          string
	Checksum      string
	Organization  string
	Project       string
	Size          int64
	UpdatedAt     *time.Time
	CreatedAt     *time.Time
	AccessURLs    []string
	AccessMethods []ProjectAccessMethod
}

type ProjectAccessMethod struct {
	AccessID string
	Type     string
	URL      string
	Headers  []string
}

type ProjectMetricsSummary struct {
	RecordCount             int
	RecordLatestUpdatedTime string
	RecordRevision          string
}

type BulkStorageProbeItem struct {
	ID                string
	ObjectURL         string
	ExpectedSizeBytes *int64
	ExpectedSHA256    string
	ExpectedName      string
}

type BulkStorageProbeResult struct {
	ID                   string
	Operation            string
	ObjectURL            string
	Provider             string
	Bucket               string
	Key                  string
	Path                 string
	Exists               bool
	Status               string
	Error                string
	ErrorKind            string
	SizeBytes            *int64
	MetaSHA256           string
	ETag                 string
	LastModified         string
	ValidationStatus     string
	SizeMatch            *bool
	NameMatch            *bool
	SHA256Match          *bool
	ValidationMismatches []string
}

type ProjectBucketSummary struct {
	ObjectURL   string
	Provider    string
	Bucket      string
	Prefix      string
	Exists      bool
	ObjectCount int
	TotalBytes  int64
	ComputedAt  string
	Mode        string
}

type ProjectBucketObject struct {
	ObjectURL    string
	Provider     string
	Bucket       string
	Key          string
	Path         string
	SizeBytes    int64
	MetaSHA256   string
	ETag         string
	LastModified string
}

type ProjectBucketInventory struct {
	Objects    []ProjectBucketObject
	Complete   bool
	Warning    string
	ObservedAt string
}

type FileUsage struct {
	ObjectID         string
	Name             string
	Size             int64
	DownloadCount    int64
	UploadCount      int64
	LastAccessTime   *time.Time
	LastDownloadTime *time.Time
	LastUploadTime   *time.Time
}

type ProjectBucketDeleteResult struct {
	ObjectURL string
	Status    string
	Error     string
}

// ProjectObjectRegistration is the narrow subset of DRS registration Gecko
// needs when it restores a Syfon record for an existing Git LFS pointer.
type ProjectObjectRegistration struct {
	Name             string
	Checksum         string
	Size             int64
	ControlledAccess []string
	AccessURLs       []string
}

type ProjectObjectRegistrationResult struct {
	ObjectID string
}

func NewManager(baseURL string, client *http.Client) *Manager {
	httpClient := client
	if httpClient == nil {
		httpClient = httpclient.NewServiceClient(5 * time.Minute)
	}
	return &Manager{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  httpClient,
	}
}

func (manager *Manager) ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error) {
	service, err := manager.bucketsService(authorizationHeader)
	if err != nil {
		return nil, err
	}
	response, err := service.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("request syfon bucket list: %w", err)
	}
	buckets := make(map[string]domain.StorageBucket, len(response.S3BUCKETS))
	for name, metadata := range response.S3BUCKETS {
		bucket := domain.StorageBucket{Bucket: name}
		if metadata.Provider != nil {
			bucket.Provider = strings.TrimSpace(*metadata.Provider)
		}
		if metadata.EndpointUrl != nil {
			bucket.Endpoint = strings.TrimSpace(*metadata.EndpointUrl)
		}
		if metadata.Region != nil {
			bucket.Region = strings.TrimSpace(*metadata.Region)
		}
		if metadata.Programs != nil {
			for _, resource := range *metadata.Programs {
				bucket.Resources = append(bucket.Resources, strings.TrimSpace(resource))
			}
		}
		buckets[name] = bucket
	}
	return buckets, nil
}

func (manager *Manager) PutBucket(ctx context.Context, authorizationHeader string, config domain.StorageConfig) error {
	service, err := manager.bucketsService(authorizationHeader)
	if err != nil {
		return err
	}
	request := bucketapi.PutBucketRequest{
		Bucket:       strings.TrimSpace(config.Bucket),
		Organization: strings.TrimSpace(config.Organization),
		ProjectId:    strings.TrimSpace(config.ProjectID),
	}
	if value := strings.TrimSpace(config.AccessKey); value != "" {
		request.AccessKey = &value
	}
	if value := strings.TrimSpace(config.Endpoint); value != "" {
		request.Endpoint = &value
	}
	if value := strings.TrimSpace(config.Provider); value != "" {
		request.Provider = &value
	}
	if value := strings.TrimSpace(config.Region); value != "" {
		request.Region = &value
	}
	if value := strings.TrimSpace(config.SecretKey); value != "" {
		request.SecretKey = &value
	}
	if value := strings.TrimSpace(config.Path); value != "" {
		request.Path = &value
	}
	if err := service.Put(ctx, request); err != nil {
		return fmt.Errorf("request syfon bucket upsert: %w", err)
	}
	return nil
}

func (manager *Manager) AddScope(ctx context.Context, authorizationHeader string, config domain.StorageConfig) error {
	service, err := manager.bucketsService(authorizationHeader)
	if err != nil {
		return err
	}
	request := bucketapi.AddBucketScopeRequest{
		Organization: strings.TrimSpace(config.Organization),
		ProjectId:    strings.TrimSpace(config.ProjectID),
	}
	if value := manager.scopePath(config); value != "" {
		request.Path = &value
	}
	if err := service.AddScope(ctx, strings.TrimSpace(config.Bucket), request); err != nil {
		return fmt.Errorf("request syfon add bucket scope: %w", err)
	}
	return nil
}

func (manager *Manager) ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error) {
	requestPath := "/data/buckets/" + url.PathEscape(strings.TrimSpace(bucket)) + "/scopes"
	var scopes []struct {
		Organization string  `json:"organization"`
		ProjectId    string  `json:"project_id"`
		Path         *string `json:"path"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodGet, requestPath, nil, nil, &scopes); err != nil {
		return nil, fmt.Errorf("request syfon bucket scopes: %w", err)
	}
	out := make([]domain.StorageBucketScope, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, domain.StorageBucketScope{
			Bucket:       strings.TrimSpace(bucket),
			Organization: strings.TrimSpace(scope.Organization),
			ProjectID:    strings.TrimSpace(scope.ProjectId),
			Path:         stringValue(scope.Path),
		})
	}
	return out, nil
}

func uniqueNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

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

func (manager *Manager) BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []BulkStorageProbeItem) ([]BulkStorageProbeResult, error) {
	return manager.bulkStorageObjectRequest(ctx, authorizationHeader, "/data/inspect/bulk", items, false)
}

func (manager *Manager) bulkStorageObjectRequest(ctx context.Context, authorizationHeader string, requestPath string, items []BulkStorageProbeItem, includeExpectedName bool) ([]BulkStorageProbeResult, error) {
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
	for start := 0; start < len(items); start += bulkStorageProbeBatchSize {
		end := start + bulkStorageProbeBatchSize
		if end > len(items) {
			end = len(items)
		}
		batchStart := start
		batch := append([]BulkStorageProbeItem(nil), items[start:end]...)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			batchStarted := time.Now()
			results, err := manager.probeStorageObjectBatch(ctx, authorizationHeader, requestPath, batch, includeExpectedName)
			batchMs := time.Since(batchStarted).Milliseconds()
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

func (manager *Manager) probeStorageObjectBatch(ctx context.Context, authorizationHeader string, requestPath string, items []BulkStorageProbeItem, includeExpectedName bool) ([]BulkStorageProbeResult, error) {
	requestBody := struct {
		Items []struct {
			ID                string `json:"id,omitempty"`
			ObjectURL         string `json:"object_url,omitempty"`
			ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
			ExpectedSHA256    string `json:"expected_sha256,omitempty"`
			ExpectedName      string `json:"expected_name,omitempty"`
		} `json:"items"`
	}{Items: make([]struct {
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
	return manager.bulkStorageObjectRequest(ctx, authorizationHeader, "/data/inspect/bulk-list", items, true)
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

func (manager *Manager) bucketsService(authorizationHeader string) (*syfonservices.BucketsService, error) {
	clientBaseURL, err := manager.clientBaseURL()
	if err != nil {
		return nil, err
	}
	client, err := bucketapi.NewClientWithResponses(clientBaseURL,
		bucketapi.WithHTTPClient(manager.client),
		bucketapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", authorizationHeader)
			req.Header.Set(refreshAuthzHeader, "true")
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create syfon bucket client: %w", err)
	}
	return syfonservices.NewBucketsService(client), nil
}

func (manager *Manager) drsClient(authorizationHeader string) (*drsapi.ClientWithResponses, error) {
	clientBaseURL, err := manager.drsAPIBaseURL()
	if err != nil {
		return nil, err
	}
	client, err := drsapi.NewClientWithResponses(clientBaseURL,
		drsapi.WithHTTPClient(manager.client),
		drsapi.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", authorizationHeader)
			req.Header.Set(refreshAuthzHeader, "true")
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create syfon drs client: %w", err)
	}
	return client, nil
}

func (manager *Manager) drsAPIBaseURL() (string, error) {
	clientBaseURL, err := manager.clientBaseURL()
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(clientBaseURL, "/ga4gh/drs/v1") {
		return clientBaseURL, nil
	}
	return strings.TrimRight(clientBaseURL+"/ga4gh/drs/v1", "/"), nil
}

func (manager *Manager) clientBaseURL() (string, error) {
	if manager.baseURL == "" {
		return "", fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(manager.baseURL), "/")
	if strings.HasSuffix(baseURL, "/data") {
		return strings.TrimSuffix(baseURL, "/data"), nil
	}
	return baseURL, nil
}

func (manager *Manager) dataAPIBaseURL() (string, error) {
	if manager.baseURL == "" {
		return "", fmt.Errorf("SYFON_DATA_API_BASE_URL is not configured")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(manager.baseURL), "/")
	if strings.HasSuffix(baseURL, "/data") {
		return baseURL, nil
	}
	return baseURL + "/data", nil
}

func (manager *Manager) scopePath(config domain.StorageConfig) string {
	if explicitPath := strings.TrimSpace(config.Path); explicitPath != "" {
		return explicitPath
	}
	if pathPrefix := strings.Trim(strings.TrimSpace(config.PathPrefix), "/"); pathPrefix != "" {
		return bucketPath(config.Provider, config.Bucket, pathPrefix)
	}
	organizationSubPath := strings.Trim(strings.TrimSpace(config.OrganizationSubPath), "/")
	projectSubPath := strings.Trim(strings.TrimSpace(config.ProjectSubPath), "/")
	if organizationSubPath == "" && projectSubPath == "" {
		return ""
	}
	return bucketPath(config.Provider, config.Bucket, path.Join(organizationSubPath, projectSubPath))
}

func bucketPath(provider string, bucket string, prefix string) string {
	cleanBucket := strings.TrimSpace(bucket)
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "gcs", "gs":
		if cleanPrefix == "" {
			return "gs://" + cleanBucket
		}
		return "gs://" + cleanBucket + "/" + cleanPrefix
	case "azure", "azblob", "az":
		if cleanPrefix == "" {
			return "azblob://" + cleanBucket
		}
		return "azblob://" + cleanBucket + "/" + cleanPrefix
	case "file":
		if cleanPrefix == "" {
			return "file://" + cleanBucket
		}
		return "file://" + cleanBucket + "/" + cleanPrefix
	default:
		if cleanPrefix == "" {
			return "s3://" + cleanBucket
		}
		return "s3://" + cleanBucket + "/" + cleanPrefix
	}
}

func (manager *Manager) requestJSON(ctx context.Context, authorizationHeader string, method string, requestPath string, query url.Values, requestBody any, out any) error {
	baseURL, err := manager.clientBaseURL()
	if err != nil {
		return err
	}
	queryURL, err := url.Parse(strings.TrimRight(baseURL, "/") + requestPath)
	if err != nil {
		return fmt.Errorf("parse syfon request url: %w", err)
	}
	if len(query) > 0 {
		queryURL.RawQuery = query.Encode()
	}
	var body io.Reader
	if requestBody != nil {
		bodyBytes, marshalErr := json.Marshal(requestBody)
		if marshalErr != nil {
			return fmt.Errorf("marshal syfon request body: %w", marshalErr)
		}
		body = strings.NewReader(string(bodyBytes))
	}
	req, err := http.NewRequestWithContext(ctx, method, queryURL.String(), body)
	if err != nil {
		return fmt.Errorf("build syfon request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set(refreshAuthzHeader, "true")
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := manager.client.Do(req)
	if err != nil {
		return fmt.Errorf("request syfon %s %s: %w", method, requestPath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &HTTPError{
			Method: method,
			Path:   requestPath,
			Status: resp.StatusCode,
			Body:   strings.TrimSpace(string(bodyBytes)),
		}
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode syfon %s %s response: %w", method, requestPath, err)
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

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func parseOptionalTime(value *string) *time.Time {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*value))
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func dedupeChecksums(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeSHA256(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
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

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(strings.TrimPrefix(value, "sha256:"))
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func normalizeSHA256(value string) string {
	trimmed := strings.TrimSpace(strings.TrimPrefix(value, "sha256:"))
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}
