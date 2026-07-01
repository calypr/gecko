package syfon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	"github.com/calypr/syfon/apigen/client/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/client/drs"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
	metricsapi "github.com/calypr/syfon/apigen/client/metricsapi"
	syfonservices "github.com/calypr/syfon/client/services"
)

const refreshAuthzHeader = "X-Syfon-Refresh-Authz"
const bulkStorageProbeBatchSize = 200

type Manager struct {
	baseURL string
	client  *http.Client
}

type ProjectRecord struct {
	ObjectID      string
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

type BulkStorageProbeItem struct {
	ID                string
	ObjectURL         string
	ExpectedSizeBytes *int64
	ExpectedSHA256    string
}

type BulkStorageProbeResult struct {
	ID                   string
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
	SHA256Match          *bool
	ValidationMismatches []string
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

func NewManager(baseURL string, client *http.Client) *Manager {
	httpClient := client
	if httpClient == nil {
		httpClient = http.DefaultClient
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

func (manager *Manager) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]ProjectRecord, error) {
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
	}
	var response struct {
		Items []struct {
			ObjectID      string   `json:"object_id"`
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
			accessMethods = append(accessMethods, ProjectAccessMethod{
				AccessID: strings.TrimSpace(method.AccessID),
				Type:     strings.TrimSpace(method.Type),
				URL:      strings.TrimSpace(method.URL),
				Headers:  append([]string(nil), method.Headers...),
			})
		}
		out = append(out, ProjectRecord{
			ObjectID:      strings.TrimSpace(item.ObjectID),
			Checksum:      checksum,
			Organization:  strings.TrimSpace(item.Organization),
			Project:       strings.TrimSpace(item.Project),
			Size:          item.Size,
			CreatedAt:     parseOptionalTime(optionalString(item.CreatedTime)),
			UpdatedAt:     parseOptionalTime(optionalString(item.UpdatedTime)),
			AccessURLs:    accessURLs,
			AccessMethods: accessMethods,
		})
	}
	return out, nil
}

func (manager *Manager) ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
	}
	var response struct {
		Items []struct {
			Bucket       string `json:"bucket"`
			Organization string `json:"organization"`
			ProjectID    string `json:"project_id"`
			Path         string `json:"path"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-scopes", nil, requestBody, &response); err != nil {
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

func (manager *Manager) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	normalized := dedupeStrings(objectIDs)
	if len(normalized) == 0 {
		return nil
	}
	requestBody := drsapi.BulkDeleteObjectsJSONRequestBody{BulkObjectIds: normalized}
	if deleteStorageData {
		requestBody.DeleteStorageData = &deleteStorageData
	}
	resp, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return err
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
	if len(items) == 0 {
		return []BulkStorageProbeResult{}, nil
	}
	out := make([]BulkStorageProbeResult, 0, len(items))
	for start := 0; start < len(items); start += bulkStorageProbeBatchSize {
		end := start + bulkStorageProbeBatchSize
		if end > len(items) {
			end = len(items)
		}
		requestBody := struct {
			Items []struct {
				ID                string `json:"id,omitempty"`
				ObjectURL         string `json:"object_url,omitempty"`
				ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
				ExpectedSHA256    string `json:"expected_sha256,omitempty"`
			} `json:"items"`
		}{Items: make([]struct {
			ID                string `json:"id,omitempty"`
			ObjectURL         string `json:"object_url,omitempty"`
			ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
			ExpectedSHA256    string `json:"expected_sha256,omitempty"`
		}, 0, end-start)}
		for _, item := range items[start:end] {
			requestBody.Items = append(requestBody.Items, struct {
				ID                string `json:"id,omitempty"`
				ObjectURL         string `json:"object_url,omitempty"`
				ExpectedSizeBytes *int64 `json:"expected_size_bytes,omitempty"`
				ExpectedSHA256    string `json:"expected_sha256,omitempty"`
			}{
				ID:                strings.TrimSpace(item.ID),
				ObjectURL:         strings.TrimSpace(item.ObjectURL),
				ExpectedSizeBytes: item.ExpectedSizeBytes,
				ExpectedSHA256:    strings.TrimSpace(item.ExpectedSHA256),
			})
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
				SHA256Match          *bool    `json:"sha256_match"`
				ValidationMismatches []string `json:"validation_mismatches"`
			} `json:"items"`
		}
		if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/bulk", nil, requestBody, &response); err != nil {
			return nil, fmt.Errorf("bulk probe syfon storage objects: %w", err)
		}
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
				SHA256Match:          item.SHA256Match,
				ValidationMismatches: append([]string(nil), item.ValidationMismatches...),
			})
		}
	}
	return out, nil
}

func (manager *Manager) ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string) ([]ProjectBucketObject, error) {
	requestBody := struct {
		Organization string `json:"organization,omitempty"`
		Project      string `json:"project,omitempty"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
	}
	var response struct {
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
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-bucket", nil, requestBody, &response); err != nil {
		return nil, fmt.Errorf("list syfon project bucket objects: %w", err)
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
	return out, nil
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
	clientBaseURL, err := manager.clientBaseURL()
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
		return fmt.Errorf("syfon %s %s failed with status %d: %s", method, requestPath, resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
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
