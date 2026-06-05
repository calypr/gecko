package syfon

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	"github.com/calypr/syfon/apigen/client/bucketapi"
	syfonservices "github.com/calypr/syfon/client/services"
)

const refreshAuthzHeader = "X-Syfon-Refresh-Authz"

type Manager struct {
	baseURL string
	client  *http.Client
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
