package syfon

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	metricsapi "github.com/calypr/syfon/apigen/client/metricsapi"
)

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
