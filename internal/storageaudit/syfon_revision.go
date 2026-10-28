package storageaudit

import (
	"context"
	"fmt"
	"strings"
)

func (service *StorageAnalyticsService) loadStorageAuditSyfonRevision(ctx context.Context, authorizationHeader string, organization string, project string) (string, error) {
	summary, err := service.storage.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return "", fmt.Errorf("get Syfon revision for storage audit: %w", err)
	}
	if summary == nil {
		return "", fmt.Errorf("get Syfon revision for storage audit: response summary is missing")
	}
	return fmt.Sprintf(
		"%d:%s:%s",
		summary.RecordCount,
		strings.TrimSpace(summary.RecordLatestUpdatedTime),
		strings.TrimSpace(summary.RecordRevision),
	), nil
}
