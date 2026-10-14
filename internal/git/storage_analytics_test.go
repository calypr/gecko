package git

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

type fakeStorageAnalyticsBackend struct {
	projectRecords                       []gintegrationsyfon.ProjectRecord
	projectMetricsSummary                *gintegrationsyfon.ProjectMetricsSummary
	bulkRecords                          map[string][]gintegrationsyfon.ProjectRecord
	buckets                              map[string]domain.StorageBucket
	bucketScopes                         map[string][]domain.StorageBucketScope
	projectScopes                        []domain.StorageBucketScope
	usageByObject                        map[string]gintegrationsyfon.FileUsage
	bulkChecksums                        []string
	probeResults                         map[string]gintegrationsyfon.BulkStorageProbeResult
	probeResultsByURL                    map[string]gintegrationsyfon.BulkStorageProbeResult
	listProbeResults                     map[string]gintegrationsyfon.BulkStorageProbeResult
	bucketObjects                        []gintegrationsyfon.ProjectBucketObject
	bucketInventoryIncomplete            bool
	bucketInventoryWarning               string
	bucketInventoryObservedAt            string
	listProjectBucketObjectsErr          error
	listBucketsCalls                     int
	listBucketScopesCalls                int
	listProjectAuditRecordsCalls         int
	listProjectAuditRecordsPathPrefix    string
	listProjectAuditRecordsDelay         time.Duration
	getProjectMetricsSummaryCalls        int
	listProjectScopesCalls               int
	listProjectFileUsageCalls            int
	listProjectFileUsageByObjectIDsCalls int
	listProjectFileUsageObjectIDs        []string
	listProjectFileUsageByObjectIDsErr   error
	listProjectBucketObjectsCalls        int
	listProjectBucketObjectsPathPrefix   string
	listProjectBucketInventoryCalls      int
	listProjectBucketInventoryPathPrefix string
	listProjectBucketInventoryDelay      time.Duration
	listProjectBucketInventoryStarted    chan struct{}
	listProjectBucketInventoryRelease    <-chan struct{}
	listProjectBucketInventoryStartOnce  sync.Once
	listProjectBucketSummaryCalls        int
	listProjectBucketSummaryMode         string
	bulkGetProjectRecordsCalls           int
	bulkGetDelay                         time.Duration
	probeCalls                           int
	probeItems                           []gintegrationsyfon.BulkStorageProbeItem
	listProbeCalls                       int
	listProbeItems                       []gintegrationsyfon.BulkStorageProbeItem
	deletedIDs                           []string
	deletedStorageIDs                    []string
	updatedAccessMethods                 map[string][]gintegrationsyfon.ProjectAccessMethod
	deletedBucketObjects                 []string
	registeredObjects                    []gintegrationsyfon.ProjectObjectRegistration
}

func (fake *fakeStorageAnalyticsBackend) ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error) {
	fake.listBucketsCalls++
	out := make(map[string]domain.StorageBucket, len(fake.buckets))
	for bucket, metadata := range fake.buckets {
		out[bucket] = metadata
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error) {
	fake.listBucketScopesCalls++
	return append([]domain.StorageBucketScope(nil), fake.bucketScopes[bucket]...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	return append([]gintegrationsyfon.ProjectRecord(nil), fake.projectRecords...), nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error) {
	fake.listProjectAuditRecordsCalls++
	fake.listProjectAuditRecordsPathPrefix = pathPrefix
	if fake.listProjectAuditRecordsDelay > 0 {
		time.Sleep(fake.listProjectAuditRecordsDelay)
	}
	return append([]gintegrationsyfon.ProjectRecord(nil), fake.projectRecords...), nil
}

func (fake *fakeStorageAnalyticsBackend) GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error) {
	fake.getProjectMetricsSummaryCalls++
	if fake.projectMetricsSummary != nil {
		summary := *fake.projectMetricsSummary
		return &summary, nil
	}
	latest := ""
	for _, record := range fake.projectRecords {
		if record.UpdatedAt == nil {
			continue
		}
		formatted := record.UpdatedAt.UTC().Format(time.RFC3339Nano)
		if formatted > latest {
			latest = formatted
		}
	}
	return &gintegrationsyfon.ProjectMetricsSummary{
		RecordCount:             len(fake.projectRecords),
		RecordLatestUpdatedTime: latest,
	}, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	fake.listProjectScopesCalls++
	if fake.projectScopes != nil {
		return append([]domain.StorageBucketScope(nil), fake.projectScopes...), nil
	}
	out := make([]domain.StorageBucketScope, 0)
	for _, scopes := range fake.bucketScopes {
		out = append(out, scopes...)
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	fake.bulkGetProjectRecordsCalls++
	fake.bulkChecksums = append([]string(nil), checksums...)
	if fake.bulkGetDelay > 0 {
		time.Sleep(fake.bulkGetDelay)
	}
	if fake.bulkRecords != nil {
		out := make(map[string][]gintegrationsyfon.ProjectRecord, len(fake.bulkRecords))
		for checksum, records := range fake.bulkRecords {
			out[checksum] = append([]gintegrationsyfon.ProjectRecord(nil), records...)
		}
		return out, nil
	}
	allowed := make(map[string]struct{}, len(checksums))
	for _, checksum := range checksums {
		allowed[strings.TrimSpace(checksum)] = struct{}{}
	}
	out := make(map[string][]gintegrationsyfon.ProjectRecord)
	for _, record := range fake.projectRecords {
		if _, ok := allowed[record.Checksum]; !ok {
			continue
		}
		if record.Organization != organization || record.Project != project {
			continue
		}
		out[record.Checksum] = append(out[record.Checksum], record)
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	fake.listProjectFileUsageCalls++
	out := make(map[string]gintegrationsyfon.FileUsage, len(fake.usageByObject))
	for objectID, usage := range fake.usageByObject {
		out[objectID] = usage
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error) {
	fake.listProjectFileUsageByObjectIDsCalls++
	fake.listProjectFileUsageObjectIDs = append(fake.listProjectFileUsageObjectIDs, objectIDs...)
	if fake.listProjectFileUsageByObjectIDsErr != nil {
		return nil, fake.listProjectFileUsageByObjectIDsErr
	}
	out := make(map[string]gintegrationsyfon.FileUsage, len(objectIDs))
	for _, objectID := range objectIDs {
		if usage, ok := fake.usageByObject[objectID]; ok {
			out[objectID] = usage
		}
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error) {
	fake.listProjectBucketObjectsCalls++
	fake.listProjectBucketObjectsPathPrefix = pathPrefix
	if fake.listProjectBucketObjectsErr != nil {
		return gintegrationsyfon.ProjectBucketInventory{}, fake.listProjectBucketObjectsErr
	}
	return gintegrationsyfon.ProjectBucketInventory{
		Objects:    append([]gintegrationsyfon.ProjectBucketObject(nil), fake.bucketObjects...),
		Complete:   !fake.bucketInventoryIncomplete,
		Warning:    fake.bucketInventoryWarning,
		ObservedAt: fake.bucketInventoryObservedAt,
	}, nil
}

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error) {
	fake.listProjectBucketInventoryCalls++
	fake.listProjectBucketInventoryPathPrefix = pathPrefix
	if fake.listProjectBucketInventoryStarted != nil {
		fake.listProjectBucketInventoryStartOnce.Do(func() { close(fake.listProjectBucketInventoryStarted) })
	}
	if fake.listProjectBucketInventoryRelease != nil {
		<-fake.listProjectBucketInventoryRelease
	}
	if fake.listProjectBucketInventoryDelay > 0 {
		time.Sleep(fake.listProjectBucketInventoryDelay)
	}
	if fake.listProjectBucketObjectsErr != nil {
		return gintegrationsyfon.ProjectBucketInventory{}, fake.listProjectBucketObjectsErr
	}
	return gintegrationsyfon.ProjectBucketInventory{
		Objects:    append([]gintegrationsyfon.ProjectBucketObject(nil), fake.bucketObjects...),
		Complete:   !fake.bucketInventoryIncomplete,
		Warning:    fake.bucketInventoryWarning,
		ObservedAt: fake.bucketInventoryObservedAt,
	}, nil
}
