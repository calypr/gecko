package storageaudit

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func (fake *fakeStorageAnalyticsBackend) ListProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*gintegrationsyfon.ProjectBucketSummary, error) {
	fake.listProjectBucketSummaryCalls++
	fake.listProjectBucketSummaryMode = mode
	var totalBytes int64
	for _, item := range fake.BucketObjects {
		totalBytes += item.SizeBytes
	}
	return &gintegrationsyfon.ProjectBucketSummary{
		Provider:    "s3",
		Bucket:      "bucket",
		Prefix:      organization + "/" + project,
		ObjectURL:   "s3://bucket/" + organization + "/" + project,
		Exists:      len(fake.BucketObjects) > 0,
		ObjectCount: len(fake.BucketObjects),
		TotalBytes:  totalBytes,
		Mode:        mode,
	}, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.probeCalls++
	fake.probeItems = append(fake.probeItems, items...)
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(items))
	for _, item := range items {
		if result, ok := fake.probeResultsByURL[item.ObjectURL]; ok {
			result.ID = item.ID
			result.ObjectURL = item.ObjectURL
			out = append(out, result)
			continue
		}
		if result, ok := fake.probeResults[item.ID]; ok {
			out = append(out, result)
			continue
		}
		exists := true
		out = append(out, gintegrationsyfon.BulkStorageProbeResult{
			ID:               item.ID,
			ObjectURL:        item.ObjectURL,
			Exists:           exists,
			Status:           "present",
			ValidationStatus: "matched",
			SizeBytes:        item.ExpectedSizeBytes,
			MetaSHA256:       item.ExpectedSHA256,
		})
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return fake.BulkProbeStorageObjects(ctx, authorizationHeader, items)
}

func markBrokenAccessURLs(fake *fakeStorageAnalyticsBackend, urls ...string) {
	if fake.probeResultsByURL == nil {
		fake.probeResultsByURL = make(map[string]gintegrationsyfon.BulkStorageProbeResult)
	}
	for _, url := range urls {
		fake.probeResultsByURL[url] = gintegrationsyfon.BulkStorageProbeResult{
			Status: "not_found", ErrorKind: "object_not_found", VerificationMethod: "ranged_get",
		}
	}
}

func (fake *fakeStorageAnalyticsBackend) BulkListStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.listProbeCalls++
	fake.listProbeItems = append(fake.listProbeItems, items...)
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(items))
	for _, item := range items {
		if result, ok := fake.listProbeResults[item.ID]; ok {
			out = append(out, result)
			continue
		}
		exists := true
		out = append(out, gintegrationsyfon.BulkStorageProbeResult{
			ID:               item.ID,
			ObjectURL:        item.ObjectURL,
			Exists:           exists,
			Status:           "present",
			ValidationStatus: "matched",
			SizeBytes:        item.ExpectedSizeBytes,
			SizeMatch:        ptrBool(true),
			NameMatch:        ptrBool(true),
		})
	}
	return out, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	fake.deletedIDs = append(fake.deletedIDs, objectIDs...)
	if deleteStorageData {
		fake.deletedStorageIDs = append(fake.deletedStorageIDs, objectIDs...)
	}
	return nil
}

func (fake *fakeStorageAnalyticsBackend) DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error) {
	fake.deletedBucketObjects = append(fake.deletedBucketObjects, objectURLs...)
	results := make([]gintegrationsyfon.ProjectBucketDeleteResult, 0, len(objectURLs))
	for _, objectURL := range objectURLs {
		results = append(results, gintegrationsyfon.ProjectBucketDeleteResult{
			ObjectURL: objectURL,
			Status:    "deleted",
		})
	}
	return results, nil
}

func (fake *fakeStorageAnalyticsBackend) BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error {
	if fake.updatedAccessMethods == nil {
		fake.updatedAccessMethods = map[string][]gintegrationsyfon.ProjectAccessMethod{}
	}
	for objectID, methods := range updates {
		fake.updatedAccessMethods[objectID] = append([]gintegrationsyfon.ProjectAccessMethod(nil), methods...)
	}
	return nil
}

func (fake *fakeStorageAnalyticsBackend) RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error) {
	fake.registeredObjects = append(fake.registeredObjects, candidates...)
	results := make([]gintegrationsyfon.ProjectObjectRegistrationResult, len(candidates))
	for index, candidate := range candidates {
		results[index] = gintegrationsyfon.ProjectObjectRegistrationResult{ObjectID: "created-" + candidate.Checksum[:8]}
		fake.projectRecords = append(fake.projectRecords, gintegrationsyfon.ProjectRecord{
			ObjectID:     results[index].ObjectID,
			Name:         candidate.Name,
			Checksum:     candidate.Checksum,
			Size:         candidate.Size,
			Organization: "HTAN_INT",
			Project:      "BForePC",
			AccessURLs:   candidate.AccessURLs,
		})
	}
	return results, nil
}

type fakeStorageAnalyticsBackend struct {
	mu                                   sync.Mutex
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
	BucketObjects                        []gintegrationsyfon.ProjectBucketObject
	bucketInventoryIncomplete            bool
	BucketInventoryWarning               string
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
	fake.mu.Lock()
	defer fake.mu.Unlock()
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
	fake.mu.Lock()
	defer fake.mu.Unlock()
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
		Objects:    append([]gintegrationsyfon.ProjectBucketObject(nil), fake.BucketObjects...),
		Complete:   !fake.bucketInventoryIncomplete,
		Warning:    fake.BucketInventoryWarning,
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
		Objects:    append([]gintegrationsyfon.ProjectBucketObject(nil), fake.BucketObjects...),
		Complete:   !fake.bucketInventoryIncomplete,
		Warning:    fake.BucketInventoryWarning,
		ObservedAt: fake.bucketInventoryObservedAt,
	}, nil
}
