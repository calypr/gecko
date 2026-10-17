package storageaudit

import (
	"context"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/browse"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	"github.com/calypr/gecko/internal/storageaudit/cleanup"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/registration"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

const cleanupInactiveDays = 30
const projectJoinCacheTTL = 45 * time.Second
const projectFileUsageBulkChunkSize = 5000

type storageAnalyticsBackend interface {
	ListBuckets(ctx context.Context, authorizationHeader string) (map[string]domain.StorageBucket, error)
	ListBucketScopes(ctx context.Context, authorizationHeader string, bucket string) ([]domain.StorageBucketScope, error)
	ListProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) ([]gintegrationsyfon.ProjectRecord, error)
	GetProjectMetricsSummary(ctx context.Context, authorizationHeader string, organization string, project string) (*gintegrationsyfon.ProjectMetricsSummary, error)
	ListProjectScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
	ListProjectFileUsageByObjectIDs(ctx context.Context, authorizationHeader string, organization string, project string, objectIDs []string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectFileUsage(ctx context.Context, authorizationHeader string, organization string, project string, inactiveDays int) (map[string]gintegrationsyfon.FileUsage, error)
	ListProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
	ListProjectBucketInventory(ctx context.Context, authorizationHeader string, organization string, project string, pathPrefix string) (gintegrationsyfon.ProjectBucketInventory, error)
	ListProjectBucketSummary(ctx context.Context, authorizationHeader string, organization string, project string, mode string) (*gintegrationsyfon.ProjectBucketSummary, error)
	BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkListStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error
	DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error)
	BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error
	RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error)
}

type StorageAnalyticsService struct {
	storage               storageAnalyticsBackend
	projectJoinMu         sync.RWMutex
	projectJoinCache      map[string]cachedProjectJoinState
	projectJoinWork       map[string]*inflightProjectJoinState
	exactProjectJoinCache storecache.ExactJoinCache
	registrationSvc       *registration.Service
	browseSvc             *browse.Service
	chainSvc              *chain.Service
	cleanupSvc            *cleanup.Service
}

func NewStorageAnalyticsService(storage storageAnalyticsBackend) *StorageAnalyticsService {
	if storage == nil {
		return nil
	}
	service := &StorageAnalyticsService{
		storage:          storage,
		projectJoinCache: map[string]cachedProjectJoinState{},
		projectJoinWork:  map[string]*inflightProjectJoinState{},
	}
	service.registrationSvc = newRegistrationService(service)
	service.browseSvc = newBrowseService(service)
	service.chainSvc = newChainService(service)
	service.cleanupSvc = newCleanupService(service)
	return service
}

func (service *StorageAnalyticsService) EnableStorageChainAuditResponseCacheFromEnv() {
	if service == nil {
		return
	}
	service.chainSvc.EnableResponseCachesFromEnv()
	service.exactProjectJoinCache = storecache.NewStorageExactProjectJoinCacheFromEnv()
}

type cachedProjectJoinState struct {
	expiresAt         time.Time
	RecordsByChecksum map[string][]record.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

type inflightProjectJoinState struct {
	done              chan struct{}
	RecordsByChecksum map[string][]record.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
	err               error
}
