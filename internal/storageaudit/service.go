package storageaudit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/browse"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	"github.com/calypr/gecko/internal/storageaudit/cleanup"
	"github.com/calypr/gecko/internal/storageaudit/registration"
	"github.com/calypr/gecko/internal/storageaudit/sharedwork"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

const cleanupInactiveDays = 30
const projectFileUsageBulkChunkSize = 5000

const StorageChainAuditWorkBudget = 15 * time.Minute

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
	storage         storageAnalyticsBackend
	join            *projectJoinCache
	registrationSvc *registration.Service
	browseSvc       *browse.Service
	chainSvc        *chain.Service
	cleanupSvc      *cleanup.Service
	observations    storecache.StorageObservationCache
	auditWork       *sharedwork.Group[chain.StorageChainAuditRefreshResult]
}

type StorageAuditCaches struct {
	Response        storecache.ResponseCache
	ExactJoin       storecache.ExactJoinCache
	BucketInventory storecache.BucketInventoryCache
	Observations    storecache.StorageObservationCache
	Coordinator     storecache.ProjectAuditCoordinator
}

// NewStorageAnalyticsService creates the domain service without cache wiring.
// Production HTTP construction must call EnableStorageChainAuditResponseCacheFromEnv before serving.
func NewStorageAnalyticsService(storage storageAnalyticsBackend) *StorageAnalyticsService {
	if storage == nil {
		return nil
	}
	service := &StorageAnalyticsService{storage: storage, auditWork: sharedwork.New[chain.StorageChainAuditRefreshResult](StorageChainAuditWorkBudget)}
	service.registrationSvc = newRegistrationService(service)
	service.browseSvc = newBrowseService(service)
	service.chainSvc = newChainService(service)
	service.join = newProjectJoinCache(service.chainSvc, service.storage)
	service.cleanupSvc = newCleanupService(service)
	return service
}

// NewStorageAnalyticsServiceWithCaches injects all cache dependencies, primarily for tests.
func NewStorageAnalyticsServiceWithCaches(storage storageAnalyticsBackend, caches StorageAuditCaches) (*StorageAnalyticsService, error) {
	if caches.Response == nil || caches.ExactJoin == nil || caches.BucketInventory == nil || caches.Observations == nil || caches.Coordinator == nil {
		return nil, fmt.Errorf("storage audit response, exact-join, bucket-inventory, observation caches, and project coordinator are required")
	}
	service := NewStorageAnalyticsService(storage)
	if service == nil {
		return nil, fmt.Errorf("storage analytics service requires a storage backend")
	}
	service.setStorageAuditCaches(caches)
	return service, nil
}

func (service *StorageAnalyticsService) EnableStorageChainAuditResponseCacheFromEnv() error {
	if service == nil {
		return fmt.Errorf("initialize storage audit caches: storage analytics service is nil")
	}
	responseCache, err := storecache.NewStorageChainAuditResponseCacheFromEnv()
	if err != nil {
		return err
	}
	exactJoinCache, err := storecache.NewStorageExactProjectJoinCacheFromEnv()
	if err != nil {
		closeCache(responseCache)
		return err
	}
	bucketInventoryCache, err := storecache.NewProjectBucketInventoryCacheFromEnv()
	if err != nil {
		closeCache(responseCache)
		closeCache(exactJoinCache)
		return err
	}
	observationCache, err := storecache.NewStorageObservationCacheFromEnv()
	if err != nil {
		closeCache(responseCache)
		closeCache(exactJoinCache)
		closeCache(bucketInventoryCache)
		return err
	}
	coordinator, err := storecache.NewRedisProjectAuditCoordinatorFromEnv()
	if err != nil {
		closeCache(responseCache)
		closeCache(exactJoinCache)
		closeCache(bucketInventoryCache)
		closeCache(observationCache)
		return err
	}
	service.setStorageAuditCaches(StorageAuditCaches{
		Response:        responseCache,
		ExactJoin:       exactJoinCache,
		BucketInventory: bucketInventoryCache,
		Observations:    observationCache,
		Coordinator:     coordinator,
	})
	return nil
}

func (service *StorageAnalyticsService) setStorageAuditCaches(caches StorageAuditCaches) {
	service.chainSvc.ChainAuditResponseCache = caches.Response
	service.chainSvc.ProjectBucketCache = caches.BucketInventory
	service.join.exact = caches.ExactJoin
	service.observations = caches.Observations
	service.chainSvc.ProjectAuditCoordinator = caches.Coordinator
}

func (service *StorageAnalyticsService) Shutdown(ctx context.Context) error {
	if service == nil {
		return nil
	}
	var shutdownErr error
	if service.chainSvc != nil {
		shutdownErr = errors.Join(shutdownErr, service.chainSvc.Shutdown(ctx))
	}
	if service.join != nil && service.join.work != nil {
		shutdownErr = errors.Join(shutdownErr, service.join.work.Shutdown(ctx))
	}
	return shutdownErr
}

func closeCache(cache any) {
	if closer, ok := cache.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}
