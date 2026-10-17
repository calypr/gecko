package cleanup

import (
	"context"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

type Store interface {
	ListProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error)
	BulkConfirmStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]gintegrationsyfon.ProjectAccessMethod) error
	BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error
	DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
	EvictProjectJoinCache(organization string, project string)
	EvictProjectAuditRecordCache(organization string, project string)
	EvictProjectChainInputCache(organization string, project string)
}

type Service struct {
	store Store
	chain *chain.Service
}

func New(store Store, chainService *chain.Service) *Service {
	return &Service{store: store, chain: chainService}
}

type ApplyPlan struct {
	DeleteObjectIDs        []string
	DeleteStorageObjectIDs []string
	DeleteBucketObjects    []string
	UpdateAccessMethods    map[string][]gintegrationsyfon.ProjectAccessMethod
	UpdatedRecordIDs       []string
	RepoDeletePaths        []string
	ManualPaths            []string
	SkippedPaths           []string
	Verifications          []Verification
	AccessMethodChanges    []model.AccessMethodChange
}

type FindingModel struct {
	Public              model.CleanupFinding
	DeleteObjectIDs     []string
	DeleteStorageData   bool
	DeleteBucketObjects []string
	UpdateAccessMethods map[string][]gintegrationsyfon.ProjectAccessMethod
	Manual              bool
}

type AuditModel struct {
	Findings             []FindingModel
	PublicFindings       []model.CleanupFinding
	Summary              model.CleanupAuditSummary
	ExpectedPathCount    int
	IncludesRepoManifest bool
	PathPrefix           string
}
