package chain

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *Service) projectChainInputCacheKey(organization string, project string) string {
	return strings.TrimSpace(organization) + "/" + strings.TrimSpace(project)
}

func (service *Service) updateChainInputCache(cacheKey string, update func(*ChainInputCacheEntry)) {
	service.chainInputMu.Lock()
	defer service.chainInputMu.Unlock()
	state := service.ChainInputCache[cacheKey]
	if time.Now().After(state.ExpiresAt) {
		state = ChainInputCacheEntry{}
	}
	state.ExpiresAt = time.Now().Add(chainInputCacheTTL)
	update(&state)
	service.ChainInputCache[cacheKey] = state
}

func (service *Service) evictProjectChainInputCache(organization string, project string) {
	baseKey := service.projectChainInputCacheKey(organization, project)
	prefix := baseKey + "::"
	service.chainInputMu.Lock()
	defer service.chainInputMu.Unlock()
	for key := range service.ChainInputCache {
		if key == baseKey || strings.HasPrefix(key, prefix) {
			delete(service.ChainInputCache, key)
		}
	}
}

func (service *Service) loadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]model.RepoInventoryFile, error) {
	index, err := service.deps.Join.LoadRepoIndex(ctx, ref, mirrorPath, repo, hash)
	if err != nil {
		return nil, err
	}
	return repoindex.FilterRepoInventoryFiles(index, gitSubpath)
}

type storageChainInputs struct {
	inventory          []model.RepoInventoryFile
	recordSet          *sarecord.RecordSet
	scopes             []domain.StorageBucketScope
	BucketSummary      *gintegrationsyfon.ProjectBucketSummary
	BucketObjects      []gintegrationsyfon.ProjectBucketObject
	BucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
	bucketInventoryErr error
	BucketMetadata     sarecord.BucketInventoryMetadata
}

func (service *Service) loadStorageChainInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, bucketMode string, validationMode string, bucketPathPrefix string, forceBucketRefresh bool, timings *model.StorageChainAuditTimings) (*storageChainInputs, error) {
	type inventoryResult struct {
		inventory []model.RepoInventoryFile
		err       error
	}
	type recordResult struct {
		recordSet *sarecord.RecordSet
		err       error
	}
	type scopeResult struct {
		scopes []domain.StorageBucketScope
		err    error
	}
	type bucketResult struct {
		BucketSummary      *gintegrationsyfon.ProjectBucketSummary
		BucketObjects      []gintegrationsyfon.ProjectBucketObject
		BucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
		err                error
		metadata           sarecord.BucketInventoryMetadata
	}

	inventoryCh := make(chan inventoryResult, 1)
	recordCh := make(chan recordResult, 1)
	scopeCh := make(chan scopeResult, 1)
	bucketCh := make(chan bucketResult, 1)

	go func() {
		start := time.Now()
		timings.StageStart("repo_index")
		inventory, err := service.loadStorageChainInventory(ctx, ref, gitSubpath, mirrorPath, repo, hash)
		timings.Record("repo_index", time.Since(start))
		timings.RecordMemory("repo_index", "git_files", len(inventory))
		logStorageChainInputResult("repo_index", len(inventory), err)
		inventoryCh <- inventoryResult{inventory: inventory, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_records")
		recordSet, err := service.loadCachedProjectAuditRecordSet(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_records", time.Since(start))
		recordCount := 0
		if recordSet != nil {
			recordCount = sarecord.CountStates(recordSet.AllProjectRecords)
		}
		timings.RecordMemory("syfon_project_records", "syfon_records", recordCount)
		logStorageChainInputResult("syfon_project_records", recordCount, err)
		recordCh <- recordResult{recordSet: recordSet, err: err}
	}()
	go func() {
		start := time.Now()
		timings.StageStart("syfon_project_scopes")
		scopes, err := service.loadCachedProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
		timings.Record("syfon_project_scopes", time.Since(start))
		logStorageChainInputResult("syfon_project_scopes", len(scopes), err)
		scopeCh <- scopeResult{scopes: scopes, err: err}
	}()
	go func() {
		if bucketMode == model.StorageChainBucketModeValidate && validationMode != model.StorageChainValidationModeList {
			timings.Record("syfon_bucket_inventory_skipped", 0)
			bucketCh <- bucketResult{
				BucketObjects:      []gintegrationsyfon.ProjectBucketObject{},
				BucketObjectsByURL: map[string]gintegrationsyfon.ProjectBucketObject{},
				metadata:           sarecord.BucketInventoryMetadata{Complete: false, Source: "not_requested"},
			}
			return
		}
		start := time.Now()
		timings.StageStart("syfon_bucket_inventory")
		var bucketObjects []gintegrationsyfon.ProjectBucketObject
		var bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject
		var err error
		var metadata sarecord.BucketInventoryMetadata
		if bucketMode == model.StorageChainBucketModeValidate {
			// Full bucket LIST calls are expensive on some providers. Reuse the
			// project inventory during the cooldown unless the caller explicitly
			// requests a new bucket traversal.
			bucketObjects, bucketObjectsByURL, metadata, err = service.loadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, "", forceBucketRefresh)
		} else {
			bucketObjects, bucketObjectsByURL, metadata, err = service.loadCachedProjectBucketInventory(ctx, authorizationHeader, organization, project, bucketPathPrefix, forceBucketRefresh)
		}
		timings.Record("syfon_bucket_inventory", time.Since(start))
		timings.RecordMemory("syfon_bucket_inventory", "bucket_objects", len(bucketObjects), "bucket_lookup", len(bucketObjectsByURL))
		logStorageChainInputResult("syfon_bucket_items", len(bucketObjects), err)
		bucketCh <- bucketResult{BucketObjects: bucketObjects, BucketObjectsByURL: bucketObjectsByURL, err: err, metadata: metadata}
	}()

	inventory := <-inventoryCh
	recordSet := <-recordCh
	scopes := <-scopeCh
	bucketResultValue := <-bucketCh
	if inventory.err != nil {
		return nil, inventory.err
	}
	if recordSet.err != nil {
		return nil, recordSet.err
	}
	if scopes.err != nil {
		return nil, scopes.err
	}
	return &storageChainInputs{
		inventory:          inventory.inventory,
		recordSet:          recordSet.recordSet,
		scopes:             scopes.scopes,
		BucketSummary:      bucketResultValue.BucketSummary,
		BucketObjects:      bucketResultValue.BucketObjects,
		BucketObjectsByURL: bucketResultValue.BucketObjectsByURL,
		bucketInventoryErr: bucketResultValue.err,
		BucketMetadata:     bucketResultValue.metadata,
	}, nil
}

func logStorageChainInputResult(stage string, count int, err error) {
	if err != nil {
		log.Printf("INFO: storage_chain_input_done stage=%s count=%d error=%q", strings.TrimSpace(stage), count, err.Error())
		return
	}
	log.Printf("INFO: storage_chain_input_done stage=%s count=%d", strings.TrimSpace(stage), count)
}

type storageAuditBaseInputs struct {
	index             *repoindex.Index
	inventory         []model.RepoInventoryFile
	RecordsByChecksum map[string][]sarecord.State
	usageByObjectID   map[string]gintegrationsyfon.FileUsage
}

func (service *Service) loadStorageAuditBaseInputs(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*storageAuditBaseInputs, error) {
	state, err := service.deps.Join.LoadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
	if err != nil {
		return nil, err
	}
	return &storageAuditBaseInputs{
		index:             state.Index,
		inventory:         state.Inventory,
		RecordsByChecksum: state.RecordsByChecksum,
		usageByObjectID:   state.UsageByObjectID,
	}, nil
}

func (service *Service) loadScopedProjectRecords(ctx context.Context, authorizationHeader string, organization string, project string, base *storageAuditBaseInputs) (*sarecord.RecordSet, error) {
	allProjectRecords, err := service.deps.Records.ListProjectRecordStates(ctx, authorizationHeader, organization, project, base.usageByObjectID)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: base.RecordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func (service *Service) loadProjectScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	return service.deps.Scopes.LoadProjectStorageScopes(ctx, authorizationHeader, organization, project)
}

func (service *Service) loadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	scopes, err := service.deps.Scopes.ListProjectScopes(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project scopes: %w", err)
	}
	if len(scopes) == 0 {
		scopes, err = service.deps.Scopes.LoadProjectStorageScopes(ctx, authorizationHeader, organization, project)
		if err != nil {
			return nil, err
		}
	}
	sort.SliceStable(scopes, func(i, j int) bool {
		iProject := strings.TrimSpace(scopes[i].ProjectID)
		jProject := strings.TrimSpace(scopes[j].ProjectID)
		if iProject == "" && jProject != "" {
			return true
		}
		if iProject != "" && jProject == "" {
			return false
		}
		if scopes[i].Bucket != scopes[j].Bucket {
			return scopes[i].Bucket < scopes[j].Bucket
		}
		return scopes[i].Path < scopes[j].Path
	})
	return scopes, nil
}
