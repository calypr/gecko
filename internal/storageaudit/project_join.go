package storageaudit

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *StorageAnalyticsService) loadJoinState(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, forceRefresh bool) (*repoindex.Index, []RepoInventoryFile, map[string][]sarecord.State, map[string]gintegrationsyfon.FileUsage, error) {
	index, err := repoindex.LoadOrBuildRepoAnalyticsIndex(ctx, mirrorPath, ref, repo, hash)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	inventory, err := repoindex.FilterRepoInventoryFiles(index, gitSubpath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	recordsByChecksum, usageByObjectID, err := service.join.load(ctx, authorizationHeader, organization, project, hash, index.Sidecar.Files, forceRefresh)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return index, inventory, recordsByChecksum, usageByObjectID, nil
}

func (service *StorageAnalyticsService) listProjectRecordStates(ctx context.Context, authorizationHeader string, organization string, project string, usageByObjectID map[string]gintegrationsyfon.FileUsage) (map[string][]sarecord.State, error) {
	projectRecords, err := service.storage.ListProjectRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project records: %w", err)
	}
	out := make(map[string][]sarecord.State)
	for _, record := range projectRecords {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		out[normalizedChecksum] = append(out[normalizedChecksum], sarecord.State{
			ProjectRecord: record,
			Usage:         usageByObjectID[record.ObjectID],
		})
	}
	return out, nil
}

func (service *StorageAnalyticsService) loadProjectStorageScopes(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error) {
	buckets, err := service.storage.ListBuckets(ctx, authorizationHeader)
	if err != nil {
		return nil, fmt.Errorf("list syfon buckets: %w", err)
	}
	bucketNames := make([]string, 0, len(buckets))
	for bucket := range buckets {
		bucketNames = append(bucketNames, bucket)
	}
	sort.Strings(bucketNames)
	scopes := make([]domain.StorageBucketScope, 0)
	for _, bucket := range bucketNames {
		items, err := service.storage.ListBucketScopes(ctx, authorizationHeader, bucket)
		if err != nil {
			return nil, fmt.Errorf("list syfon bucket scopes for %s: %w", bucket, err)
		}
		for _, scope := range items {
			if !strings.EqualFold(strings.TrimSpace(scope.Organization), organization) {
				continue
			}
			scopeProject := strings.TrimSpace(scope.ProjectID)
			if scopeProject != "" && !strings.EqualFold(scopeProject, project) {
				continue
			}
			scopes = append(scopes, scope)
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

func (service *StorageAnalyticsService) attachStorageProbes(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, false, nil)
}

func (service *StorageAnalyticsService) attachStorageListValidations(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, true, nil)
}

func (service *StorageAnalyticsService) attachStorageValidationResults(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, useListValidation bool, auditOptions *chain.AuditEvidenceOptions) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	started := time.Now()
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemKeys := map[string]string{}
	recordProbeKeysByObjectID := map[string][]string{}
	accessURLCount := 0

	for _, group := range allProjectRecords {
		for _, record := range group {
			for _, accessURL := range sarecord.ProbeAccessURLsForRecord(record) {
				accessURLCount++
				normalizedURL := strings.TrimSpace(accessURL)
				if normalizedURL == "" {
					continue
				}
				key := sarecord.StorageProbeRequestKey(normalizedURL, record.Size, record.Checksum)
				expectedName := ""
				if useListValidation {
					expectedName = sarecord.ExpectedStorageObjectNameForListValidation(normalizedURL, record.Name)
					key = sarecord.StorageListValidationRequestKey(normalizedURL, record.Size, expectedName)
				}
				recordProbeKeysByObjectID[record.ObjectID] = append(recordProbeKeysByObjectID[record.ObjectID], key)
				if _, ok := itemKeys[key]; ok {
					continue
				}
				itemKeys[key] = key
				expectedSize := record.Size
				item := gintegrationsyfon.BulkStorageProbeItem{
					ID:                key,
					ObjectURL:         normalizedURL,
					ExpectedSizeBytes: &expectedSize,
					ExpectedSHA256:    strings.TrimSpace(record.Checksum),
				}
				if useListValidation {
					item.ExpectedSHA256 = ""
					item.ExpectedName = expectedName
				}
				items = append(items, item)
				if useListValidation && len(items) <= 20 {
					log.Printf(
						"INFO: storage_chain_bulk_list_item object_id=%s name=%q checksum=%s submitted_url=%s raw_access_urls=%q canonical_access_urls=%q expected_size=%d expected_name=%q",
						strings.TrimSpace(record.ObjectID),
						strings.TrimSpace(record.Name),
						strings.TrimSpace(record.Checksum),
						normalizedURL,
						strings.Join(sarecord.RawAccessURLsForRecord(record), ","),
						strings.Join(sarecord.AccessURLsForStorage(record), ","),
						record.Size,
						item.ExpectedName,
					)
				}
			}
		}
	}
	mode := "head"
	operation := StorageChainValidationModeMetadata
	if useListValidation {
		mode = "list"
		operation = StorageChainValidationModeList
	}
	log.Printf(
		"INFO: storage_chain_validation_request_built mode=%s record_count=%d access_url_count=%d unique_request_count=%d duplicate_request_count=%d build_ms=%d",
		mode,
		sarecord.CountStates(allProjectRecords),
		accessURLCount,
		len(items),
		accessURLCount-len(items),
		time.Since(started).Milliseconds(),
	)

	resultState := &storageObservationResultMap{results: map[string]gintegrationsyfon.BulkStorageProbeResult{}}
	missItems := items
	keysByID := map[string]storageObservationKeys{}
	version := storecache.StorageObservationVersion{}
	cacheEnabled := auditOptions != nil && service.observations != nil
	progressBackend, hasProgressBackend := service.storage.(storageValidationProgressBackend)
	if auditOptions != nil && hasProgressBackend && service.observations == nil {
		return nil, nil, fmt.Errorf("storage observation cache is required for audit progress")
	}
	if cacheEnabled {
		var cachedResults map[string]gintegrationsyfon.BulkStorageProbeResult
		var err error
		missItems, cachedResults, keysByID, version, err = service.prepareStorageObservationInputs(ctx, items, operation, *auditOptions)
		if err != nil {
			log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
			return nil, nil, err
		}
		resultState.add(cachedResults)
	}
	if len(items) > 0 {
		checkpoint := func(batchItems []gintegrationsyfon.BulkStorageProbeItem, batchResults []gintegrationsyfon.BulkStorageProbeResult) error {
			processed, err := service.checkpointStorageObservationBatch(ctx, batchItems, batchResults, operation, *auditOptions, keysByID, version)
			resultState.add(processed)
			return err
		}
		if len(missItems) == 0 {
			log.Printf("INFO: storage_chain_validation_cache_hit mode=%s request_count=%d", mode, len(items))
		} else {
			var (
				results []gintegrationsyfon.BulkStorageProbeResult
				err     error
			)
			if cacheEnabled && hasProgressBackend {
				if useListValidation {
					results, err = progressBackend.BulkListStorageObjectsWithProgress(ctx, authorizationHeader, missItems, checkpoint)
				} else {
					results, err = progressBackend.BulkProbeStorageObjectsWithProgress(ctx, authorizationHeader, missItems, checkpoint)
				}
			} else if useListValidation {
				results, err = service.storage.BulkListStorageObjects(ctx, authorizationHeader, missItems)
			} else {
				results, err = service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, missItems)
			}
			if err != nil {
				if useListValidation {
					log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
					return nil, nil, fmt.Errorf("list-validate syfon storage objects: %w", err)
				}
				log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d duration_ms=%d error=%q", mode, len(items), time.Since(started).Milliseconds(), err.Error())
				return nil, nil, fmt.Errorf("probe syfon storage objects: %w", err)
			}
			if cacheEnabled && !hasProgressBackend {
				if err := checkpoint(missItems, results); err != nil {
					return nil, nil, err
				}
			} else if !cacheEnabled {
				resultMap := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(results))
				for _, result := range results {
					result.Operation = operation
					resultMap[strings.TrimSpace(result.ID)] = result
				}
				resultState.add(resultMap)
			}
		}
	}
	resultsByKey := resultState.snapshot()
	log.Printf("INFO: storage_chain_validation_request_done mode=%s request_count=%d result_count=%d duration_ms=%d", mode, len(items), len(resultsByKey), time.Since(started).Milliseconds())

	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				keys := sarecord.UniqueStrings(recordProbeKeysByObjectID[record.ObjectID])
				probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0, len(keys))
				for _, key := range keys {
					if result, ok := resultsByKey[key]; ok {
						result.Operation = operation
						probes = append(probes, result)
					}
				}
				clone.AccessProbes = append(append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...), probes...)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}

	return attach(recordsByChecksum), attach(allProjectRecords), nil
}

func cloneRecordStateMap(input map[string][]sarecord.State) map[string][]sarecord.State {
	out := make(map[string][]sarecord.State, len(input))
	for checksum, group := range input {
		out[checksum] = append([]sarecord.State(nil), group...)
	}
	return out
}
