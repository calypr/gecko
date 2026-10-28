package chain

import (
	"context"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func (service *Service) buildStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *sarecord.RecordSet, inventory []model.RepoInventoryFile, scopes []domain.StorageBucketScope, bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, bucketInventoryErr error, bucketMetadata sarecord.BucketInventoryMetadata, bucketMode string, validationMode string, bucketPathPrefix string, forceStorageRevalidation bool, timings *model.StorageChainAuditTimings) (*sarecord.StorageView, error) {
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	evidenceOptions := AuditEvidenceOptions{
		Organization:             organization,
		Project:                  project,
		Scopes:                   append([]domain.StorageBucketScope(nil), scopes...),
		BucketObjects:            append([]gintegrationsyfon.ProjectBucketObject(nil), bucketObjects...),
		BucketObjectsByURL:       bucketObjectsByURL,
		BucketInventoryComplete:  bucketMetadata.Complete,
		BucketInventoryStale:     bucketMetadata.Stale,
		BucketPathPrefix:         bucketPathPrefix,
		ForceStorageRevalidation: forceStorageRevalidation,
	}
	view := &sarecord.StorageView{
		Scopes:                   append([]domain.StorageBucketScope(nil), scopes...),
		RecordsByChecksum:        recordSet.RecordsByChecksum,
		AllProjectRecords:        recordSet.AllProjectRecords,
		BucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		BucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		BucketInventoryAvailable: true,
		BucketInventoryComplete:  bucketMetadata.Complete,
		BucketInventoryWarning:   bucketMetadata.Warning,
		BucketInventoryObserved:  bucketMetadata.Observed,
		BucketInventorySource:    bucketMetadata.Source,
		BucketInventoryStale:     bucketMetadata.Stale,
	}
	if bucketMode == model.StorageChainBucketModeValidate {
		if bucketInventoryErr != nil {
			view.BucketInventoryAvailable = false
			view.BucketInventoryError = strings.TrimSpace(bucketInventoryErr.Error())
		}
		if validationMode == model.StorageChainValidationModeInventory {
			return view, nil
		}
		if validationMode == model.StorageChainValidationModeList {
			if bucketInventoryErr != nil {
				// A failed LIST is not an empty bucket. Keep the Syfon records and
				// surface the inventory error without turning every record into a
				// potentially expensive HEAD request.
				return view, nil
			}
			view.BucketObjects = bucketObjects
			view.BucketObjectsByURL = bucketObjectsByURL
			validateStart := time.Now()
			probedRecordSet, err := attachProjectStorageInventoryValidations(recordSet, inventory, view.BucketObjectsByURL, scopes, organization, project)
			if err != nil {
				return nil, err
			}
			timings.Record("inventory_list_validation", time.Since(validateStart))

			candidateStart := time.Now()
			candidates := selectInventoryMissRecordSet(probedRecordSet)
			if bucketMetadata.Stale || !bucketMetadata.Complete {
				candidates = nil
			}
			timings.Record("exact_list_candidate_selection", time.Since(candidateStart))
			if candidates != nil {
				probeStart := time.Now()
				exactRecordSet, probeErr := service.attachProjectStorageProbesForAudit(ctx, authorizationHeader, candidates, evidenceOptions)
				timings.Record("targeted_head_validation", time.Since(probeStart))
				if probeErr != nil {
					return nil, probeErr
				}
				probedRecordSet = mergeRecordSetProbes(probedRecordSet, exactRecordSet)
			}
			timings.RecordMemory("inventory_list_validation", "syfon_records", sarecord.CountStates(probedRecordSet.AllProjectRecords), "bucket_objects", len(view.BucketObjectsByURL), "exact_probe_records", countRecordSet(candidates))
			view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
			view.AllProjectRecords = probedRecordSet.AllProjectRecords
			return view, nil
		}
		probeStart := time.Now()
		var (
			probedRecordSet *sarecord.RecordSet
			probeErr        error
			stage           string
		)
		switch validationMode {
		case model.StorageChainValidationModeMetadata:
			probedRecordSet, probeErr = service.attachProjectStorageProbesForAudit(ctx, authorizationHeader, recordSet, evidenceOptions)
			stage = "bulk_metadata_validation"
		default:
			probedRecordSet, probeErr = service.attachProjectStorageListValidationsForAudit(ctx, authorizationHeader, recordSet, evidenceOptions)
			stage = "bulk_list_validation"
		}
		timings.Record(stage, time.Since(probeStart))
		if probedRecordSet != nil {
			timings.RecordMemory(stage, "syfon_records", sarecord.CountStates(probedRecordSet.AllProjectRecords))
		}
		if probeErr != nil {
			return nil, probeErr
		}
		view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
		view.AllProjectRecords = probedRecordSet.AllProjectRecords
		view.BucketObjects, view.BucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.AllProjectRecords)
		return view, nil
	}
	if bucketInventoryErr != nil {
		return nil, bucketInventoryErr
	}
	view.BucketObjects, view.BucketObjectsByURL = cloneBucketInventory(bucketObjects, bucketObjectsByURL)
	if validationMode == model.StorageChainValidationModeInventory || validationMode == model.StorageChainValidationModeList {
		return view, nil
	}

	probeSelectStart := time.Now()
	probeCandidates := selectTargetedProbeRecordSet(recordSet, view.BucketObjectsByURL)
	timings.Record("targeted_probe_selection", time.Since(probeSelectStart))
	if probeCandidates == nil {
		return view, nil
	}
	probeStart := time.Now()
	probedSubset, probeErr := service.attachProjectStorageProbesForAudit(ctx, authorizationHeader, probeCandidates, evidenceOptions)
	timings.Record("bulk_probe", time.Since(probeStart))
	if probedSubset != nil {
		timings.RecordMemory("bulk_probe", "probe_records", sarecord.CountStates(probedSubset.AllProjectRecords))
	}
	if probeErr != nil {
		return nil, probeErr
	}
	merged := mergeRecordSetProbes(recordSet, probedSubset)
	view.RecordsByChecksum = merged.RecordsByChecksum
	view.AllProjectRecords = merged.AllProjectRecords
	return view, nil
}

func (service *Service) loadStorageAuditStorageView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *sarecord.RecordSet, includeBucketInventory bool, includeProbes bool) (*sarecord.StorageView, error) {
	scopes, err := service.loadProjectScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	if includeProbes {
		recordSet, err = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
		if err != nil {
			return nil, err
		}
	}
	view := &sarecord.StorageView{
		Scopes:                   scopes,
		RecordsByChecksum:        recordSet.RecordsByChecksum,
		AllProjectRecords:        recordSet.AllProjectRecords,
		BucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		BucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		BucketInventoryAvailable: includeBucketInventory,
	}
	if includeBucketInventory {
		bucketObjects, bucketObjectsByURL, metadata, err := service.loadProjectBucketInventory(ctx, authorizationHeader, organization, project, "")
		if err != nil {
			if !includeProbes || !shouldDegradeBucketInventory(err) {
				return nil, err
			}
			view.BucketInventoryAvailable = false
			view.BucketInventoryError = strings.TrimSpace(err.Error())
			bucketObjects, bucketObjectsByURL = synthesizeBucketInventoryFromProbes(recordSet.AllProjectRecords)
		}
		view.BucketObjects = bucketObjects
		view.BucketObjectsByURL = bucketObjectsByURL
		view.BucketInventoryComplete = metadata.Complete
		view.BucketInventoryWarning = metadata.Warning
		view.BucketInventoryObserved = metadata.Observed
		view.BucketInventorySource = metadata.Source
	}
	return view, nil
}
