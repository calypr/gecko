package chain

import (
	"context"
	"fmt"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"path"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

func applyScopeCanonicalization(recordSet *sarecord.RecordSet, scopes []domain.StorageBucketScope, organization string, project string) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	recordsByChecksum, allProjectRecords := applyScopedStorageMappings(recordSet.RecordsByChecksum, recordSet.AllProjectRecords, scopes, organization, project)
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}
}

func (service *Service) attachProjectStorageProbes(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.store.AttachStorageProbes(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func (service *Service) attachProjectStorageListValidations(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.store.AttachStorageListValidations(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func attachProjectStorageInventoryValidations(recordSet *sarecord.RecordSet, inventory []model.RepoInventoryFile, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, scopes []domain.StorageBucketScope, organization string, project string) (*sarecord.RecordSet, error) {
	if recordSet == nil {
		return nil, nil
	}
	inventoryBuckets := projectInventoryBuckets(bucketObjectsByURL)
	repoPathsByChecksum := repoPathsByChecksumForInventory(inventory)
	attach := func(input map[string][]sarecord.State) (map[string][]sarecord.State, error) {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				probes, err := inventoryValidationProbesForRecord(record, repoPathsByChecksum[sarecord.NormalizeAnalyticsChecksum(record.Checksum)], bucketObjectsByURL, inventoryBuckets, scopes, organization, project)
				if err != nil {
					return nil, err
				}
				clone.AccessProbes = probes
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out, nil
	}
	recordsByChecksum, err := attach(recordSet.RecordsByChecksum)
	if err != nil {
		return nil, err
	}
	allProjectRecords, err := attach(recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func repoPathsByChecksumForInventory(inventory []model.RepoInventoryFile) map[string][]string {
	out := make(map[string][]string)
	for _, item := range inventory {
		checksum := sarecord.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		out[checksum] = append(out[checksum], item.RepoPath)
	}
	for checksum, paths := range out {
		out[checksum] = sarecord.UniqueStrings(paths)
	}
	return out
}

func inventoryValidationProbesForRecord(record sarecord.State, repoPaths []string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, inventoryBuckets map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	probes := make([]gintegrationsyfon.BulkStorageProbeResult, 0)
	seen := make(map[string]struct{})
	accessURLs, err := recordAccessURLsForProjectInventory(record.AccessURLs, inventoryBuckets, scopes, organization, project)
	if err != nil {
		return nil, err
	}
	accessURLs = append(accessURLs, sarecord.ProjectScopeRepoPathObjectURLs(repoPaths, scopes, organization, project)...)
	accessURLs = sarecord.UniqueStrings(accessURLs)
	for _, accessURL := range accessURLs {
		objectURL := sarecord.CanonicalStorageURL("", "", accessURL)
		if objectURL == "" {
			continue
		}
		bucket, _, _ := sarecord.ParseStorageURL(objectURL)
		if len(inventoryBuckets) > 0 {
			if _, ok := inventoryBuckets[bucket]; !ok {
				return nil, fmt.Errorf("storage access URL mapped to %q, but bucket %q is not present in project bucket inventory for project %s/%s", objectURL, bucket, strings.TrimSpace(organization), strings.TrimSpace(project))
			}
		}
		if _, ok := seen[objectURL]; ok {
			continue
		}
		seen[objectURL] = struct{}{}
		expectedName := sarecord.ExpectedStorageObjectNameForListValidation(objectURL, record.Name)
		if item, ok := bucketObjectsByURL[objectURL]; ok {
			probes = append(probes, inventoryPresentProbe(record, objectURL, expectedName, item))
			continue
		}
		probes = append(probes, inventoryMissingProbe(record, objectURL, expectedName))
	}
	return probes, nil
}

func recordAccessURLsForProjectInventory(accessURLs []string, inventoryBuckets map[string]struct{}, scopes []domain.StorageBucketScope, organization string, project string) ([]string, error) {
	out := make([]string, 0, len(accessURLs))
	for _, accessURL := range accessURLs {
		mapped := false
		if objectURL := sarecord.CanonicalizeScopedStorageURL(accessURL, scopes, organization, project); objectURL != "" {
			out = append(out, objectURL)
			mapped = true
		}
		objectURL := sarecord.CanonicalStorageURL("", "", accessURL)
		if objectURL == "" {
			continue
		}
		bucket, _, ok := sarecord.ParseStorageURL(objectURL)
		if !ok {
			continue
		}
		if len(inventoryBuckets) > 0 {
			if _, ok := inventoryBuckets[bucket]; ok {
				out = append(out, objectURL)
				mapped = true
			}
		}
		if !mapped {
			return nil, fmt.Errorf("storage access URL %q could not be mapped into bucket scopes for project %s/%s", strings.TrimSpace(accessURL), strings.TrimSpace(organization), strings.TrimSpace(project))
		}
	}
	return sarecord.UniqueStrings(out), nil
}

func projectInventoryBuckets(bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) map[string]struct{} {
	out := make(map[string]struct{})
	for objectURL, item := range bucketObjectsByURL {
		if bucket := strings.TrimSpace(item.Bucket); bucket != "" {
			out[bucket] = struct{}{}
			continue
		}
		bucket, _, ok := sarecord.ParseStorageURL(objectURL)
		if ok {
			out[bucket] = struct{}{}
		}
	}
	return out
}

func inventoryPresentProbe(record sarecord.State, objectURL string, expectedName string, item gintegrationsyfon.ProjectBucketObject) gintegrationsyfon.BulkStorageProbeResult {
	size := item.SizeBytes
	sizeMatch := sarecord.StorageSizesMatchForAudit(record.Size, item.SizeBytes)
	nameMatch := true
	mismatches := make([]string, 0, 1)
	if !sizeMatch {
		mismatches = append(mismatches, "size_mismatch")
	}
	validationStatus := "matched"
	if len(mismatches) > 0 {
		validationStatus = "mismatched"
	}
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   sarecord.StorageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            model.StorageChainValidationModeInventory,
		ObjectURL:            objectURL,
		Provider:             strings.TrimSpace(item.Provider),
		Bucket:               strings.TrimSpace(item.Bucket),
		Key:                  strings.Trim(strings.TrimSpace(item.Key), "/"),
		Path:                 strings.TrimSpace(item.Path),
		Exists:               true,
		Status:               "present",
		SizeBytes:            &size,
		MetaSHA256:           strings.TrimSpace(item.MetaSHA256),
		ETag:                 strings.TrimSpace(item.ETag),
		LastModified:         strings.TrimSpace(item.LastModified),
		ValidationStatus:     validationStatus,
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: mismatches,
	}
}

func inventoryMissingProbe(record sarecord.State, objectURL string, expectedName string) gintegrationsyfon.BulkStorageProbeResult {
	bucket, key, _ := sarecord.ParseStorageURL(objectURL)
	sizeMatch := false
	nameMatch := false
	return gintegrationsyfon.BulkStorageProbeResult{
		ID:                   sarecord.StorageListValidationRequestKey(objectURL, record.Size, expectedName),
		Operation:            model.StorageChainValidationModeInventory,
		ObjectURL:            objectURL,
		Provider:             "s3",
		Bucket:               bucket,
		Key:                  key,
		Path:                 path.Base(key),
		Exists:               false,
		Status:               "unknown",
		Error:                fmt.Sprintf("object %q was absent from project bucket inventory and requires exact verification", objectURL),
		ErrorKind:            "inventory_miss",
		ValidationStatus:     "unverifiable",
		SizeMatch:            &sizeMatch,
		NameMatch:            &nameMatch,
		ValidationMismatches: []string{},
	}
}

func (service *Service) buildStorageChainView(ctx context.Context, authorizationHeader string, organization string, project string, recordSet *sarecord.RecordSet, inventory []model.RepoInventoryFile, scopes []domain.StorageBucketScope, bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject, bucketInventoryErr error, BucketMetadata sarecord.BucketInventoryMetadata, bucketMode string, validationMode string, timings *model.StorageChainAuditTimings) (*sarecord.StorageView, error) {
	recordSet = applyScopeCanonicalization(recordSet, scopes, organization, project)
	view := &sarecord.StorageView{
		Scopes:                   append([]domain.StorageBucketScope(nil), scopes...),
		RecordsByChecksum:        recordSet.RecordsByChecksum,
		AllProjectRecords:        recordSet.AllProjectRecords,
		BucketObjects:            []gintegrationsyfon.ProjectBucketObject{},
		BucketObjectsByURL:       map[string]gintegrationsyfon.ProjectBucketObject{},
		BucketInventoryAvailable: true,
		BucketInventoryComplete:  BucketMetadata.Complete,
		BucketInventoryWarning:   BucketMetadata.Warning,
		BucketInventoryObserved:  BucketMetadata.Observed,
		BucketInventorySource:    BucketMetadata.Source,
		BucketInventoryStale:     BucketMetadata.Stale,
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
				probeStart := time.Now()
				probedRecordSet, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
				timings.Record("inventory_error_head_fallback", time.Since(probeStart))
				if probeErr != nil {
					return nil, probeErr
				}
				view.RecordsByChecksum = probedRecordSet.RecordsByChecksum
				view.AllProjectRecords = probedRecordSet.AllProjectRecords
				view.BucketObjects, view.BucketObjectsByURL = synthesizeBucketInventoryFromProbes(probedRecordSet.AllProjectRecords)
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
			if BucketMetadata.Stale {
				candidates = recordSet
			}
			timings.Record("exact_list_candidate_selection", time.Since(candidateStart))
			if candidates != nil {
				probeStart := time.Now()
				exactRecordSet, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, candidates)
				timings.Record("targeted_head_validation", time.Since(probeStart))
				if probeErr != nil {
					return nil, probeErr
				}
				probedRecordSet = mergeRecordSetProbes(probedRecordSet, exactRecordSet)
			}
			timings.RecordMemory("inventory_list_validation", "syfon_records", countRecordStates(probedRecordSet.AllProjectRecords), "bucket_objects", len(view.BucketObjectsByURL), "exact_probe_records", countRecordSet(candidates))
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
			probedRecordSet, probeErr = service.attachProjectStorageProbes(ctx, authorizationHeader, recordSet)
			stage = "bulk_metadata_validation"
		default:
			probedRecordSet, probeErr = service.attachProjectStorageListValidations(ctx, authorizationHeader, recordSet)
			stage = "bulk_list_validation"
		}
		timings.Record(stage, time.Since(probeStart))
		if probedRecordSet != nil {
			timings.RecordMemory(stage, "syfon_records", countRecordStates(probedRecordSet.AllProjectRecords))
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
	probedSubset, probeErr := service.attachProjectStorageProbes(ctx, authorizationHeader, probeCandidates)
	timings.Record("bulk_probe", time.Since(probeStart))
	if probedSubset != nil {
		timings.RecordMemory("bulk_probe", "probe_records", countRecordStates(probedSubset.AllProjectRecords))
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

func cloneRecordStateMap(input map[string][]sarecord.State) map[string][]sarecord.State {
	out := make(map[string][]sarecord.State, len(input))
	for checksum, group := range input {
		states := make([]sarecord.State, 0, len(group))
		for _, record := range group {
			clone := record
			clone.CanonicalAccessURLs = append([]string(nil), record.CanonicalAccessURLs...)
			clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
			states = append(states, clone)
		}
		out[checksum] = states
	}
	return out
}

func selectTargetedProbeRecordSet(recordSet *sarecord.RecordSet, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			if !recordNeedsTargetedProbe(record, bucketObjectsByURL) {
				continue
			}
			selected[checksum] = append(selected[checksum], record)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func selectInventoryMissRecordSet(recordSet *sarecord.RecordSet) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			accessURLs := make(map[string]struct{})
			for _, accessURL := range sarecord.AccessURLsForStorage(record) {
				if objectURL := sarecord.CanonicalStorageURL("", "", accessURL); objectURL != "" {
					accessURLs[objectURL] = struct{}{}
				}
			}
			missingURLs := make([]string, 0)
			for _, probe := range record.AccessProbes {
				if strings.TrimSpace(probe.Operation) != model.StorageChainValidationModeInventory || strings.TrimSpace(probe.ErrorKind) != "inventory_miss" {
					continue
				}
				if objectURL := sarecord.SyfonProbeObjectURL(probe); objectURL != "" {
					if _, ok := accessURLs[objectURL]; !ok {
						continue
					}
					missingURLs = append(missingURLs, objectURL)
				}
			}
			missingURLs = sarecord.UniqueStrings(missingURLs)
			if len(missingURLs) == 0 {
				continue
			}
			clone := record
			clone.CanonicalAccessURLs = missingURLs
			selected[checksum] = append(selected[checksum], clone)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func countRecordSet(recordSet *sarecord.RecordSet) int {
	if recordSet == nil {
		return 0
	}
	return countRecordStates(recordSet.AllProjectRecords)
}

func recordNeedsTargetedProbe(record sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	resolution := sarecord.ResolveRecordStorage(record, bucketObjectsByURL)
	if len(resolution.MatchedBucketObjectURLs) == 0 {
		return true
	}
	if !sameStringSet(sarecord.RawAccessURLsForRecord(record), sarecord.AccessURLsForStorage(record)) {
		return true
	}
	checksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
	if checksum == "" {
		return false
	}
	for _, objectURL := range resolution.MatchedBucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if sarecord.NormalizeAnalyticsChecksum(item.MetaSHA256) != "" {
			return false
		}
	}
	return true
}

func sameStringSet(left []string, right []string) bool {
	a := sarecord.UniqueStrings(left)
	b := sarecord.UniqueStrings(right)
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, value := range a {
		set[strings.TrimSpace(value)] = struct{}{}
	}
	for _, value := range b {
		if _, ok := set[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func mergeRecordSetProbes(base *sarecord.RecordSet, probed *sarecord.RecordSet) *sarecord.RecordSet {
	if base == nil {
		return probed
	}
	if probed == nil {
		return base
	}
	probesByObjectID := make(map[string][]gintegrationsyfon.BulkStorageProbeResult)
	for _, group := range probed.AllProjectRecords {
		for _, record := range group {
			probesByObjectID[strings.TrimSpace(record.ObjectID)] = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
		}
	}
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				if probes, ok := probesByObjectID[strings.TrimSpace(record.ObjectID)]; ok {
					clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), probes...)
				}
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: attach(base.RecordsByChecksum),
		AllProjectRecords: attach(base.AllProjectRecords),
	}
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

type InflightResponseRefresh struct {
	done               chan struct{}
	forceBucketRefresh bool
	value              storecache.CachedResponse
	err                error
}

// coalesceStorageChainAuditRefresh ensures a root audit has one authoritative
// refresh result per cache key. The UI can issue overlapping forced refreshes;
// without this guard, a partial bucket LIST from the later request can replace
// the completed response from the earlier request.
func applyScopedStorageMappings(recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, scopes []domain.StorageBucketScope, organization string, project string) (map[string][]sarecord.State, map[string][]sarecord.State) {
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, state := range group {
				clone := state
				clone.CanonicalAccessURLs = sarecord.CanonicalizeRecordAccessURLs(state.AccessURLs, scopes, organization, project)
				clone.CanonicalAccessURLByRaw = sarecord.CanonicalizeRecordAccessURLMappings(state.AccessURLs, scopes, organization, project)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return attach(recordsByChecksum), attach(allProjectRecords)
}
