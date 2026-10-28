package storageaudit

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/chain"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

const storageObservationCheckpointBudget = 5 * time.Second

type storageValidationProgressBackend interface {
	BulkProbeStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	BulkListStorageObjectsWithProgress(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem, onBatch func([]gintegrationsyfon.BulkStorageProbeItem, []gintegrationsyfon.BulkStorageProbeResult) error) ([]gintegrationsyfon.BulkStorageProbeResult, error)
}

type storageObservationKeys struct {
	observation string
	retirement  string
}

func (service *StorageAnalyticsService) attachStorageProbesForAudit(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, options chain.AuditEvidenceOptions) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, false, &options)
}

func (service *StorageAnalyticsService) attachStorageListValidationsForAudit(ctx context.Context, authorizationHeader string, recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, options chain.AuditEvidenceOptions) (map[string][]sarecord.State, map[string][]sarecord.State, error) {
	return service.attachStorageValidationResults(ctx, authorizationHeader, recordsByChecksum, allProjectRecords, true, &options)
}

func (service *StorageAnalyticsService) prepareStorageObservationInputs(ctx context.Context, items []gintegrationsyfon.BulkStorageProbeItem, operation string, options chain.AuditEvidenceOptions) ([]gintegrationsyfon.BulkStorageProbeItem, map[string]gintegrationsyfon.BulkStorageProbeResult, map[string]storageObservationKeys, storecache.StorageObservationVersion, error) {
	if options.BucketObjectsByURL == nil && options.BucketInventoryComplete && !options.BucketInventoryStale {
		options.BucketObjectsByURL = make(map[string]gintegrationsyfon.ProjectBucketObject, len(options.BucketObjects))
		for _, item := range options.BucketObjects {
			if objectURL := sarecord.CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL); objectURL != "" {
				options.BucketObjectsByURL[objectURL] = item
			}
		}
	}
	keysByID := make(map[string]storageObservationKeys, len(items))
	cacheKeys := make([]string, 0, len(items)*2)
	for _, item := range items {
		keys := storageObservationCacheKeys(options, operation, item)
		keysByID[item.ID] = keys
		cacheKeys = append(cacheKeys, keys.observation, keys.retirement)
	}
	if len(items) == 0 {
		return nil, map[string]gintegrationsyfon.BulkStorageProbeResult{}, keysByID, storecache.StorageObservationVersion{}, nil
	}
	version, err := service.observations.NextVersion(ctx, options.Organization, options.Project)
	if err != nil {
		return nil, nil, nil, storecache.StorageObservationVersion{}, fmt.Errorf("allocate storage observation version from %s cache: %w", service.observations.Source(), storecache.WrapCacheOperationError(service.observations.Source(), "next_version", err))
	}
	fenceKeys := make([]string, 0, len(keysByID))
	for _, keys := range keysByID {
		fenceKeys = append(fenceKeys, keys.retirement)
	}
	if err := service.observations.EnsureFences(ctx, options.Organization, options.Project, version, sarecord.UniqueStrings(fenceKeys)); err != nil {
		return nil, nil, nil, storecache.StorageObservationVersion{}, fmt.Errorf("prepare storage observation fences in %s cache: %w", service.observations.Source(), storecache.WrapCacheOperationError(service.observations.Source(), "ensure_fences", err))
	}

	cached := map[string]storecache.CachedStorageObservation{}
	if !options.ForceStorageRevalidation {
		values, err := service.observations.GetMany(ctx, options.Organization, options.Project, version, sarecord.UniqueStrings(cacheKeys))
		if err != nil {
			return nil, nil, nil, storecache.StorageObservationVersion{}, fmt.Errorf("load storage observations from %s cache: %w", service.observations.Source(), storecache.WrapCacheOperationError(service.observations.Source(), "get_many", err))
		}
		cached = values
	}

	resultsByID := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(items))
	misses := make([]gintegrationsyfon.BulkStorageProbeItem, 0, len(items))
	for _, item := range items {
		keys := keysByID[item.ID]
		observation, ok := cached[keys.observation]
		retired, hasRetirement := cached[keys.retirement]
		if !ok || !hasRetirement || !retired.Retired || observation.Version.Epoch != version.Epoch || retired.Version.Epoch != version.Epoch || observation.Retired || !isReusableStorageObservation(observation) || observation.Version.Counter <= retired.Version.Counter {
			misses = append(misses, item)
			continue
		}
		result := cloneBulkStorageProbeResult(observation.Result)
		if strings.TrimSpace(result.ID) != strings.TrimSpace(item.ID) || sarecord.CanonicalStorageURL(result.Bucket, result.Key, result.ObjectURL) != sarecord.CanonicalStorageURL("", "", item.ObjectURL) || strings.TrimSpace(result.Operation) != strings.TrimSpace(operation) {
			return nil, nil, nil, storecache.StorageObservationVersion{}, fmt.Errorf("storage observation cache entry for request %q does not match its target", item.ID)
		}
		if _, err := time.Parse(time.RFC3339Nano, observation.ObservedAt); err != nil {
			return nil, nil, nil, storecache.StorageObservationVersion{}, fmt.Errorf("storage observation cache entry for request %q has invalid observed_at: %w", item.ID, err)
		}
		result.CacheHit = true
		result.ObservedAt = observation.ObservedAt
		resultsByID[item.ID] = result
	}
	if len(misses) == 0 {
		return misses, resultsByID, keysByID, version, nil
	}
	return misses, resultsByID, keysByID, version, nil
}

func (service *StorageAnalyticsService) checkpointStorageObservationBatch(ctx context.Context, batchItems []gintegrationsyfon.BulkStorageProbeItem, batchResults []gintegrationsyfon.BulkStorageProbeResult, operation string, options chain.AuditEvidenceOptions, keysByID map[string]storageObservationKeys, version storecache.StorageObservationVersion) (map[string]gintegrationsyfon.BulkStorageProbeResult, error) {
	requested := make(map[string]gintegrationsyfon.BulkStorageProbeItem, len(batchItems))
	for _, item := range batchItems {
		if _, ok := keysByID[item.ID]; !ok {
			return nil, fmt.Errorf("storage probe progress included unrequested item ID %q", item.ID)
		}
		requested[item.ID] = item
	}
	results := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(batchResults))
	writes := make([]storecache.StorageObservationWrite, 0, len(batchResults)*2)
	for _, result := range batchResults {
		item, ok := requested[result.ID]
		if !ok {
			return nil, fmt.Errorf("storage probe progress returned unrequested item ID %q", result.ID)
		}
		canonicalResultURL := sarecord.CanonicalStorageURL(result.Bucket, result.Key, result.ObjectURL)
		canonicalTargetURL := sarecord.CanonicalStorageURL("", "", item.ObjectURL)
		if canonicalResultURL == "" || canonicalResultURL != canonicalTargetURL {
			return nil, fmt.Errorf("storage probe progress for item %q returned URL %q, expected %q", result.ID, canonicalResultURL, canonicalTargetURL)
		}
		if _, exists := results[result.ID]; exists {
			return nil, fmt.Errorf("storage probe progress returned duplicate item ID %q", result.ID)
		}
		result.Operation = operation
		result.CacheHit = false
		if strings.TrimSpace(result.ObservedAt) == "" {
			result.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if _, err := time.Parse(time.RFC3339Nano, result.ObservedAt); err != nil {
			return nil, fmt.Errorf("storage probe progress for item %q has invalid observed_at: %w", result.ID, err)
		}
		results[result.ID] = result
		keys := keysByID[result.ID]
		if successfulStorageObservation(result) {
			writes = append(writes, storecache.StorageObservationWrite{Key: keys.observation, FenceKey: keys.retirement, Observation: storecache.CachedStorageObservation{
				Version:    version,
				ObservedAt: result.ObservedAt,
				Result:     cloneBulkStorageProbeResult(result),
			}})
		} else if reportedMissingStorageObject(result) {
			retired := storecache.CachedStorageObservation{Version: version, ObservedAt: result.ObservedAt, Retired: true}
			writes = append(writes, storecache.StorageObservationWrite{Key: keys.retirement, FenceKey: keys.retirement, Observation: retired})
		}
	}
	if len(writes) > 0 {
		checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storageObservationCheckpointBudget)
		defer cancel()
		if err := service.observations.SetMany(checkpointCtx, options.Organization, options.Project, writes); err != nil {
			return nil, fmt.Errorf("save storage observation progress to %s cache: %w", service.observations.Source(), storecache.WrapCacheOperationError(service.observations.Source(), "set_many", err))
		}
	}
	for _, item := range batchItems {
		if _, ok := results[item.ID]; !ok {
			return results, fmt.Errorf("storage probe progress omitted requested item %q", item.ID)
		}
	}
	return results, nil
}

func storageObservationCacheKeys(options chain.AuditEvidenceOptions, operation string, item gintegrationsyfon.BulkStorageProbeItem) storageObservationKeys {
	expectedSize := ""
	if item.ExpectedSizeBytes != nil {
		expectedSize = strconv.FormatInt(*item.ExpectedSizeBytes, 10)
	}
	canonicalURL := sarecord.CanonicalStorageURL("", "", item.ObjectURL)
	mappingIdentity := storageObservationMappingIdentity(options.Scopes, options.Organization, options.Project, canonicalURL)
	inventoryFingerprint := storageObservationInventoryFingerprint(options, canonicalURL)
	keys := storageObservationKeys{
		observation: storecache.StorageObservationCacheKey(options.Organization, options.Project, operation, canonicalURL, expectedSize, item.ExpectedSHA256, item.ExpectedName, mappingIdentity, inventoryFingerprint),
		retirement:  storecache.StorageObservationRetirementKey(options.Organization, options.Project, canonicalURL, mappingIdentity),
	}
	return keys
}

func storageObservationMappingIdentity(scopes []domain.StorageBucketScope, organization string, project string, canonicalURL string) string {
	bucket, _, ok := sarecord.ParseStorageURL(canonicalURL)
	if !ok {
		return ""
	}
	type mapping struct {
		Organization string `json:"organization"`
		Project      string `json:"project"`
		Bucket       string `json:"bucket"`
		Path         string `json:"path"`
	}
	mappings := make([]mapping, 0)
	for _, scope := range scopes {
		if !sarecord.StorageScopeApplies(scope, organization, project) {
			continue
		}
		scopeBucket, scopePath, valid := sarecord.ParseStorageScopePath(scope)
		if !valid || !strings.EqualFold(strings.TrimSpace(scopeBucket), bucket) {
			continue
		}
		mappings = append(mappings, mapping{
			Organization: strings.ToLower(strings.TrimSpace(scope.Organization)),
			Project:      strings.ToLower(strings.TrimSpace(scope.ProjectID)),
			Bucket:       strings.TrimSpace(scopeBucket),
			Path:         strings.Trim(strings.TrimSpace(scopePath), "/"),
		})
	}
	sort.Slice(mappings, func(i, j int) bool {
		if mappings[i].Organization != mappings[j].Organization {
			return mappings[i].Organization < mappings[j].Organization
		}
		if mappings[i].Project != mappings[j].Project {
			return mappings[i].Project < mappings[j].Project
		}
		if mappings[i].Bucket != mappings[j].Bucket {
			return mappings[i].Bucket < mappings[j].Bucket
		}
		return mappings[i].Path < mappings[j].Path
	})
	encoded, _ := json.Marshal(mappings)
	return string(encoded)
}

func storageObservationInventoryFingerprint(options chain.AuditEvidenceOptions, canonicalURL string) string {
	if !options.BucketInventoryComplete || options.BucketInventoryStale {
		return ""
	}
	bucket, key, ok := sarecord.ParseStorageURL(canonicalURL)
	if !ok || !storageObservationInventoryCoversTarget(options, bucket, key) {
		return ""
	}
	if item, ok := options.BucketObjectsByURL[canonicalURL]; ok {
		fingerprint, _ := json.Marshal(struct {
			State        string `json:"state"`
			SizeBytes    int64  `json:"size_bytes"`
			MetaSHA256   string `json:"meta_sha256"`
			ETag         string `json:"etag"`
			LastModified string `json:"last_modified"`
		}{"present", item.SizeBytes, strings.TrimSpace(item.MetaSHA256), strings.TrimSpace(item.ETag), strings.TrimSpace(item.LastModified)})
		return string(fingerprint)
	}
	if !storageObservationInventoryCoversTarget(options, bucket, key) {
		return ""
	}
	fingerprint, _ := json.Marshal(struct {
		State string `json:"state"`
	}{"complete_absent"})
	return string(fingerprint)
}

func storageObservationInventoryCoversTarget(options chain.AuditEvidenceOptions, bucket string, key string) bool {
	prefix := strings.Trim(strings.TrimSpace(options.BucketPathPrefix), "/")
	for _, scope := range options.Scopes {
		if !sarecord.StorageScopeApplies(scope, options.Organization, options.Project) {
			continue
		}
		scopeBucket, scopePath, ok := sarecord.ParseStorageScopePath(scope)
		if !ok || !strings.EqualFold(strings.TrimSpace(scopeBucket), bucket) {
			continue
		}
		if !pathHasPrefix(key, scopePath) || !pathHasPrefix(key, prefix) {
			continue
		}
		return true
	}
	return false
}

func pathHasPrefix(path string, prefix string) bool {
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	if cleanPrefix == "" {
		return true
	}
	cleanPath := strings.Trim(strings.TrimSpace(path), "/")
	return cleanPath == cleanPrefix || strings.HasPrefix(cleanPath, cleanPrefix+"/")
}

func isReusableStorageObservation(observation storecache.CachedStorageObservation) bool {
	return !observation.Retired && successfulStorageObservation(observation.Result)
}

func successfulStorageObservation(result gintegrationsyfon.BulkStorageProbeResult) bool {
	return result.Exists && strings.EqualFold(strings.TrimSpace(result.Status), "present") && strings.TrimSpace(result.Error) == "" && strings.TrimSpace(result.ErrorKind) == ""
}

func reportedMissingStorageObject(result gintegrationsyfon.BulkStorageProbeResult) bool {
	if result.Exists {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(result.Status))
	errorKind := strings.ToLower(strings.TrimSpace(result.ErrorKind))
	return status == "not_found" || errorKind == "object_not_found"
}

func cloneBulkStorageProbeResult(result gintegrationsyfon.BulkStorageProbeResult) gintegrationsyfon.BulkStorageProbeResult {
	if result.SizeBytes != nil {
		value := *result.SizeBytes
		result.SizeBytes = &value
	}
	if result.SizeMatch != nil {
		value := *result.SizeMatch
		result.SizeMatch = &value
	}
	if result.NameMatch != nil {
		value := *result.NameMatch
		result.NameMatch = &value
	}
	if result.SHA256Match != nil {
		value := *result.SHA256Match
		result.SHA256Match = &value
	}
	result.ValidationMismatches = append([]string(nil), result.ValidationMismatches...)
	return result
}

type storageObservationResultMap struct {
	mu      sync.Mutex
	results map[string]gintegrationsyfon.BulkStorageProbeResult
}

func (results *storageObservationResultMap) add(batch map[string]gintegrationsyfon.BulkStorageProbeResult) {
	results.mu.Lock()
	for key, result := range batch {
		results.results[key] = result
	}
	results.mu.Unlock()
}

func (results *storageObservationResultMap) snapshot() map[string]gintegrationsyfon.BulkStorageProbeResult {
	results.mu.Lock()
	defer results.mu.Unlock()
	out := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(results.results))
	for key, result := range results.results {
		out[key] = cloneBulkStorageProbeResult(result)
	}
	return out
}
