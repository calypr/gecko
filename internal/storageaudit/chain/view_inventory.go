package chain

import (
	"context"
	"fmt"
	"path"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func (service *Service) attachProjectStorageProbes(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.deps.Evidence.AttachStorageProbes(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
	if err != nil {
		return nil, err
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}, nil
}

func (service *Service) attachProjectStorageListValidations(ctx context.Context, authorizationHeader string, recordSet *sarecord.RecordSet) (*sarecord.RecordSet, error) {
	recordsByChecksum, allProjectRecords, err := service.deps.Evidence.AttachStorageListValidations(ctx, authorizationHeader, recordSet.RecordsByChecksum, recordSet.AllProjectRecords)
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
