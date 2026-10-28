package storageaudit

import (
	"fmt"
	"strings"
)

func NormalizeCleanupAuditRequest(request *CleanupAuditRequest) {
	if request == nil {
		return
	}
	request.GitSubpath = normalizeSubpath(request.GitSubpath)
	request.SelectedRepoPaths = normalizePathList(request.SelectedRepoPaths)
}

func NormalizeCleanupApplyRequest(request *CleanupApplyRequest) {
	if request == nil {
		return
	}
	request.GitSubpath = normalizeSubpath(request.GitSubpath)
	request.SelectedRepoPaths = normalizePathList(request.SelectedRepoPaths)
	for i := range request.Actions {
		request.Actions[i].NormalizedPath = normalizeSubpath(request.Actions[i].NormalizedPath)
		request.Actions[i].Kind = strings.TrimSpace(request.Actions[i].Kind)
		request.Actions[i].Action = strings.TrimSpace(request.Actions[i].Action)
	}
	for i := range request.Findings {
		normalizeCleanupFinding(&request.Findings[i])
	}
}

func NormalizeGitOnlySyfonRegistrationRequest(request *GitOnlySyfonRegistrationRequest) {
	if request == nil {
		return
	}
	request.RepoPaths = normalizePathList(request.RepoPaths)
}

func normalizeCleanupFinding(finding *CleanupApplyFinding) {
	finding.Kind = strings.TrimSpace(finding.Kind)
	finding.NormalizedPath = normalizeSubpath(finding.NormalizedPath)
	finding.ObjectIDs = normalizeStringList(finding.ObjectIDs)
	finding.BucketObjectURL = strings.TrimSpace(finding.BucketObjectURL)
	finding.BucketObjectURLs = normalizePathList(finding.BucketObjectURLs)
	finding.AccessURLs = normalizePathList(finding.AccessURLs)
	finding.AvailableActions = normalizeStringList(finding.AvailableActions)
	finding.DefaultAction = strings.TrimSpace(finding.DefaultAction)
	finding.SuggestedAction = strings.TrimSpace(finding.SuggestedAction)
	if finding.Evidence != nil {
		finding.Evidence.ObjectIDs = normalizeStringList(finding.Evidence.ObjectIDs)
		finding.Evidence.AccessURLs = normalizePathList(finding.Evidence.AccessURLs)
		finding.Evidence.BucketObjectURLs = normalizePathList(finding.Evidence.BucketObjectURLs)
		finding.Evidence.SourcePaths = normalizePathList(finding.Evidence.SourcePaths)
	}
	for i := range finding.Records {
		normalizeCleanupRecord(&finding.Records[i])
	}
}

func normalizeCleanupRecord(record *CleanupRecordAudit) {
	record.ObjectID = strings.TrimSpace(record.ObjectID)
	record.Checksum = strings.TrimSpace(record.Checksum)
	record.NormalizedPath = normalizeSubpath(record.NormalizedPath)
	record.CleanupScope = strings.TrimSpace(record.CleanupScope)
	record.AccessURLs = normalizePathList(record.AccessURLs)
	for i := range record.AccessMethods {
		record.AccessMethods[i].AccessID = strings.TrimSpace(record.AccessMethods[i].AccessID)
		record.AccessMethods[i].Type = strings.TrimSpace(record.AccessMethods[i].Type)
		record.AccessMethods[i].URL = strings.TrimSpace(record.AccessMethods[i].URL)
		record.AccessMethods[i].Headers = normalizeStringList(record.AccessMethods[i].Headers)
	}
	for i := range record.AccessProbes {
		record.AccessProbes[i].URL = strings.TrimSpace(record.AccessProbes[i].URL)
		record.AccessProbes[i].Status = strings.TrimSpace(record.AccessProbes[i].Status)
		record.AccessProbes[i].ErrorKind = strings.TrimSpace(record.AccessProbes[i].ErrorKind)
	}
}

func normalizeSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func normalizePathList(values []string) []string {
	return normalizeList(values, normalizeSubpath)
}

func normalizeStringList(values []string) []string {
	return normalizeList(values, strings.TrimSpace)
}

func normalizeList(values []string, normalize func(string) string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := normalize(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

type NormalizedChainAuditRequest struct {
	GitSubpath       string
	BucketPathPrefix string
	FindingKind      string
	Options          StorageChainAuditOptions
}

func NormalizeStorageChainAuditRequest(request ChainAuditRequest) (NormalizedChainAuditRequest, error) {
	probeMode, ok := NormalizeStorageChainProbeMode(request.ProbeMode)
	if !ok {
		return NormalizedChainAuditRequest{}, fmt.Errorf("probe_mode must be either full or inventory_only")
	}
	validationMode := StorageChainValidationModeList
	if strings.TrimSpace(request.ValidationMode) != "" {
		var valid bool
		validationMode, valid = NormalizeStorageChainValidationMode(request.ValidationMode)
		if !valid {
			return NormalizedChainAuditRequest{}, fmt.Errorf("validation_mode must be either list, metadata, or inventory")
		}
	} else if strings.TrimSpace(request.ProbeMode) != "" {
		validationMode = DefaultStorageChainValidationMode(probeMode, "")
	}
	bucketMode := StorageChainBucketModeValidate
	if strings.TrimSpace(request.BucketInventoryMode) != "" {
		var valid bool
		bucketMode, valid = NormalizeStorageChainBucketInventoryMode(request.BucketInventoryMode)
		if !valid {
			return NormalizedChainAuditRequest{}, fmt.Errorf("bucket_inventory_mode must be either validate or items")
		}
	}
	if validationMode == StorageChainValidationModeInventory && strings.TrimSpace(request.BucketInventoryMode) == "" {
		bucketMode = StorageChainBucketModeItems
	}
	if request.FindingLimit < -1 {
		return NormalizedChainAuditRequest{}, fmt.Errorf("finding_limit must be -1, 0, or a positive integer")
	}
	bucketPathPrefix := normalizeSubpath(request.BucketPathPrefix)
	if bucketPathPrefix != "" && bucketMode != StorageChainBucketModeItems {
		return NormalizedChainAuditRequest{}, fmt.Errorf("bucket_path_prefix requires bucket_inventory_mode=items")
	}
	forceBucketRefresh := request.ForceBucketInventoryRefresh
	forceAuditRefresh := request.Refresh || request.ForceAuditRefresh || forceBucketRefresh
	return NormalizedChainAuditRequest{
		GitSubpath:       normalizeSubpath(request.GitSubpath),
		BucketPathPrefix: bucketPathPrefix,
		FindingKind:      strings.TrimSpace(request.FindingKind),
		Options: StorageChainAuditOptions{
			ProbeMode:           probeMode,
			ValidationMode:      validationMode,
			BucketInventoryMode: bucketMode,
			BucketPathPrefix:    bucketPathPrefix,
			FindingKind:         strings.TrimSpace(request.FindingKind),
			FindingLimit:        request.FindingLimit,
			ForceAuditRefresh:   forceAuditRefresh,
			ForceBucketRefresh:  forceBucketRefresh,
		},
	}, nil
}
