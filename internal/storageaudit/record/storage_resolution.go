package record

import (
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func RecordsReferenceBucketObject(matches []State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if len(ResolveRecordStorage(match, bucketObjectsByURL).MatchedBucketObjectURLs) > 0 {
			return true
		}
	}
	return false
}

func MatchedBucketObjectURLs(record State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	return ResolveRecordStorage(record, bucketObjectsByURL).MatchedBucketObjectURLs
}

func CleanupMatchedBucketObjectURLs(matches []State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []string {
	objectURLs := make([]string, 0)
	for _, match := range matches {
		objectURLs = append(objectURLs, MatchedBucketObjectURLs(match, bucketObjectsByURL)...)
		for _, probe := range AccessProbesForRecord(match) {
			if strings.TrimSpace(probe.Status) != "present" {
				continue
			}
			objectURL := CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
			if strings.TrimSpace(objectURL) == "" {
				continue
			}
			objectURLs = append(objectURLs, objectURL)
		}
	}
	return UniqueStrings(objectURLs)
}

func RecordBucketURLs(record State) []string {
	return RecordStorageCandidateURLs(record)
}

func RecordStorageCandidateURLs(record State) []string {
	out := make([]string, 0)
	for _, accessURL := range record.CanonicalAccessURLs {
		if objectURL := CanonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	for _, probe := range record.AccessProbes {
		if objectURL := CanonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	if len(record.CanonicalAccessURLs) == 0 {
		for _, accessURL := range RawAccessURLsForRecord(record) {
			if objectURL := CanonicalStorageURL("", "", accessURL); objectURL != "" {
				out = append(out, objectURL)
			}
		}
	}
	return UniqueStrings(out)
}

type Resolution struct {
	CandidateURLs             []string
	MatchedBucketObjectURLs   []string
	HasPresentProbe           bool
	HasMissingProbe           bool
	HasBrokenMappingProbe     bool
	HasProbeError             bool
	HasValidationMismatch     bool
	HasAcceptedCanonicalProbe bool
	HasPresentRawAccessProbe  bool
	HasMissingRawAccessProbe  bool
	HasBrokenRawAccessProbe   bool
	HasRawAccessProbeError    bool
}

func ResolveRecordStorage(record State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) Resolution {
	resolution := Resolution{
		CandidateURLs: RecordStorageCandidateURLs(record),
	}
	rawURLs := make(map[string]struct{}, len(record.AccessURLs))
	rawCandidateURLs := make(map[string]struct{}, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
			rawURLs[trimmed] = struct{}{}
			if objectURL := CanonicalStorageURL("", "", trimmed); objectURL != "" {
				rawCandidateURLs[objectURL] = struct{}{}
			}
		}
	}
	canonicalURLs := make(map[string]struct{}, len(record.CanonicalAccessURLs))
	for _, accessURL := range record.CanonicalAccessURLs {
		trimmed := strings.TrimSpace(accessURL)
		if trimmed == "" {
			continue
		}
		if _, raw := rawURLs[trimmed]; !raw {
			canonicalURLs[trimmed] = struct{}{}
		}
	}
	for _, probe := range record.AccessProbes {
		objectURL := SyfonProbeObjectURL(probe)
		status := strings.TrimSpace(probe.Status)
		_, rawAccessProbe := rawCandidateURLs[objectURL]
		if status == "present" {
			resolution.HasPresentProbe = true
			if rawAccessProbe {
				resolution.HasPresentRawAccessProbe = true
			}
			if _, ok := canonicalURLs[objectURL]; ok {
				switch strings.TrimSpace(probe.ValidationStatus) {
				case "", "not_requested", "matched":
					resolution.HasAcceptedCanonicalProbe = true
				}
			}
		}
		if StorageProbeValidationMismatchIsSignificant(record, probe) {
			resolution.HasValidationMismatch = true
		}
		switch strings.TrimSpace(probe.ErrorKind) {
		case "credential_missing":
			resolution.HasBrokenMappingProbe = true
			if rawAccessProbe {
				resolution.HasBrokenRawAccessProbe = true
			}
		}
		switch status {
		case "not_found":
			if strings.TrimSpace(probe.Operation) == model.StorageChainValidationModeInventory {
				resolution.HasProbeError = true
				if rawAccessProbe {
					resolution.HasRawAccessProbeError = true
				}
				continue
			}
			resolution.HasMissingProbe = true
			if rawAccessProbe {
				resolution.HasMissingRawAccessProbe = true
			}
		case "unknown", "forbidden", "unsupported", "invalid", "error":
			resolution.HasProbeError = true
			if rawAccessProbe {
				resolution.HasRawAccessProbeError = true
			}
		}
	}
	if len(bucketObjectsByURL) > 0 {
		for _, objectURL := range resolution.CandidateURLs {
			if _, ok := bucketObjectsByURL[objectURL]; ok {
				resolution.MatchedBucketObjectURLs = append(resolution.MatchedBucketObjectURLs, objectURL)
			}
		}
		resolution.MatchedBucketObjectURLs = UniqueStrings(resolution.MatchedBucketObjectURLs)
	}
	return resolution
}
