package record

import (
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func AccessURLsForStorage(record State) []string {
	if len(record.CanonicalAccessURLs) > 0 {
		return record.CanonicalAccessURLs
	}
	return record.AccessURLs
}

func ProbeAccessURLsForRecord(record State) []string {
	urls := make([]string, 0, len(record.CanonicalAccessURLs)+len(record.AccessURLs)+len(record.AccessProbes))
	if len(record.CanonicalAccessURLs) > 0 {
		urls = append(urls, record.CanonicalAccessURLs...)
	} else {
		urls = append(urls, RawAccessURLsForRecord(record)...)
	}
	for _, probe := range record.AccessProbes {
		if strings.TrimSpace(probe.Operation) != model.StorageChainValidationModeInventory || strings.TrimSpace(probe.Status) != "unknown" {
			continue
		}
		if objectURL := SyfonProbeObjectURL(probe); objectURL != "" {
			urls = append(urls, objectURL)
		}
	}
	return UniqueStrings(urls)
}

func RawAccessURLsForRecord(record State) []string {
	out := make([]string, 0, len(record.AccessURLs))
	for _, accessURL := range record.AccessURLs {
		if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return UniqueStrings(out)
}

func BrokenBucketMappingRepairPlan(records []State) ([]string, map[string][]gintegrationsyfon.ProjectAccessMethod) {
	deleteIDs := make([]string, 0)
	updateAccessMethods := make(map[string][]gintegrationsyfon.ProjectAccessMethod)
	for _, record := range records {
		remainingMethods, shouldDelete, ok := RepairBrokenBucketMappingRecord(record)
		if !ok {
			continue
		}
		if shouldDelete {
			deleteIDs = append(deleteIDs, record.ObjectID)
			continue
		}
		updateAccessMethods[record.ObjectID] = remainingMethods
	}
	return UniqueStrings(deleteIDs), updateAccessMethods
}

func RepairBrokenBucketMappingRecord(record State) ([]gintegrationsyfon.ProjectAccessMethod, bool, bool) {
	if len(record.AccessMethods) == 0 || len(record.AccessProbes) == 0 {
		return nil, false, false
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probesByURL[strings.TrimSpace(probe.ObjectURL)] = append(probesByURL[strings.TrimSpace(probe.ObjectURL)], probe)
	}
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(record.AccessMethods))
	removedAny := false
	for _, method := range record.AccessMethods {
		if !AccessURLHasBrokenBucketMapping(method.URL, probesByURL) {
			remaining = append(remaining, method)
			continue
		}
		removedAny = true
	}
	if !removedAny {
		return nil, false, false
	}
	auditRecord := CleanupRecordAuditForRecord("", "access_url", record)
	remaining = AppendReplacementAccessMethods(remaining, auditRecord.AccessProbes, BrokenAccessURLsForRecord(auditRecord))
	if len(remaining) == 0 {
		return nil, true, true
	}
	return remaining, false, true
}

func AccessURLHasBrokenBucketMapping(accessURL string, probesByURL map[string][]gintegrationsyfon.BulkStorageProbeResult) bool {
	probes := probesByURL[strings.TrimSpace(accessURL)]
	if len(probes) == 0 {
		return false
	}
	hasBrokenBucketMapping := false
	for _, probe := range probes {
		if strings.TrimSpace(probe.Status) == "present" {
			return false
		}
		if SyfonProbeIsBrokenAccess(probe) {
			hasBrokenBucketMapping = true
		}
	}
	return hasBrokenBucketMapping
}

func RepairableBrokenAccessRecord(record State) State {
	probes := RepairableBrokenAccessProbes(record)
	if len(probes) == 0 {
		return record
	}
	clone := record
	clone.AccessProbes = probes
	return clone
}

func RepairableBrokenAccessProbes(record State) []gintegrationsyfon.BulkStorageProbeResult {
	if len(record.AccessProbes) == 0 {
		return nil
	}
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		objectURL := SyfonProbeObjectURL(probe)
		if objectURL == "" {
			continue
		}
		probesByURL[objectURL] = append(probesByURL[objectURL], probe)
	}
	out := make([]gintegrationsyfon.BulkStorageProbeResult, 0)
	for _, accessURL := range RawAccessURLsForRecord(record) {
		rawURL := strings.TrimSpace(accessURL)
		if rawURL == "" {
			continue
		}
		probeURLs := []string{rawURL}
		if mappedURL := strings.TrimSpace(record.CanonicalAccessURLByRaw[rawURL]); mappedURL != "" && mappedURL != rawURL {
			probeURLs = append(probeURLs, mappedURL)
		}
		hasPresentProbe := false
		for _, probeURL := range probeURLs {
			if AccessURLHasPresentProbe(probeURL, probesByURL) {
				hasPresentProbe = true
				break
			}
		}
		if hasPresentProbe {
			continue
		}
		for _, probeURL := range probeURLs {
			for _, probe := range probesByURL[probeURL] {
				if SyfonProbeIsBrokenAccess(probe) {
					probe.ObjectURL = rawURL
					out = append(out, probe)
				}
			}
		}
	}
	return out
}

func RecordHasUsableAccessProbe(record State) bool {
	probesByURL := make(map[string][]gintegrationsyfon.BulkStorageProbeResult, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		if objectURL := SyfonProbeObjectURL(probe); objectURL != "" {
			probesByURL[objectURL] = append(probesByURL[objectURL], probe)
		}
	}
	for _, accessURL := range RawAccessURLsForRecord(record) {
		rawURL := strings.TrimSpace(accessURL)
		if AccessURLHasPresentProbe(rawURL, probesByURL) {
			return true
		}
		mappedURL := strings.TrimSpace(record.CanonicalAccessURLByRaw[rawURL])
		if mappedURL != "" && AccessURLHasPresentProbe(mappedURL, probesByURL) {
			return true
		}
	}
	return false
}

func AccessURLHasPresentProbe(accessURL string, probesByURL map[string][]gintegrationsyfon.BulkStorageProbeResult) bool {
	for _, probe := range probesByURL[strings.TrimSpace(accessURL)] {
		if strings.TrimSpace(probe.Status) != "present" {
			continue
		}
		switch strings.TrimSpace(probe.ValidationStatus) {
		case "", "not_requested", "matched":
			return true
		}
	}
	return false
}

func SyfonProbeObjectURL(probe gintegrationsyfon.BulkStorageProbeResult) string {
	if objectURL := strings.TrimSpace(probe.ObjectURL); objectURL != "" {
		return objectURL
	}
	return CanonicalStorageURL(probe.Bucket, probe.Key, "")
}

func SyfonProbeIsBrokenAccess(probe gintegrationsyfon.BulkStorageProbeResult) bool {
	switch strings.TrimSpace(probe.ErrorKind) {
	case "missing_access_url", "scope_not_found", "credential_missing":
		return true
	}
	return strings.TrimSpace(probe.Operation) == model.StorageChainValidationModeMetadata && strings.TrimSpace(probe.Status) == "not_found"
}

func SyfonProbeHasMismatch(probe gintegrationsyfon.BulkStorageProbeResult, mismatch string) bool {
	for _, item := range probe.ValidationMismatches {
		if strings.TrimSpace(item) == mismatch {
			return true
		}
	}
	return false
}
