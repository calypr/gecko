package record

import (
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func BrokenAccessURLsForRecord(record model.CleanupRecordAudit) map[string]struct{} {
	broken := make(map[string]struct{})
	for _, probe := range record.AccessProbes {
		url := NormalizeCleanupSelectionKey(probe.URL)
		if url == "" {
			continue
		}
		if AccessProbeIsBroken(probe) {
			broken[url] = struct{}{}
		}
	}
	for _, accessURL := range record.AccessURLs {
		if strings.TrimSpace(accessURL) == "" {
			continue
		}
		if len(record.AccessProbes) == 0 && RecordStatusMeansBrokenAccess(record.Status) {
			broken[NormalizeCleanupSelectionKey(accessURL)] = struct{}{}
		}
	}
	return broken
}

func AppendReplacementAccessMethods(existing []gintegrationsyfon.ProjectAccessMethod, probes []model.CleanupAccessProbe, brokenURLs map[string]struct{}) []gintegrationsyfon.ProjectAccessMethod {
	seen := make(map[string]struct{}, len(existing)+len(probes))
	out := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(existing)+len(probes))
	for _, method := range existing {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		key := NormalizeCleanupSelectionKey(url)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, method)
	}
	for _, probe := range probes {
		if strings.TrimSpace(probe.Status) != "present" || !ProbeValidationMatched(probe.ValidationStatus) {
			continue
		}
		replacementURL := CanonicalStorageURL(probe.Bucket, probe.Key, probe.URL)
		if replacementURL == "" {
			continue
		}
		key := NormalizeCleanupSelectionKey(replacementURL)
		if _, broken := brokenURLs[key]; broken {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, gintegrationsyfon.ProjectAccessMethod{
			AccessID: "s3",
			Type:     "s3",
			URL:      replacementURL,
		})
	}
	return out
}

func ProbeValidationMatched(status string) bool {
	switch strings.TrimSpace(status) {
	case "", "not_requested", "matched":
		return true
	default:
		return false
	}
}

func RecordStatusMeansBrokenAccess(status string) bool {
	switch strings.TrimSpace(status) {
	case "missing", "error":
		return true
	default:
		return false
	}
}

func AccessProbeIsBroken(probe model.CleanupAccessProbe) bool {
	switch strings.TrimSpace(probe.ErrorKind) {
	case "missing_access_url", "credential_missing":
		return true
	}
	switch strings.TrimSpace(probe.Status) {
	case "missing", "forbidden", "unsupported", "invalid", "error":
		return true
	case "not_found":
		return strings.TrimSpace(probe.Operation) == model.StorageChainValidationModeMetadata
	}
	return false
}
