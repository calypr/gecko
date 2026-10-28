package record

import (
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

func NormalizeCleanupSelectionKey(raw string) string {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return ""
	}
	if objectURL := CanonicalStorageURL("", "", trimmed); objectURL != "" {
		return objectURL
	}
	return trimmed
}

func CleanupSelectionCandidatesForRecords(checksum string, displayPath string, matches []State) []string {
	candidates := []string{displayPath}
	normalizedChecksum := NormalizeAnalyticsChecksum(checksum)
	if normalizedChecksum != "" {
		candidates = append(candidates, normalizedChecksum, "sha256/"+normalizedChecksum)
	}
	for _, match := range matches {
		candidates = append(candidates, match.ObjectID)
		candidates = append(candidates, match.AccessURLs...)
		candidates = append(candidates, match.CanonicalAccessURLs...)
		candidates = append(candidates, RecordStorageCandidateURLs(match)...)
		for _, probe := range match.AccessProbes {
			candidates = append(candidates, probe.ObjectURL)
			candidates = append(candidates, CanonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL))
		}
	}
	return UniqueCleanupSelectionCandidates(candidates)
}

func CleanupSelectionCandidatesForBucketObject(objectURL string, item gintegrationsyfon.ProjectBucketObject) []string {
	return UniqueCleanupSelectionCandidates([]string{
		objectURL,
		item.ObjectURL,
		CanonicalStorageURL(item.Bucket, item.Key, item.ObjectURL),
		item.Key,
	})
}

func UniqueCleanupSelectionCandidates(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := NormalizeCleanupSelectionKey(value)
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

func BucketObjectHasCompleteChain(matches []State, repoPathsByChecksum map[string][]string, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	for _, match := range matches {
		if ClassifyStorageFinding(match, bucketObjectsByURL) != FindingNone {
			continue
		}
		if len(UniqueStrings(repoPathsByChecksum[NormalizeAnalyticsChecksum(match.Checksum)])) > 0 {
			return true
		}
	}
	return false
}

func BuildCleanupFinding(kind string, normalizedPath string, matches []State, repoDeleteCandidate bool, cleanupScope string, action string) model.CleanupFinding {
	records := make([]model.CleanupRecordAudit, 0, len(matches))
	var latestUpdate *time.Time
	var latestDownload *time.Time
	var totalBytes int64
	var totalDownloads int64
	checksum := ""
	for _, match := range matches {
		if checksum == "" {
			checksum = strings.TrimSpace(match.Checksum)
		}
		records = append(records, CleanupRecordAuditForRecord(normalizedPath, cleanupScope, match))
		totalBytes += match.Size
		totalDownloads += match.Usage.DownloadCount
		latestUpdate = LaterTime(latestUpdate, match.UpdatedAt)
		latestDownload = LaterTime(latestDownload, match.Usage.LastDownloadTime)
	}
	bucketEvaluation := "not_checked"
	if cleanupScope == "access_url" {
		bucketEvaluation = "probed"
	}
	actionability, availableActions, defaultAction, supportsDryRun := StorageCleanupActionSupport(kind)
	return model.CleanupFinding{
		Kind:                kind,
		NormalizedPath:      normalizedPath,
		Checksum:            checksum,
		ObjectIDs:           RecordObjectIDs(matches),
		Records:             records,
		RecommendedAction:   action,
		RepoDeleteCandidate: repoDeleteCandidate,
		CleanupScope:        cleanupScope,
		SizeBytes:           totalBytes,
		LastUpdated:         FormatOptionalTime(latestUpdate),
		DownloadCount:       totalDownloads,
		LastDownload:        FormatOptionalTime(latestDownload),
		Actionability:       actionability,
		AvailableActions:    availableActions,
		DefaultAction:       defaultAction,
		SupportsDryRun:      supportsDryRun,
		Evidence:            BuildFindingEvidence(checksum, nil, matches, bucketEvaluation),
	}
}

func CleanupRecordAuditForRecord(normalizedPath string, cleanupScope string, record State) model.CleanupRecordAudit {
	return model.CleanupRecordAudit{
		ObjectID:       record.ObjectID,
		Checksum:       strings.TrimSpace(record.Checksum),
		NormalizedPath: normalizedPath,
		CleanupScope:   cleanupScope,
		AccessURLs:     UniqueStrings(record.AccessURLs),
		AccessMethods:  CleanupAccessMethodsFromProjectMethods(record.AccessMethods),
		AccessProbes:   AccessProbesForRecord(record),
		Status:         AccessStatusForRecord(record),
		Error:          AccessErrorForRecord(record),
		SizeBytes:      record.Size,
		LastUpdated:    FormatOptionalTime(record.UpdatedAt),
		DownloadCount:  record.Usage.DownloadCount,
		LastDownload:   FormatOptionalTime(record.Usage.LastDownloadTime),
	}
}

func CleanupAccessMethodsFromProjectMethods(methods []gintegrationsyfon.ProjectAccessMethod) []model.CleanupAccessMethod {
	out := make([]model.CleanupAccessMethod, 0, len(methods))
	for _, method := range methods {
		out = append(out, model.CleanupAccessMethod{
			AccessID: strings.TrimSpace(method.AccessID),
			Type:     strings.TrimSpace(method.Type),
			URL:      strings.TrimSpace(method.URL),
			Headers:  append([]string(nil), method.Headers...),
		})
	}
	return out
}

func ProjectAccessMethodsFromCleanupMethods(methods []model.CleanupAccessMethod) []gintegrationsyfon.ProjectAccessMethod {
	out := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(methods))
	for _, method := range methods {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		out = append(out, gintegrationsyfon.ProjectAccessMethod{
			AccessID: strings.TrimSpace(method.AccessID),
			Type:     strings.TrimSpace(method.Type),
			URL:      url,
			Headers:  append([]string(nil), method.Headers...),
		})
	}
	return out
}

func RepoOrphanAction(kind string) string {
	if kind == "repo_orphan_live_object" {
		return "Delete Syfon record and purge storage object"
	}
	return "Delete stale Syfon record"
}

func StorageCleanupActionSupport(kind string) (string, []string, string, bool) {
	p := RepairPolicyForKind(kind)
	return p.Actionability, append([]string(nil), p.Actions...), p.DefaultAction, p.SupportsDryRun
}
