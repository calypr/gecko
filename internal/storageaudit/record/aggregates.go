package record

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

func NormalizeAnalyticsChecksum(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "sha256:")
	trimmed = strings.TrimPrefix(trimmed, "SHA256:")
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

func ApplyUsage(agg *Aggregate, record State) {
	agg.DownloadCount += record.Usage.DownloadCount
	agg.LastDownload = LaterTime(agg.LastDownload, record.Usage.LastDownloadTime)
	agg.LatestUpdate = LaterTime(agg.LatestUpdate, record.UpdatedAt)
}

func AggregateMatchedSize(matches []State, fallback int64) int64 {
	var total int64
	for _, match := range matches {
		if match.Size > 0 {
			total += match.Size
		}
	}
	if total == 0 {
		return fallback
	}
	return total
}

func AggregateMatchedDownloads(matches []State) int64 {
	var total int64
	for _, match := range matches {
		total += match.Usage.DownloadCount
	}
	return total
}

func LatestMatchedDownload(matches []State) *time.Time {
	var latest *time.Time
	for _, match := range matches {
		latest = LaterTime(latest, match.Usage.LastDownloadTime)
	}
	return latest
}

func LaterTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		copyTime := candidate.UTC()
		return &copyTime
	}
	return current
}

func ImmediateChild(root string, repoPath string) (string, string, string) {
	normalizedPath := NormalizeRepoSubpath(repoPath)
	if root != "" {
		prefix := root + "/"
		if !strings.HasPrefix(normalizedPath, prefix) {
			return "", "", ""
		}
		normalizedPath = strings.TrimPrefix(normalizedPath, prefix)
	}
	if normalizedPath == "" {
		return "", "", ""
	}
	parts := strings.Split(normalizedPath, "/")
	childName := parts[0]
	if len(parts) == 1 {
		if root == "" {
			return childName, childName, "file"
		}
		return childName, root + "/" + childName, "file"
	}
	if root == "" {
		return childName, childName, "directory"
	}
	return childName, root + "/" + childName, "directory"
}

func NormalizeRepoSubpath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func RecordObjectIDs(matches []State) []string {
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		if trimmed := strings.TrimSpace(match.ObjectID); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return UniqueStrings(out)
}

func FlattenRecordStates(recordsByChecksum map[string][]State) []State {
	out := make([]State, 0)
	seen := map[string]struct{}{}
	for _, group := range recordsByChecksum {
		for _, record := range group {
			objectID := strings.TrimSpace(record.ObjectID)
			if objectID == "" {
				continue
			}
			if _, ok := seen[objectID]; ok {
				continue
			}
			seen[objectID] = struct{}{}
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObjectID < out[j].ObjectID
	})
	return out
}

func ChainPathCount(gitPaths []string) int {
	if len(gitPaths) == 0 {
		return 1
	}
	return len(gitPaths)
}

func UniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func StorageProbeRequestKey(objectURL string, size int64, checksum string) string {
	return strings.TrimSpace(objectURL) + "|" + fmt.Sprintf("%d", size) + "|" + strings.TrimSpace(checksum)
}

func StorageListValidationRequestKey(objectURL string, size int64, expectedName string) string {
	return strings.TrimSpace(objectURL) + "|" + fmt.Sprintf("%d", size) + "|" + strings.TrimSpace(expectedName)
}

func ExpectedStorageObjectNameForListValidation(objectURL string, recordName string) string {
	expectedName := path.Base(strings.Trim(strings.TrimSpace(recordName), "/"))
	if expectedName == "." || expectedName == "/" || expectedName == "" {
		return ""
	}
	return expectedName
}

func SortStorageAggregates(items []Aggregate, sortBy string, sortOrder string) {
	desc := !strings.EqualFold(strings.TrimSpace(sortOrder), "asc")
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		sort.Slice(items, func(i, j int) bool {
			left := strings.ToLower(items[i].Name)
			right := strings.ToLower(items[j].Name)
			if desc {
				return left > right
			}
			return left < right
		})
	default:
		sort.Slice(items, func(i, j int) bool {
			if items[i].TotalBytes != items[j].TotalBytes {
				if desc {
					return items[i].TotalBytes > items[j].TotalBytes
				}
				return items[i].TotalBytes < items[j].TotalBytes
			}
			if items[i].RowType != items[j].RowType {
				return items[i].RowType == "directory"
			}
			return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
		})
	}
}

func OrphanDisplayPath(checksum string, sourcePaths []string) string {
	if len(sourcePaths) > 0 && strings.TrimSpace(sourcePaths[0]) != "" {
		return strings.TrimSpace(sourcePaths[0])
	}
	return "sha256/" + strings.TrimSpace(checksum)
}

func SyfonRecordDisplayPath(record State) string {
	if name := strings.TrimSpace(record.Name); name != "" {
		return name
	}
	if objectID := strings.TrimSpace(record.ObjectID); objectID != "" {
		return "syfon/" + objectID
	}
	if checksum := strings.TrimSpace(record.Checksum); checksum != "" {
		return "sha256/" + checksum
	}
	return ""
}

func RecordSourcePaths(matches []State) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, match := range matches {
		for _, accessURL := range match.AccessURLs {
			normalized := strings.TrimSpace(accessURL)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			out = append(out, normalized)
		}
	}
	sort.Strings(out)
	return out
}

func FormatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func BoolPtr(value bool) *bool {
	return &value
}

func DifferenceStrings(values []string, toRemove []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	removeSet := make(map[string]struct{}, len(toRemove))
	for _, value := range toRemove {
		removeSet[value] = struct{}{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}
