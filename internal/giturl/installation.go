package giturl

import (
	"net/url"
	"strings"
)

func NormalizeInstallationHTMLURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	values := parsed.Query()
	stripEmptyRepositoryIDs(values, "repository_ids")
	stripEmptyRepositoryIDs(values, "repository_ids[]")
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

func stripEmptyRepositoryIDs(values url.Values, key string) bool {
	repositoryIDs, hasRepositoryIDs := values[key]
	if !hasRepositoryIDs {
		return false
	}
	for _, repositoryID := range repositoryIDs {
		normalizedID := strings.TrimSpace(repositoryID)
		if normalizedID != "" && normalizedID != "[]" {
			return true
		}
	}
	values.Del(key)
	return true
}
