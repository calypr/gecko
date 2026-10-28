package record

import (
	"path"
	"strings"

	"github.com/calypr/gecko/internal/domain"
)

func CanonicalizeRecordAccessURLs(accessURLs []string, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(accessURLs))
	for _, accessURL := range accessURLs {
		if objectURL := CanonicalizeScopedStorageURL(accessURL, scopes, organization, project); objectURL != "" {
			out = append(out, objectURL)
			continue
		}
		if objectURL := CanonicalStorageURL("", "", accessURL); objectURL != "" {
			out = append(out, objectURL)
		}
	}
	return UniqueStrings(out)
}

func CanonicalizeRecordAccessURLMappings(accessURLs []string, scopes []domain.StorageBucketScope, organization string, project string) map[string]string {
	out := make(map[string]string, len(accessURLs))
	for _, accessURL := range accessURLs {
		rawURL := strings.TrimSpace(accessURL)
		if rawURL == "" {
			continue
		}
		if objectURL := CanonicalizeScopedStorageURL(rawURL, scopes, organization, project); objectURL != "" {
			out[rawURL] = objectURL
			continue
		}
		if objectURL := CanonicalStorageURL("", "", rawURL); objectURL != "" {
			out[rawURL] = objectURL
		}
	}
	return out
}

func CanonicalizeScopedStorageURL(accessURL string, scopes []domain.StorageBucketScope, organization string, project string) string {
	if len(scopes) == 0 {
		return ""
	}
	_, key, ok := ParseStorageURL(accessURL)
	if !ok {
		return ""
	}
	targetBucket := ""
	prefixes := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if !StorageScopeApplies(scope, organization, project) {
			continue
		}
		scopeBucket, scopePrefix, ok := ParseStorageScopePath(scope)
		if !ok {
			continue
		}
		if strings.TrimSpace(scopeBucket) != "" {
			targetBucket = strings.TrimSpace(scopeBucket)
		}
		if strings.TrimSpace(scopePrefix) != "" {
			prefixes = append(prefixes, strings.Trim(strings.TrimSpace(scopePrefix), "/"))
		}
	}
	if targetBucket == "" {
		return ""
	}
	normalizedKey := NormalizeScopedStorageKeyForGecko(key, prefixes)
	if normalizedKey == "" {
		return ""
	}
	return CanonicalStorageURL(targetBucket, normalizedKey, "")
}

func StorageScopeApplies(scope domain.StorageBucketScope, organization string, project string) bool {
	scopeOrg := strings.TrimSpace(scope.Organization)
	if scopeOrg != "" && !strings.EqualFold(scopeOrg, strings.TrimSpace(organization)) {
		return false
	}
	scopeProject := strings.TrimSpace(scope.ProjectID)
	return scopeProject == "" || strings.EqualFold(scopeProject, strings.TrimSpace(project))
}

func ParseStorageScopePath(scope domain.StorageBucketScope) (string, string, bool) {
	bucket := strings.TrimSpace(scope.Bucket)
	pathValue := strings.TrimSpace(scope.Path)
	if pathValue == "" {
		return bucket, "", bucket != ""
	}
	if strings.HasPrefix(strings.ToLower(pathValue), "s3://") {
		parsedBucket, parsedPrefix, ok := ParseStorageURLAllowRoot(pathValue)
		if !ok {
			return bucket, "", bucket != ""
		}
		if bucket == "" {
			bucket = parsedBucket
		}
		return bucket, parsedPrefix, bucket != ""
	}
	return bucket, strings.Trim(pathValue, "/"), bucket != ""
}

func ParseStorageURL(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	rest = strings.TrimLeft(rest, "/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	bucket := strings.TrimSpace(parts[0])
	key := strings.Trim(strings.TrimSpace(parts[1]), "/")
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}

func ParseStorageURLAllowRoot(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	rest = strings.TrimLeft(rest, "/")
	parts := strings.SplitN(rest, "/", 2)
	bucket := strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return bucket, "", true
	}
	return bucket, strings.Trim(strings.TrimSpace(parts[1]), "/"), true
}

func NormalizeScopedStorageKeyForGecko(key string, prefixes []string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	normalizedPrefixes := NormalizedScopePrefixesForGecko(prefixes)
	remainder := key
	for _, prefix := range normalizedPrefixes {
		remainder = TrimLeadingStoragePrefixForGecko(remainder, prefix)
	}
	composedPrefix := strings.Join(normalizedPrefixes, "/")
	switch {
	case composedPrefix == "":
		return remainder
	case remainder == "":
		return composedPrefix
	default:
		return path.Join(composedPrefix, remainder)
	}
}

func NormalizedScopePrefixesForGecko(prefixes []string) []string {
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if prefix == "" {
			continue
		}
		if len(out) == 0 {
			out = append(out, prefix)
			continue
		}
		last := out[len(out)-1]
		switch {
		case prefix == last:
			continue
		case strings.HasPrefix(prefix, last+"/"):
			out[len(out)-1] = prefix
		case strings.HasPrefix(last, prefix+"/"):
			continue
		default:
			out = append(out, prefix)
		}
	}
	return out
}

func TrimLeadingStoragePrefixForGecko(key, prefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if key == "" || prefix == "" {
		return key
	}
	if key == prefix {
		return ""
	}
	if strings.HasPrefix(key, prefix+"/") {
		return strings.TrimPrefix(key, prefix+"/")
	}
	return key
}

func CanonicalStorageURL(bucket string, key string, objectURL string) string {
	cleanBucket := strings.TrimSpace(bucket)
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	if cleanBucket != "" && cleanKey != "" {
		return "s3://" + cleanBucket + "/" + cleanKey
	}
	trimmed := strings.TrimSpace(objectURL)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "s3://") {
		return ""
	}
	rest := strings.TrimPrefix(trimmed, "s3://")
	if strings.HasPrefix(rest, "/") {
		rest = strings.TrimLeft(rest, "/")
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	return "s3://" + strings.TrimSpace(parts[0]) + "/" + strings.Trim(strings.TrimSpace(parts[1]), "/")
}

func ProjectScopeRepoPathObjectURLs(repoPaths []string, scopes []domain.StorageBucketScope, organization string, project string) []string {
	out := make([]string, 0, len(repoPaths))
	for _, repoPath := range repoPaths {
		normalizedPath := NormalizeRepoSubpath(repoPath)
		if normalizedPath == "" {
			continue
		}
		for _, scope := range scopes {
			if !StorageScopeApplies(scope, organization, project) {
				continue
			}
			bucket, prefix, ok := ParseStorageScopePath(scope)
			if !ok || strings.TrimSpace(bucket) == "" {
				continue
			}
			key := normalizedPath
			if strings.TrimSpace(prefix) != "" {
				key = path.Join(strings.Trim(strings.TrimSpace(prefix), "/"), normalizedPath)
			}
			if objectURL := CanonicalStorageURL(bucket, key, ""); objectURL != "" {
				out = append(out, objectURL)
			}
		}
	}
	return UniqueStrings(out)
}
