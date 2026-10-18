package syfon

import (
	"strings"
	"time"
)

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func parseOptionalTime(value *string) *time.Time {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*value))
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func dedupeChecksums(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeSHA256(value)
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

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(strings.TrimPrefix(value, "sha256:"))
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

func normalizeSHA256(value string) string {
	trimmed := strings.TrimSpace(strings.TrimPrefix(value, "sha256:"))
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}
