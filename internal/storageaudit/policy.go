package storageaudit

import "strings"

func AllowsAutomaticRepair(kind string, evidenceStatus EvidenceStatus) bool {
	if evidenceStatus != EvidenceVerified {
		return false
	}
	switch strings.TrimSpace(kind) {
	case "storage_object_missing", "syfon_git_no_bucket", "syfon_missing_bucket_object":
		return true
	default:
		return false
	}
}
