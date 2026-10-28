package model

type ChainAuditRequest struct {
	GitSubpath                  string `json:"git_subpath,omitempty"`
	Ref                         string `json:"ref,omitempty"`
	Refresh                     bool   `json:"refresh,omitempty"`
	ProbeMode                   string `json:"probe_mode,omitempty"`
	ValidationMode              string `json:"validation_mode,omitempty"`
	BucketInventoryMode         string `json:"bucket_inventory_mode,omitempty"`
	BucketPathPrefix            string `json:"bucket_path_prefix,omitempty"`
	FindingKind                 string `json:"finding_kind,omitempty"`
	FindingLimit                int    `json:"finding_limit,omitempty"`
	ForceAuditRefresh           bool   `json:"force_audit_refresh,omitempty"`
	ForceBucketInventoryRefresh bool   `json:"force_bucket_inventory_refresh,omitempty"`
}

type ChainFinding struct {
	Kind              string               `json:"kind"`
	EvidenceStatus    string               `json:"evidence_status"`
	NormalizedPath    string               `json:"normalized_path"`
	Checksum          string               `json:"checksum,omitempty"`
	SourcePaths       []string             `json:"source_paths,omitempty"`
	ObjectIDs         []string             `json:"object_ids"`
	Records           []CleanupRecordAudit `json:"records,omitempty"`
	AccessURLs        []string             `json:"access_urls,omitempty"`
	BucketObjectURL   string               `json:"bucket_object_url,omitempty"`
	ResolvedBucket    string               `json:"resolved_bucket,omitempty"`
	ResolvedKey       string               `json:"resolved_key,omitempty"`
	ProbeStatus       string               `json:"probe_status,omitempty"`
	ErrorKind         string               `json:"error_kind,omitempty"`
	Error             string               `json:"error,omitempty"`
	RecordCount       int                  `json:"record_count"`
	SizeBytes         int64                `json:"size_bytes,omitempty"`
	BucketSizeBytes   int64                `json:"bucket_size_bytes,omitempty"`
	RecommendedAction string               `json:"recommended_action"`
	SuggestedFix      string               `json:"suggested_fix,omitempty"`
	SuggestedAction   string               `json:"suggested_action,omitempty"`
	Actionability     string               `json:"actionability,omitempty"`
	AvailableActions  []string             `json:"available_actions,omitempty"`
	DefaultAction     string               `json:"default_action,omitempty"`
	SupportsDryRun    bool                 `json:"supports_dry_run,omitempty"`
	Evidence          *Evidence            `json:"evidence,omitempty"`
}

// GitOnlySyfonRegistrationRequest names Git LFS paths that the caller wants
// registered in Syfon after Gecko has revalidated their scoped bucket objects.
type GitOnlySyfonRegistrationRequest struct {
	Ref                 string   `json:"ref,omitempty"`
	ExpectedGitRevision string   `json:"expected_git_revision"`
	RepoPaths           []string `json:"repo_paths"`
}

type GitOnlySyfonRegistrationResult struct {
	NormalizedPath  string `json:"normalized_path"`
	Checksum        string `json:"checksum,omitempty"`
	GitSizeBytes    int64  `json:"git_size_bytes,omitempty"`
	BucketObjectURL string `json:"bucket_object_url,omitempty"`
	BucketSizeBytes int64  `json:"bucket_size_bytes,omitempty"`
	Status          string `json:"status"`
	Reason          string `json:"reason,omitempty"`
	ObjectID        string `json:"object_id,omitempty"`
}

type GitOnlySyfonRegistrationResponse struct {
	GitRevision string                           `json:"git_revision"`
	Results     []GitOnlySyfonRegistrationResult `json:"results"`
}

type ChainSummary struct {
	CountsByKind             map[string]int `json:"counts_by_kind"`
	TotalFindings            int            `json:"total_findings"`
	ReturnedFindings         int            `json:"returned_findings"`
	FindingsTruncated        bool           `json:"findings_truncated"`
	FindingLimit             int            `json:"finding_limit,omitempty"`
	ValidationMode           string         `json:"validation_mode,omitempty"`
	GitRevision              string         `json:"git_revision,omitempty"`
	SyfonRevision            string         `json:"syfon_revision,omitempty"`
	ObservedAt               string         `json:"observed_at,omitempty"`
	BucketObjectCount        int            `json:"bucket_object_count"`
	SyfonRecordCount         int            `json:"syfon_record_count"`
	GitTrackedFileCount      int            `json:"git_tracked_file_count"`
	BucketPathExists         *bool          `json:"bucket_path_exists,omitempty"`
	BucketPathObjectURL      string         `json:"bucket_path_object_url,omitempty"`
	BucketSummaryMode        string         `json:"bucket_summary_mode,omitempty"`
	BucketInventoryAvailable bool           `json:"bucket_inventory_available"`
	BucketInventoryComplete  bool           `json:"bucket_inventory_complete"`
	BucketInventoryError     string         `json:"bucket_inventory_error,omitempty"`
	BucketInventoryWarning   string         `json:"bucket_inventory_warning,omitempty"`
	BucketInventoryObserved  string         `json:"bucket_inventory_observed_at,omitempty"`
	BucketInventorySource    string         `json:"bucket_inventory_source,omitempty"`
	AuditCacheHit            bool           `json:"audit_cache_hit,omitempty"`
	AuditCachedAt            string         `json:"audit_cached_at,omitempty"`
	AuditCacheAgeSeconds     int64          `json:"audit_cache_age_seconds,omitempty"`
	AuditRefreshDurationMs   int64          `json:"audit_refresh_duration_ms,omitempty"`
	AuditCacheSource         string         `json:"audit_cache_source,omitempty"`
	AuditCacheError          string         `json:"audit_cache_error,omitempty"`
}

type ChainIssueGroup struct {
	Kind         string `json:"kind"`
	FindingCount int    `json:"finding_count"`
	PathCount    int    `json:"path_count"`
	RecordCount  int    `json:"record_count"`
	ObjectCount  int    `json:"object_count"`
	TotalBytes   int64  `json:"total_bytes,omitempty"`
}

type ChainAuditResponse struct {
	Findings         []ChainFinding    `json:"findings"`
	Groups           []ChainIssueGroup `json:"groups,omitempty"`
	Summary          ChainSummary      `json:"summary"`
	PathPrefix       string            `json:"path_prefix"`
	BucketPathPrefix string            `json:"bucket_path_prefix,omitempty"`
}
