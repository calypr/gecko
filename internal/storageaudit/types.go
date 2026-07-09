package storageaudit

type Evidence struct {
	Checksum          string   `json:"checksum,omitempty"`
	SourcePaths       []string `json:"source_paths,omitempty"`
	ObjectIDs         []string `json:"object_ids,omitempty"`
	AccessURLs        []string `json:"access_urls,omitempty"`
	BucketObjectURLs  []string `json:"bucket_object_urls,omitempty"`
	Buckets           []string `json:"buckets,omitempty"`
	Keys              []string `json:"keys,omitempty"`
	StorageOperations []string `json:"storage_operations,omitempty"`
	ProbeStatuses     []string `json:"probe_statuses,omitempty"`
	ValidationStates  []string `json:"validation_states,omitempty"`
	ErrorKinds        []string `json:"error_kinds,omitempty"`
	Errors            []string `json:"errors,omitempty"`
	BucketEvaluation  string   `json:"bucket_evaluation,omitempty"`
}

type ProjectDiffAuditRequest struct {
	GitSubpath string `json:"git_subpath,omitempty"`
	Ref        string `json:"ref,omitempty"`
}

type ProjectDiffFinding struct {
	Kind              string    `json:"kind"`
	NormalizedPath    string    `json:"normalized_path"`
	Checksum          string    `json:"checksum,omitempty"`
	SourcePaths       []string  `json:"source_paths,omitempty"`
	ObjectIDs         []string  `json:"object_ids"`
	RecordCount       int       `json:"record_count"`
	SizeBytes         int64     `json:"size_bytes,omitempty"`
	DownloadCount     int64     `json:"download_count,omitempty"`
	LastDownload      string    `json:"last_download_time,omitempty"`
	RecommendedAction string    `json:"recommended_action"`
	Evidence          *Evidence `json:"evidence,omitempty"`
}

type ProjectDiffSummary struct {
	CountsByKind         map[string]int `json:"counts_by_kind"`
	TotalFindings        int            `json:"total_findings"`
	IndexedPathCount     int            `json:"indexed_path_count"`
	ExpectedPathCount    int            `json:"expected_path_count"`
	MatchedPathCount     int            `json:"matched_path_count"`
	IncludesRepoManifest bool           `json:"includes_repo_manifest"`
	ScannedRecordCount   int            `json:"scanned_record_count"`
}

type ProjectDiffAuditResponse struct {
	Findings   []ProjectDiffFinding `json:"findings"`
	Summary    ProjectDiffSummary   `json:"summary"`
	PathPrefix string               `json:"path_prefix"`
}

type CleanupAuditRequest struct {
	GitSubpath        string   `json:"git_subpath,omitempty"`
	Ref               string   `json:"ref,omitempty"`
	CheckStorage      bool     `json:"check_storage,omitempty"`
	SelectedRepoPaths []string `json:"selected_repo_paths,omitempty"`
}

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
	BucketInventoryError     string         `json:"bucket_inventory_error,omitempty"`
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

type CleanupAccessProbe struct {
	URL                  string   `json:"url"`
	Operation            string   `json:"operation,omitempty"`
	Provider             string   `json:"provider,omitempty"`
	Bucket               string   `json:"bucket,omitempty"`
	Key                  string   `json:"key,omitempty"`
	Path                 string   `json:"path,omitempty"`
	Exists               *bool    `json:"exists,omitempty"`
	Status               string   `json:"status,omitempty"`
	Error                string   `json:"error,omitempty"`
	ErrorKind            string   `json:"error_kind,omitempty"`
	SizeBytes            *int64   `json:"size_bytes,omitempty"`
	MetaSHA256           string   `json:"meta_sha256,omitempty"`
	ETag                 string   `json:"etag,omitempty"`
	LastModified         string   `json:"last_modified,omitempty"`
	ValidationStatus     string   `json:"validation_status,omitempty"`
	SizeMatch            *bool    `json:"size_match,omitempty"`
	NameMatch            *bool    `json:"name_match,omitempty"`
	SHA256Match          *bool    `json:"sha256_match,omitempty"`
	ValidationMismatches []string `json:"validation_mismatches,omitempty"`
}

type CleanupAccessMethod struct {
	AccessID string   `json:"access_id,omitempty"`
	Type     string   `json:"type,omitempty"`
	URL      string   `json:"url,omitempty"`
	Headers  []string `json:"headers,omitempty"`
}

type CleanupRecordAudit struct {
	ObjectID       string                `json:"object_id"`
	Checksum       string                `json:"checksum,omitempty"`
	NormalizedPath string                `json:"normalized_path,omitempty"`
	CleanupScope   string                `json:"cleanup_scope"`
	AccessURLs     []string              `json:"access_urls,omitempty"`
	AccessMethods  []CleanupAccessMethod `json:"access_methods,omitempty"`
	AccessProbes   []CleanupAccessProbe  `json:"access_probes"`
	Status         string                `json:"status,omitempty"`
	Error          string                `json:"error,omitempty"`
	SizeBytes      int64                 `json:"size,omitempty"`
	LastUpdated    string                `json:"updated_time,omitempty"`
	DownloadCount  int64                 `json:"download_count,omitempty"`
	LastDownload   string                `json:"last_download_time,omitempty"`
}

type CleanupFinding struct {
	Kind                string               `json:"kind"`
	NormalizedPath      string               `json:"normalized_path"`
	Checksum            string               `json:"checksum,omitempty"`
	ObjectIDs           []string             `json:"object_ids"`
	Records             []CleanupRecordAudit `json:"records"`
	RecommendedAction   string               `json:"recommended_action"`
	RepoDeleteCandidate bool                 `json:"repo_delete_candidate"`
	CleanupScope        string               `json:"cleanup_scope"`
	SizeBytes           int64                `json:"total_bytes,omitempty"`
	LastUpdated         string               `json:"last_updated,omitempty"`
	DownloadCount       int64                `json:"download_count,omitempty"`
	LastDownload        string               `json:"last_download_time,omitempty"`
	Actionability       string               `json:"actionability,omitempty"`
	AvailableActions    []string             `json:"available_actions,omitempty"`
	DefaultAction       string               `json:"default_action,omitempty"`
	SupportsDryRun      bool                 `json:"supports_dry_run,omitempty"`
	Evidence            *Evidence            `json:"evidence,omitempty"`
}

type CleanupApplyAction struct {
	Action         string `json:"action"`
	Kind           string `json:"kind,omitempty"`
	NormalizedPath string `json:"normalized_path,omitempty"`
}

type CleanupApplyFinding struct {
	Kind             string               `json:"kind"`
	NormalizedPath   string               `json:"normalized_path"`
	ObjectIDs        []string             `json:"object_ids,omitempty"`
	Records          []CleanupRecordAudit `json:"records,omitempty"`
	BucketObjectURL  string               `json:"bucket_object_url,omitempty"`
	BucketObjectURLs []string             `json:"bucket_object_urls,omitempty"`
	AccessURLs       []string             `json:"access_urls,omitempty"`
	AvailableActions []string             `json:"available_actions,omitempty"`
	DefaultAction    string               `json:"default_action,omitempty"`
	SuggestedAction  string               `json:"suggested_action,omitempty"`
	Evidence         *Evidence            `json:"evidence,omitempty"`
}

type CleanupAuditSummary struct {
	CountsByKind             map[string]int `json:"counts_by_kind"`
	TotalFindings            int            `json:"total_findings"`
	ManualFindingCount       int            `json:"manual_finding_count"`
	RepoDeleteCandidateCount int            `json:"repo_delete_candidate_count"`
	StaleDuplicateCount      int            `json:"stale_duplicate_count"`
	RepoOrphanCount          int            `json:"repo_orphan_count"`
}

type CleanupAuditResponse struct {
	Findings             []CleanupFinding    `json:"findings"`
	Summary              CleanupAuditSummary `json:"summary"`
	ExpectedPathCount    int                 `json:"expected_path_count"`
	IncludesRepoManifest bool                `json:"includes_repo_manifest"`
	PathPrefix           string              `json:"path_prefix"`
}

type CleanupApplyRequest struct {
	GitSubpath                 string                `json:"git_subpath,omitempty"`
	Ref                        string                `json:"ref,omitempty"`
	DeleteRepoOrphans          bool                  `json:"delete_repo_orphans,omitempty"`
	DeleteStaleDuplicates      bool                  `json:"delete_stale_duplicates,omitempty"`
	DeleteBucketOnlyObjects    bool                  `json:"delete_bucket_only_objects,omitempty"`
	RepairBrokenBucketMappings bool                  `json:"repair_broken_bucket_mappings,omitempty"`
	DryRun                     bool                  `json:"dry_run,omitempty"`
	SelectedRepoPaths          []string              `json:"selected_repo_paths,omitempty"`
	Actions                    []CleanupApplyAction  `json:"actions,omitempty"`
	Findings                   []CleanupApplyFinding `json:"findings,omitempty"`
}

type CleanupPurgeResult struct {
	ObjectID string `json:"object_id"`
	Success  *bool  `json:"success"`
	Status   string `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

type CleanupApplyResponse struct {
	DeletedRecordIDs        []string             `json:"deleted_record_ids"`
	DeletedBucketObjectURLs []string             `json:"deleted_bucket_object_urls"`
	UpdatedRecordIDs        []string             `json:"updated_record_ids"`
	PurgeResults            []CleanupPurgeResult `json:"purge_results"`
	RepoDeletePaths         []string             `json:"repo_delete_paths"`
	ManualPaths             []string             `json:"manual_paths"`
	SkippedPaths            []string             `json:"skipped_paths"`
	DryRun                  bool                 `json:"dry_run"`
}
