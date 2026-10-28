package model

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
	ObservedAt           string   `json:"observed_at,omitempty"`
	CacheHit             bool     `json:"cache_hit,omitempty"`
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
	ExpectedGitRevision        string                `json:"expected_git_revision,omitempty"`
	ExpectedSyfonRevision      string                `json:"expected_syfon_revision,omitempty"`
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
	AccessMethodChanges     []AccessMethodChange `json:"access_method_changes,omitempty"`
}

type AccessMethodProbe struct {
	URL                string `json:"url"`
	Status             string `json:"status"`
	ErrorKind          string `json:"error_kind,omitempty"`
	VerificationMethod string `json:"verification_method,omitempty"`
	ValidationStatus   string `json:"validation_status,omitempty"`
}

type AccessMethodChange struct {
	ObjectID    string                `json:"object_id"`
	Before      []CleanupAccessMethod `json:"before"`
	After       []CleanupAccessMethod `json:"after"`
	Removed     []string              `json:"removed"`
	Retained    []string              `json:"retained"`
	Added       []string              `json:"added"`
	FreshProbes []AccessMethodProbe   `json:"fresh_probes"`
}
