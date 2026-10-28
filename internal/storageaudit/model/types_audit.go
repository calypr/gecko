package model

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
