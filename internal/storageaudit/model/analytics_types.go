package model

import "time"

type RepoAnalyticsChild struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Type       string `json:"type"`
	FileCount  int    `json:"file_count"`
	TotalBytes int64  `json:"total_bytes"`
}

type RepoAnalyticsDirectory struct {
	Path             string               `json:"path"`
	DirectChildCount int                  `json:"direct_child_count"`
	FileCount        int                  `json:"file_count"`
	TotalBytes       int64                `json:"total_bytes"`
	Children         []RepoAnalyticsChild `json:"children"`
}

type RepoAnalyticsIndexSidecar struct {
	SchemaVersion int                      `json:"schema_version"`
	CommitHash    string                   `json:"commit_hash"`
	RefName       string                   `json:"ref_name"`
	GeneratedAt   time.Time                `json:"generated_at"`
	Files         []RepoInventoryFile      `json:"files"`
	Directories   []RepoAnalyticsDirectory `json:"directories"`
}

type StorageSummaryResponse struct {
	Path               string `json:"path"`
	Source             string `json:"source,omitempty"`
	FileCount          int    `json:"file_count"`
	RecordCount        int    `json:"record_count"`
	DirectChildCount   int    `json:"direct_child_count"`
	TotalBytes         int64  `json:"total_bytes"`
	DownloadCount      int64  `json:"download_count"`
	LastDownloadTime   string `json:"last_download_time,omitempty"`
	LatestUpdateTime   string `json:"latest_update_time,omitempty"`
	DuplicatePathCount int    `json:"duplicate_path_count"`
}

type StorageChildResponseItem struct {
	Name             string `json:"name"`
	Path             string `json:"path"`
	Type             string `json:"type"`
	FileCount        int    `json:"file_count"`
	RecordCount      int    `json:"record_count"`
	TotalBytes       int64  `json:"total_bytes"`
	DownloadCount    int64  `json:"download_count"`
	LastDownloadTime string `json:"last_download_time,omitempty"`
	LatestUpdateTime string `json:"latest_update_time,omitempty"`
}

type StorageChildrenResponse struct {
	Items      []StorageChildResponseItem `json:"items"`
	HasMore    bool                       `json:"has_more"`
	NextCursor string                     `json:"next_cursor,omitempty"`
}

type StorageFolderResponse struct {
	Summary  StorageSummaryResponse  `json:"summary"`
	Children StorageChildrenResponse `json:"children"`
}
