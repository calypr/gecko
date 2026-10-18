package repo

import (
	"time"

	"github.com/calypr/gecko/internal/domain"
)

type Identity = domain.GitRepositoryIdentity

type Ref struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Hash    string `json:"hash"`
	Default bool   `json:"default"`
}

type RefsResponse struct {
	ProjectID     string `json:"project_id"`
	DefaultBranch string `json:"default_branch,omitempty"`
	Refs          []Ref  `json:"refs"`
}

type TreeEntry struct {
	Name           string          `json:"name"`
	Path           string          `json:"path"`
	Type           string          `json:"type"`
	Hash           string          `json:"hash"`
	Size           int64           `json:"size,omitempty"`
	LastModifiedAt *time.Time      `json:"last_modified_at,omitempty"`
	LFSPointer     *LFSPointerInfo `json:"lfs_pointer,omitempty"`
}

type TreeResponseOptions struct {
	IncludeSize         bool
	IncludeLastModified bool
	IncludeLFSPointer   bool
	Limit               int
}

type TreeResponse struct {
	ProjectID  string      `json:"project_id"`
	Ref        string      `json:"ref"`
	Path       string      `json:"path"`
	EntryCount int         `json:"entry_count"`
	Truncated  bool        `json:"truncated,omitempty"`
	Entries    []TreeEntry `json:"entries"`
}

type ManifestResponseOptions struct {
	Limit     int
	Cursor    string
	FilesOnly bool
}

type ManifestResponse struct {
	ProjectID  string      `json:"project_id"`
	Ref        string      `json:"ref"`
	Path       string      `json:"path"`
	EntryCount int         `json:"entry_count"`
	HasMore    bool        `json:"has_more"`
	NextCursor string      `json:"next_cursor,omitempty"`
	Entries    []TreeEntry `json:"entries"`
}

type FileResponse struct {
	ProjectID   string          `json:"project_id"`
	Ref         string          `json:"ref"`
	Path        string          `json:"path"`
	Name        string          `json:"name"`
	Hash        string          `json:"hash"`
	Size        int64           `json:"size"`
	HTMLURL     string          `json:"html_url,omitempty"`
	DownloadURL string          `json:"download_url,omitempty"`
	LFSPointer  *LFSPointerInfo `json:"lfs_pointer,omitempty"`
}

type LFSPointerInfo struct {
	Version string `json:"version"`
	OID     string `json:"oid"`
	Size    int64  `json:"size"`
}
