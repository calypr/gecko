package upload

type FileManifest struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type SessionCreateRequest struct {
	BaseBranch   string         `json:"base_branch"`
	TargetSubdir string         `json:"target_subdirectory"`
	Files        []FileManifest `json:"files"`
}

type FileAttachment struct {
	FileName    string `json:"file_name"`
	TargetPath  string `json:"target_path"`
	Checksum    string `json:"checksum"`
	DRSObjectID string `json:"drs_object_id"`
	Size        int64  `json:"size"`
}

type AttachFilesRequest struct {
	Files []FileAttachment `json:"files"`
}

type FinalizeRequest struct {
	PRTitle string `json:"pr_title"`
	PRBody  string `json:"pr_body"`
}

type FileStatus struct {
	FileName    string `json:"file_name"`
	TargetPath  string `json:"target_path"`
	Size        int64  `json:"size"`
	Checksum    string `json:"checksum,omitempty"`
	DRSObjectID string `json:"drs_object_id,omitempty"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
	Collision   bool   `json:"collision"`
}

type SessionResponse struct {
	SessionID      string       `json:"session_id"`
	ProjectID      string       `json:"project_id"`
	BaseBranch     string       `json:"base_branch"`
	TargetSubdir   string       `json:"target_subdirectory,omitempty"`
	BranchName     string       `json:"branch_name"`
	PRTitle        string       `json:"pr_title"`
	PRBody         string       `json:"pr_body"`
	Status         string       `json:"status"`
	PullRequestURL string       `json:"pull_request_url,omitempty"`
	CommitSHA      string       `json:"commit_sha,omitempty"`
	Files          []FileStatus `json:"files"`
	HasConflicts   bool         `json:"has_conflicts"`
}
