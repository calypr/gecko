package domain

import "fmt"

type GitRepositoryIdentity struct {
	Host  string `json:"host"`
	Owner string `json:"owner"`
	Repo  string `json:"repo"`
	URL   string `json:"url"`
}

type GitRepositoryInstallationStatus struct {
	Installed           bool   `json:"installed"`
	InstallationID      *int64 `json:"installation_id,omitempty"`
	Target              string `json:"target,omitempty"`
	TargetType          string `json:"target_type,omitempty"`
	HTMLURL             string `json:"html_url,omitempty"`
	RepositorySelection string `json:"repository_selection,omitempty"`
}

type GitHubInstallationRepository struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	HTMLURL  string `json:"html_url"`
	CloneURL string `json:"clone_url"`
}

type GitHubRepositoryMetadata struct {
	DefaultBranch string `json:"default_branch"`
	HTMLURL       string `json:"html_url"`
}

type StorageBucket struct {
	Bucket    string
	Provider  string
	Endpoint  string
	Region    string
	Resources []string
}

type StorageBucketScope struct {
	Bucket       string
	Organization string
	ProjectID    string
	Path         string
}

type StorageConfig struct {
	Bucket              string
	Provider            string
	Endpoint            string
	Region              string
	AccessKey           string
	SecretKey           string
	Organization        string
	ProjectID           string
	Path                string
	PathPrefix          string
	OrganizationSubPath string
	ProjectSubPath      string
}

type HTTPStatusError struct {
	StatusCode int
	Code       string
	Message    string
}

func (err *HTTPStatusError) Error() string {
	if err == nil {
		return ""
	}
	if err.Message != "" {
		return err.Message
	}
	return fmt.Sprintf("HTTP Status %d", err.StatusCode)
}
