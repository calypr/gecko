package syfon

import "time"

type ProjectRecord struct {
	ObjectID      string
	Name          string
	Checksum      string
	Organization  string
	Project       string
	Size          int64
	UpdatedAt     *time.Time
	CreatedAt     *time.Time
	AccessURLs    []string
	AccessMethods []ProjectAccessMethod
}

type ProjectAccessMethod struct {
	AccessID string
	Type     string
	URL      string
	Headers  []string
}

type ProjectMetricsSummary struct {
	RecordCount             int
	RecordLatestUpdatedTime string
	RecordRevision          string
}

type BulkStorageProbeItem struct {
	ID                string
	ObjectURL         string
	ExpectedSizeBytes *int64
	ExpectedSHA256    string
	ExpectedName      string
}

type BulkStorageProbeResult struct {
	ID                   string
	Operation            string
	ObjectURL            string
	Provider             string
	Bucket               string
	Key                  string
	Path                 string
	Exists               bool
	Status               string
	Error                string
	ErrorKind            string
	VerificationMethod   string
	SizeBytes            *int64
	MetaSHA256           string
	ETag                 string
	LastModified         string
	ValidationStatus     string
	SizeMatch            *bool
	NameMatch            *bool
	SHA256Match          *bool
	ValidationMismatches []string
	ObservedAt           string
	CacheHit             bool
}

type ProjectBucketSummary struct {
	ObjectURL   string
	Provider    string
	Bucket      string
	Prefix      string
	Exists      bool
	ObjectCount int
	TotalBytes  int64
	ComputedAt  string
	Mode        string
}

type ProjectBucketObject struct {
	ObjectURL    string
	Provider     string
	Bucket       string
	Key          string
	Path         string
	SizeBytes    int64
	MetaSHA256   string
	ETag         string
	LastModified string
}

type ProjectBucketInventory struct {
	Objects    []ProjectBucketObject
	Complete   bool
	Warning    string
	ObservedAt string
}

type FileUsage struct {
	ObjectID         string
	Name             string
	Size             int64
	DownloadCount    int64
	UploadCount      int64
	LastAccessTime   *time.Time
	LastDownloadTime *time.Time
	LastUploadTime   *time.Time
}

type ProjectBucketDeleteResult struct {
	ObjectURL string
	Status    string
	Error     string
}

// ProjectObjectRegistration is the narrow subset of DRS registration Gecko
// needs when it restores a Syfon record for an existing Git LFS pointer.
type ProjectObjectRegistration struct {
	Name             string
	Checksum         string
	Size             int64
	ControlledAccess []string
	AccessURLs       []string
}

type ProjectObjectRegistrationResult struct {
	ObjectID string
}
