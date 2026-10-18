package record

import (
	"time"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

type State struct {
	gintegrationsyfon.ProjectRecord
	CanonicalAccessURLs     []string
	CanonicalAccessURLByRaw map[string]string
	Usage                   gintegrationsyfon.FileUsage
	AccessProbes            []gintegrationsyfon.BulkStorageProbeResult
}

func CountStates(input map[string][]State) int {
	count := 0
	for _, group := range input {
		count += len(group)
	}
	return count
}

type RecordSet struct {
	RecordsByChecksum map[string][]State
	AllProjectRecords map[string][]State
}

type StorageView struct {
	Scopes                   []domain.StorageBucketScope
	BucketObjects            []gintegrationsyfon.ProjectBucketObject
	BucketObjectsByURL       map[string]gintegrationsyfon.ProjectBucketObject
	RecordsByChecksum        map[string][]State
	AllProjectRecords        map[string][]State
	BucketInventoryAvailable bool
	BucketInventoryError     string
	BucketInventoryComplete  bool
	BucketInventoryWarning   string
	BucketInventoryObserved  string
	BucketInventorySource    string
	BucketInventoryStale     bool
}

type BucketInventoryMetadata struct {
	Complete bool
	Warning  string
	Observed string
	Source   string
	Stale    bool
}

type ChainIndex struct {
	Inventory            []model.RepoInventoryFile
	AllRecords           []State
	BucketObjectsByURL   map[string]gintegrationsyfon.ProjectBucketObject
	RepoPathsByChecksum  map[string][]string
	RepoPathsByBucketURL map[string][]string
	RepoChecksumsByPath  map[string]string
	RecordsByBucketURL   map[string][]State
	EquivalentRecordKeys EquivalentRecordKeyIndex
}

type EquivalentRecordKeyIndex struct {
	ByName            map[string]struct{}
	ByNameSize        map[string]map[int64]struct{}
	UnknownSizeByName map[string]struct{}
}

type Aggregate struct {
	Name           string
	Path           string
	RowType        string
	FileCount      int
	RecordCount    int
	TotalBytes     int64
	DownloadCount  int64
	LastDownload   *time.Time
	LatestUpdate   *time.Time
	DuplicateCount int
}

type FindingKind string

const (
	FindingNone               FindingKind = ""
	FindingBrokenAccessURL    FindingKind = "broken_access_url_error"
	FindingBrokenBucketMap    FindingKind = "broken_bucket_mapping"
	FindingObjectMissing      FindingKind = "storage_object_missing"
	FindingValidationMismatch FindingKind = "storage_validation_mismatch"
	FindingProbeError         FindingKind = "storage_probe_error"
)
