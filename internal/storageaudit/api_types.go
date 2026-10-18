package storageaudit

import "github.com/calypr/gecko/internal/storageaudit/model"

type (
	Evidence                         = model.Evidence
	ProjectDiffAuditRequest          = model.ProjectDiffAuditRequest
	ProjectDiffFinding               = model.ProjectDiffFinding
	ProjectDiffSummary               = model.ProjectDiffSummary
	ProjectDiffAuditResponse         = model.ProjectDiffAuditResponse
	CleanupAuditRequest              = model.CleanupAuditRequest
	ChainAuditRequest                = model.ChainAuditRequest
	ChainFinding                     = model.ChainFinding
	GitOnlySyfonRegistrationRequest  = model.GitOnlySyfonRegistrationRequest
	GitOnlySyfonRegistrationResult   = model.GitOnlySyfonRegistrationResult
	GitOnlySyfonRegistrationResponse = model.GitOnlySyfonRegistrationResponse
	ChainSummary                     = model.ChainSummary
	ChainIssueGroup                  = model.ChainIssueGroup
	ChainAuditResponse               = model.ChainAuditResponse
	CleanupAccessProbe               = model.CleanupAccessProbe
	CleanupAccessMethod              = model.CleanupAccessMethod
	CleanupRecordAudit               = model.CleanupRecordAudit
	CleanupFinding                   = model.CleanupFinding
	CleanupApplyAction               = model.CleanupApplyAction
	CleanupApplyFinding              = model.CleanupApplyFinding
	CleanupAuditSummary              = model.CleanupAuditSummary
	CleanupAuditResponse             = model.CleanupAuditResponse
	CleanupApplyRequest              = model.CleanupApplyRequest
	CleanupPurgeResult               = model.CleanupPurgeResult
	CleanupApplyResponse             = model.CleanupApplyResponse
	AccessMethodProbe                = model.AccessMethodProbe
	AccessMethodChange               = model.AccessMethodChange
	RepoAnalyticsChild               = model.RepoAnalyticsChild
	RepoAnalyticsDirectory           = model.RepoAnalyticsDirectory
	RepoAnalyticsIndexSidecar        = model.RepoAnalyticsIndexSidecar
	StorageSummaryResponse           = model.StorageSummaryResponse
	StorageChildResponseItem         = model.StorageChildResponseItem
	StorageChildrenResponse          = model.StorageChildrenResponse
	StorageFolderResponse            = model.StorageFolderResponse
	RepoInventoryFile                = model.RepoInventoryFile
	StorageChainAuditOptions         = model.StorageChainAuditOptions
	StorageChainAuditStageTiming     = model.StorageChainAuditStageTiming
	StorageChainAuditTimings         = model.StorageChainAuditTimings
	StorageFolderTimings             = model.StorageFolderTimings
)

const (
	StorageChainProbeModeFull           = model.StorageChainProbeModeFull
	StorageChainProbeModeInventoryOnly  = model.StorageChainProbeModeInventoryOnly
	StorageChainBucketModeItems         = model.StorageChainBucketModeItems
	StorageChainBucketModeValidate      = model.StorageChainBucketModeValidate
	StorageChainValidationModeList      = model.StorageChainValidationModeList
	StorageChainValidationModeMetadata  = model.StorageChainValidationModeMetadata
	StorageChainValidationModeInventory = model.StorageChainValidationModeInventory
	StorageFolderSummaryModeExact       = model.StorageFolderSummaryModeExact
	StorageFolderSummarySourceGitIndex  = model.StorageFolderSummarySourceGitIndex
	StorageFolderSummarySourceExactJoin = model.StorageFolderSummarySourceExactJoin
)

var (
	NormalizeStorageChainProbeMode           = model.NormalizeStorageChainProbeMode
	NormalizeStorageChainValidationMode      = model.NormalizeStorageChainValidationMode
	DefaultStorageChainValidationMode        = model.DefaultStorageChainValidationMode
	NormalizeStorageChainBucketInventoryMode = model.NormalizeStorageChainBucketInventoryMode
)
