package git

import (
	"strings"
	"sync"
	"time"
)

const (
	StorageChainProbeModeFull          = "full"
	StorageChainProbeModeInventoryOnly = "inventory_only"

	StorageChainBucketModeItems    = "items"
	StorageChainBucketModeValidate = "validate"
)

type StorageChainAuditOptions struct {
	ProbeMode           string
	BucketInventoryMode string
	BucketPathPrefix    string
	FindingLimit        int
	Timings             *StorageChainAuditTimings
}

type StorageChainAuditStageTiming struct {
	Stage    string
	Duration time.Duration
}

type StorageChainAuditTimings struct {
	mu          sync.Mutex
	stages      []StorageChainAuditStageTiming
	Logf        func(format string, args ...any)
	DebugPrefix string
}

func (timings *StorageChainAuditTimings) StageStart(stage string) {
	if timings == nil || timings.Logf == nil {
		return
	}
	timings.Logf("storage_chain_audit_stage_start %s stage=%s", strings.TrimSpace(timings.DebugPrefix), strings.TrimSpace(stage))
}

func (timings *StorageChainAuditTimings) Record(stage string, duration time.Duration) {
	if timings == nil {
		return
	}
	normalizedStage := strings.TrimSpace(stage)
	timings.mu.Lock()
	timings.stages = append(timings.stages, StorageChainAuditStageTiming{
		Stage:    normalizedStage,
		Duration: duration,
	})
	logf := timings.Logf
	prefix := strings.TrimSpace(timings.DebugPrefix)
	timings.mu.Unlock()
	if logf != nil {
		logf("storage_chain_audit_stage_done %s stage=%s duration_ms=%d", prefix, normalizedStage, duration.Milliseconds())
	}
}

func (timings *StorageChainAuditTimings) Snapshot() []StorageChainAuditStageTiming {
	if timings == nil {
		return nil
	}
	timings.mu.Lock()
	defer timings.mu.Unlock()
	out := make([]StorageChainAuditStageTiming, len(timings.stages))
	copy(out, timings.stages)
	return out
}

func NormalizeStorageChainProbeMode(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "", StorageChainProbeModeFull:
		return StorageChainProbeModeFull, true
	case StorageChainProbeModeInventoryOnly:
		return StorageChainProbeModeInventoryOnly, true
	default:
		return "", false
	}
}

func NormalizeStorageChainBucketInventoryMode(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "", StorageChainBucketModeItems:
		return StorageChainBucketModeItems, true
	case StorageChainBucketModeValidate:
		return StorageChainBucketModeValidate, true
	default:
		return "", false
	}
}
