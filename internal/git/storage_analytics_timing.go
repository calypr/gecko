package git

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	StorageChainProbeModeFull          = "full"
	StorageChainProbeModeInventoryOnly = "inventory_only"

	StorageChainBucketModeItems    = "items"
	StorageChainBucketModeValidate = "validate"

	StorageChainValidationModeList      = "list"
	StorageChainValidationModeMetadata  = "metadata"
	StorageChainValidationModeInventory = "inventory"
)

type StorageChainAuditOptions struct {
	ProbeMode           string
	ValidationMode      string
	BucketInventoryMode string
	BucketPathPrefix    string
	FindingKind         string
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

func (timings *StorageChainAuditTimings) RecordMemory(stage string, fields ...any) {
	if timings == nil || timings.Logf == nil {
		return
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	prefix := strings.TrimSpace(timings.DebugPrefix)
	normalizedStage := strings.TrimSpace(stage)
	extra := formatStorageChainMemoryFields(fields...)
	timings.Logf(
		"storage_chain_audit_memory %s stage=%s alloc_mib=%d heap_alloc_mib=%d heap_inuse_mib=%d heap_sys_mib=%d stack_sys_mib=%d sys_mib=%d next_gc_mib=%d num_gc=%d%s",
		prefix,
		normalizedStage,
		bytesToMiB(stats.Alloc),
		bytesToMiB(stats.HeapAlloc),
		bytesToMiB(stats.HeapInuse),
		bytesToMiB(stats.HeapSys),
		bytesToMiB(stats.StackSys),
		bytesToMiB(stats.Sys),
		bytesToMiB(stats.NextGC),
		stats.NumGC,
		extra,
	)
}

func bytesToMiB(bytes uint64) uint64 {
	return bytes / 1024 / 1024
}

func formatStorageChainMemoryFields(fields ...any) string {
	if len(fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		key := strings.TrimSpace(fmt.Sprint(fields[i]))
		if key == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", key, fields[i+1]))
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
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

func NormalizeStorageChainValidationMode(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "", StorageChainValidationModeList:
		return StorageChainValidationModeList, true
	case StorageChainValidationModeMetadata, StorageChainProbeModeFull:
		return StorageChainValidationModeMetadata, true
	case StorageChainValidationModeInventory, StorageChainProbeModeInventoryOnly:
		return StorageChainValidationModeInventory, true
	default:
		return "", false
	}
}

func DefaultStorageChainValidationMode(probeMode string, bucketMode string) string {
	switch strings.TrimSpace(probeMode) {
	case StorageChainProbeModeInventoryOnly:
		return StorageChainValidationModeInventory
	case StorageChainProbeModeFull:
		return StorageChainValidationModeMetadata
	}
	return StorageChainValidationModeList
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
