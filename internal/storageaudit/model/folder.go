package model

import "time"

const (
	StorageFolderSummaryModeExact       = "exact"
	StorageFolderSummarySourceGitIndex  = "git_index"
	StorageFolderSummarySourceExactJoin = "exact_join"
)

type StorageFolderTimings struct {
	DebugPrefix string
	Logf        func(format string, args ...any)
}

func (timings *StorageFolderTimings) Record(stage string, duration time.Duration) {
	if timings == nil || timings.Logf == nil {
		return
	}
	timings.Logf("storage_folder_stage %s stage=%s duration_ms=%d", timings.DebugPrefix, stage, duration.Milliseconds())
}
