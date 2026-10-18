package chain

import (
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/storageaudit/model"
)

func cloneRecordStateMap(input map[string][]sarecord.State) map[string][]sarecord.State {
	out := make(map[string][]sarecord.State, len(input))
	for checksum, group := range input {
		states := make([]sarecord.State, 0, len(group))
		for _, record := range group {
			clone := record
			clone.CanonicalAccessURLs = append([]string(nil), record.CanonicalAccessURLs...)
			clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
			states = append(states, clone)
		}
		out[checksum] = states
	}
	return out
}

func selectTargetedProbeRecordSet(recordSet *sarecord.RecordSet, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			if !recordNeedsTargetedProbe(record, bucketObjectsByURL) {
				continue
			}
			selected[checksum] = append(selected[checksum], record)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func selectInventoryMissRecordSet(recordSet *sarecord.RecordSet) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	selected := make(map[string][]sarecord.State)
	for checksum, group := range recordSet.AllProjectRecords {
		for _, record := range group {
			accessURLs := make(map[string]struct{})
			for _, accessURL := range sarecord.AccessURLsForStorage(record) {
				if objectURL := sarecord.CanonicalStorageURL("", "", accessURL); objectURL != "" {
					accessURLs[objectURL] = struct{}{}
				}
			}
			missingURLs := make([]string, 0)
			for _, probe := range record.AccessProbes {
				if strings.TrimSpace(probe.Operation) != model.StorageChainValidationModeInventory || strings.TrimSpace(probe.ErrorKind) != "inventory_miss" {
					continue
				}
				if objectURL := sarecord.SyfonProbeObjectURL(probe); objectURL != "" {
					if _, ok := accessURLs[objectURL]; !ok {
						continue
					}
					missingURLs = append(missingURLs, objectURL)
				}
			}
			missingURLs = sarecord.UniqueStrings(missingURLs)
			if len(missingURLs) == 0 {
				continue
			}
			clone := record
			clone.CanonicalAccessURLs = missingURLs
			selected[checksum] = append(selected[checksum], clone)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: cloneRecordStateMap(selected),
		AllProjectRecords: cloneRecordStateMap(selected),
	}
}

func countRecordSet(recordSet *sarecord.RecordSet) int {
	if recordSet == nil {
		return 0
	}
	return sarecord.CountStates(recordSet.AllProjectRecords)
}

func recordNeedsTargetedProbe(record sarecord.State, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) bool {
	resolution := sarecord.ResolveRecordStorage(record, bucketObjectsByURL)
	if len(resolution.MatchedBucketObjectURLs) == 0 {
		return true
	}
	if !sameStringSet(sarecord.RawAccessURLsForRecord(record), sarecord.AccessURLsForStorage(record)) {
		return true
	}
	checksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
	if checksum == "" {
		return false
	}
	for _, objectURL := range resolution.MatchedBucketObjectURLs {
		item, ok := bucketObjectsByURL[objectURL]
		if !ok {
			continue
		}
		if sarecord.NormalizeAnalyticsChecksum(item.MetaSHA256) != "" {
			return false
		}
	}
	return true
}

func sameStringSet(left []string, right []string) bool {
	a := sarecord.UniqueStrings(left)
	b := sarecord.UniqueStrings(right)
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, value := range a {
		set[strings.TrimSpace(value)] = struct{}{}
	}
	for _, value := range b {
		if _, ok := set[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func mergeRecordSetProbes(base *sarecord.RecordSet, probed *sarecord.RecordSet) *sarecord.RecordSet {
	if base == nil {
		return probed
	}
	if probed == nil {
		return base
	}
	probesByObjectID := make(map[string][]gintegrationsyfon.BulkStorageProbeResult)
	for _, group := range probed.AllProjectRecords {
		for _, record := range group {
			probesByObjectID[strings.TrimSpace(record.ObjectID)] = append([]gintegrationsyfon.BulkStorageProbeResult(nil), record.AccessProbes...)
		}
	}
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, record := range group {
				clone := record
				if probes, ok := probesByObjectID[strings.TrimSpace(record.ObjectID)]; ok {
					clone.AccessProbes = append([]gintegrationsyfon.BulkStorageProbeResult(nil), probes...)
				}
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: attach(base.RecordsByChecksum),
		AllProjectRecords: attach(base.AllProjectRecords),
	}
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
