package chain

import (
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	"github.com/calypr/gecko/internal/domain"
)

func applyScopeCanonicalization(recordSet *sarecord.RecordSet, scopes []domain.StorageBucketScope, organization string, project string) *sarecord.RecordSet {
	if recordSet == nil {
		return nil
	}
	recordsByChecksum, allProjectRecords := applyScopedStorageMappings(recordSet.RecordsByChecksum, recordSet.AllProjectRecords, scopes, organization, project)
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: allProjectRecords,
	}
}

func applyScopedStorageMappings(recordsByChecksum map[string][]sarecord.State, allProjectRecords map[string][]sarecord.State, scopes []domain.StorageBucketScope, organization string, project string) (map[string][]sarecord.State, map[string][]sarecord.State) {
	attach := func(input map[string][]sarecord.State) map[string][]sarecord.State {
		out := make(map[string][]sarecord.State, len(input))
		for checksum, group := range input {
			states := make([]sarecord.State, 0, len(group))
			for _, state := range group {
				clone := state
				clone.CanonicalAccessURLs = sarecord.CanonicalizeRecordAccessURLs(state.AccessURLs, scopes, organization, project)
				clone.CanonicalAccessURLByRaw = sarecord.CanonicalizeRecordAccessURLMappings(state.AccessURLs, scopes, organization, project)
				states = append(states, clone)
			}
			out[checksum] = states
		}
		return out
	}
	return attach(recordsByChecksum), attach(allProjectRecords)
}
