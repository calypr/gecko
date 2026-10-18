package storecache

import (
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
)

func CloneCachedResponse(value CachedResponse) CachedResponse {
	return CachedResponse{
		CachedAt:              value.CachedAt,
		RefreshDurationMillis: value.RefreshDurationMillis,
		Response:              CloneResponse(value.Response),
	}
}

func CloneCachedBucketInventory(value CachedBucketInventory) CachedBucketInventory {
	return CachedBucketInventory{
		CachedAt: value.CachedAt,
		Objects:  append([]gintegrationsyfon.ProjectBucketObject(nil), value.Objects...),
		Complete: value.Complete,
		Warning:  value.Warning,
		Observed: value.Observed,
	}
}

func CloneResponse(response model.ChainAuditResponse) model.ChainAuditResponse {
	response.Findings = append([]model.ChainFinding(nil), response.Findings...)
	response.Groups = append([]model.ChainIssueGroup(nil), response.Groups...)
	response.Summary.CountsByKind = CloneStringIntMap(response.Summary.CountsByKind)
	return response
}

func CloneCachedExactJoinState(value CachedExactJoinState) CachedExactJoinState {
	return CachedExactJoinState{
		CachedAt:          value.CachedAt,
		RecordsByChecksum: cloneRecordStateMap(value.RecordsByChecksum),
		UsageByObjectID:   CloneFileUsageMap(value.UsageByObjectID),
	}
}

func CloneFileUsageMap(input map[string]gintegrationsyfon.FileUsage) map[string]gintegrationsyfon.FileUsage {
	if input == nil {
		return nil
	}
	out := make(map[string]gintegrationsyfon.FileUsage, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func CloneStringIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	out := make(map[string]int, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneRecordStateMap(input map[string][]record.State) map[string][]record.State {
	return record.CloneStateMap(input)
}
