package chain

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	sarecord "github.com/calypr/gecko/internal/storageaudit/record"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
)

func (service *Service) loadCachedProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	start := time.Now()
	cacheKey := service.projectChainInputCacheKey(organization, project)
	summary, err := service.deps.Records.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("get syfon project metrics summary: %w", err)
	}
	if summary != nil {
		validator := projectAuditRecordValidatorFromSummary(*summary)
		service.chainInputMu.RLock()
		cached, ok := service.projectAuditCache[cacheKey]
		service.chainInputMu.RUnlock()
		if ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			log.Printf("syfon_project_record_cache_hit org=%s project=%s record_count=%d age_ms=%d duration_ms=%d", organization, project, len(cached.records), time.Since(cached.cachedAt).Milliseconds(), time.Since(start).Milliseconds())
			return cached.records, nil
		}

		workKey := projectAuditRecordWorkKey(cacheKey, validator)
		service.chainInputMu.Lock()
		if cached, ok := service.projectAuditCache[cacheKey]; ok && cached.validator == validator && time.Since(cached.cachedAt) < chainProjectRecordCacheMaxAge {
			service.chainInputMu.Unlock()
			log.Printf("syfon_project_record_cache_hit org=%s project=%s record_count=%d age_ms=%d duration_ms=%d", organization, project, len(cached.records), time.Since(cached.cachedAt).Milliseconds(), time.Since(start).Milliseconds())
			return cached.records, nil
		}
		if inflight, ok := service.projectAuditWork[workKey]; ok {
			service.chainInputMu.Unlock()
			<-inflight.done
			if inflight.err != nil {
				return nil, inflight.err
			}
			log.Printf("syfon_project_record_cache_wait org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(inflight.records), time.Since(start).Milliseconds())
			return inflight.records, nil
		}
		inflight := &inflightProjectAuditRecordState{
			done:      make(chan struct{}),
			validator: validator,
		}
		service.projectAuditWork[workKey] = inflight
		service.chainInputMu.Unlock()
		log.Printf("syfon_project_record_cache_refresh org=%s project=%s validator_count=%d validator_latest=%q validator_revision=%q", organization, project, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
		defer func() {
			service.chainInputMu.Lock()
			delete(service.projectAuditWork, workKey)
			close(inflight.done)
			service.chainInputMu.Unlock()
		}()

		ProjectRecords, err := service.deps.Records.ListProjectAuditRecords(context.WithoutCancel(ctx), authorizationHeader, organization, project)
		if err != nil {
			inflight.err = fmt.Errorf("list syfon project audit records: %w", err)
			return nil, inflight.err
		}
		copiedRecords := append([]gintegrationsyfon.ProjectRecord(nil), ProjectRecords...)
		service.chainInputMu.Lock()
		service.projectAuditCache[cacheKey] = cachedProjectAuditRecordState{
			records:   copiedRecords,
			validator: validator,
			cachedAt:  time.Now(),
		}
		service.chainInputMu.Unlock()
		inflight.records = copiedRecords
		log.Printf("syfon_project_record_cache_refreshed org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(copiedRecords), time.Since(start).Milliseconds())
		return copiedRecords, nil
	}
	ProjectRecords, err := service.deps.Records.ListProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("list syfon project audit records: %w", err)
	}
	log.Printf("syfon_project_record_cache_uncached org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(ProjectRecords), time.Since(start).Milliseconds())
	return ProjectRecords, nil
}

func projectAuditRecordWorkKey(cacheKey string, validator projectAuditRecordValidator) string {
	return fmt.Sprintf("%s::records::%d::%s::%s", cacheKey, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
}

func buildProjectAuditRecordSet(projectRecords []gintegrationsyfon.ProjectRecord) *sarecord.RecordSet {
	recordsByChecksum := make(map[string][]sarecord.State)
	for _, record := range projectRecords {
		normalizedChecksum := sarecord.NormalizeAnalyticsChecksum(record.Checksum)
		if normalizedChecksum == "" {
			continue
		}
		record.Checksum = normalizedChecksum
		recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], sarecord.State{
			ProjectRecord: record,
		})
	}
	return &sarecord.RecordSet{
		RecordsByChecksum: recordsByChecksum,
		AllProjectRecords: recordsByChecksum,
	}
}

func projectAuditRecordValidatorFromSummary(summary gintegrationsyfon.ProjectMetricsSummary) projectAuditRecordValidator {
	return projectAuditRecordValidator{
		RecordCount:             summary.RecordCount,
		RecordLatestUpdatedTime: strings.TrimSpace(summary.RecordLatestUpdatedTime),
		RecordRevision:          strings.TrimSpace(summary.RecordRevision),
	}
}

func (service *Service) loadCachedProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string) (*sarecord.RecordSet, error) {
	projectRecords, err := service.loadCachedProjectAuditRecords(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, err
	}
	return buildProjectAuditRecordSet(projectRecords), nil
}
