package chain

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/sharedwork"
)

func (service *Service) loadProjectAuditRecords(ctx context.Context, authorizationHeader string, organization string, project string) ([]gintegrationsyfon.ProjectRecord, error) {
	records, _, err := service.loadProjectAuditRecordsWithRevision(ctx, authorizationHeader, organization, project, false, true)
	return records, err
}

func (service *Service) loadProjectAuditRecordsWithRevision(ctx context.Context, authorizationHeader string, organization string, project string, forceRefresh bool, share bool) ([]gintegrationsyfon.ProjectRecord, string, error) {
	start := time.Now()
	cacheKey := service.projectChainInputCacheKey(organization, project)
	summary, err := service.deps.Records.GetProjectMetricsSummary(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, "", fmt.Errorf("get syfon project metrics summary: %w", err)
	}
	validator := projectAuditRecordValidator{}
	if summary != nil {
		validator = projectAuditRecordValidatorFromSummary(*summary)
	}
	syfonRevision := syfonRevisionFromProjectAuditValidator(validator)
	if !share {
		projectRecords, err := service.deps.Records.ListProjectAuditRecords(ctx, authorizationHeader, organization, project)
		if err != nil {
			return nil, "", fmt.Errorf("list syfon project audit records: %w", err)
		}
		return append([]gintegrationsyfon.ProjectRecord(nil), projectRecords...), syfonRevision, nil
	}
	workKey := projectAuditRecordWorkKey(cacheKey, validator)
	projectRecords, joined, err := service.projectAuditWork.Do(ctx, cacheKey, sharedwork.Demand{
		AuthorizationScope: sharedwork.AuthorizationScope(authorizationHeader),
		SemanticKey:        workKey,
		ForceAudit:         forceRefresh,
	}, func(workCtx context.Context) ([]gintegrationsyfon.ProjectRecord, error) {
		log.Printf("syfon_project_record_refresh org=%s project=%s validator_count=%d validator_latest=%q validator_revision=%q force_refresh=%t", organization, project, validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision, forceRefresh)
		projectRecords, err := service.deps.Records.ListProjectAuditRecords(workCtx, authorizationHeader, organization, project)
		if err != nil {
			return nil, fmt.Errorf("list syfon project audit records: %w", err)
		}
		return append([]gintegrationsyfon.ProjectRecord(nil), projectRecords...), nil
	})
	if err != nil {
		return nil, "", err
	}
	if joined {
		log.Printf("syfon_project_record_refresh_join org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(projectRecords), time.Since(start).Milliseconds())
	} else if summary == nil {
		log.Printf("syfon_project_record_load org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(projectRecords), time.Since(start).Milliseconds())
	} else if forceRefresh {
		log.Printf("syfon_project_record_forced_refresh org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(projectRecords), time.Since(start).Milliseconds())
	} else {
		log.Printf("syfon_project_record_loaded org=%s project=%s record_count=%d duration_ms=%d", organization, project, len(projectRecords), time.Since(start).Milliseconds())
	}
	return append([]gintegrationsyfon.ProjectRecord(nil), projectRecords...), syfonRevision, nil
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

func syfonRevisionFromProjectAuditValidator(validator projectAuditRecordValidator) string {
	return fmt.Sprintf("%d:%s:%s", validator.RecordCount, validator.RecordLatestUpdatedTime, validator.RecordRevision)
}

func (service *Service) loadProjectAuditRecordSet(ctx context.Context, authorizationHeader string, organization string, project string, forceRefresh bool, share bool) (*sarecord.RecordSet, string, error) {
	projectRecords, syfonRevision, err := service.loadProjectAuditRecordsWithRevision(ctx, authorizationHeader, organization, project, forceRefresh, share)
	if err != nil {
		return nil, "", err
	}
	return buildProjectAuditRecordSet(projectRecords), syfonRevision, nil
}
