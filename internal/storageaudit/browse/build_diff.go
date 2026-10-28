package browse

import (
	"context"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *Service) BuildProjectDiffAudit(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*model.ProjectDiffAuditResponse, error) {
	state, err := service.store.LoadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
	if err != nil {
		return nil, err
	}
	inventory, recordsByChecksum, usageByObjectID := state.Inventory, state.RecordsByChecksum, state.UsageByObjectID
	allProjectRecords, err := service.store.ListProjectRecordStates(ctx, authorizationHeader, organization, project, usageByObjectID)
	if err != nil {
		return nil, err
	}
	audit := buildProjectDiffAuditModel(gitSubpath, inventory, recordsByChecksum, allProjectRecords)
	return &model.ProjectDiffAuditResponse{
		Findings:   audit.Findings,
		Summary:    audit.Summary,
		PathPrefix: audit.PathPrefix,
	}, nil
}

type projectDiffAuditModel struct {
	Findings   []model.ProjectDiffFinding
	Summary    model.ProjectDiffSummary
	PathPrefix string
}

func buildProjectDiffAuditModel(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]record.State, allProjectRecords map[string][]record.State) *projectDiffAuditModel {
	findings := make([]model.ProjectDiffFinding, 0)
	countsByKind := map[string]int{
		"duplicate_syfon_paths": 0,
		"syfon_missing_in_repo": 0,
		"repo_missing_in_syfon": 0,
		"unknown":               0,
	}
	matchedPathCount := 0
	scannedRecordCount := 0
	repoChecksums := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		normalizedChecksum := record.NormalizeAnalyticsChecksum(item.Checksum)
		repoChecksums[normalizedChecksum] = struct{}{}
		matches := recordsByChecksum[normalizedChecksum]
		scannedRecordCount += len(matches)
		if len(matches) > 0 {
			matchedPathCount++
		}
		if len(matches) == 0 {
			evidence := record.BuildFindingEvidence(item.Checksum, []string{item.RepoPath}, nil, "not_checked")
			findings = append(findings, model.ProjectDiffFinding{
				Kind:              "repo_missing_in_syfon",
				NormalizedPath:    item.RepoPath,
				Checksum:          item.Checksum,
				SourcePaths:       []string{item.RepoPath},
				ObjectIDs:         []string{},
				RecordCount:       0,
				SizeBytes:         item.Size,
				RecommendedAction: "No Syfon record matched this Git-tracked checksum. Bucket presence is not part of this check; review ingest or metadata mapping for this path.",
				Evidence:          evidence,
			})
			countsByKind["repo_missing_in_syfon"]++
			continue
		}
		if len(matches) > 1 {
			evidence := record.BuildFindingEvidence(item.Checksum, []string{item.RepoPath}, matches, "not_checked")
			findings = append(findings, model.ProjectDiffFinding{
				Kind:              "duplicate_syfon_paths",
				NormalizedPath:    item.RepoPath,
				Checksum:          item.Checksum,
				SourcePaths:       record.RecordSourcePaths(matches),
				ObjectIDs:         record.RecordObjectIDs(matches),
				RecordCount:       len(matches),
				SizeBytes:         record.AggregateMatchedSize(matches, item.Size),
				DownloadCount:     record.AggregateMatchedDownloads(matches),
				LastDownload:      record.FormatOptionalTime(record.LatestMatchedDownload(matches)),
				RecommendedAction: "Review duplicate Syfon records before deleting anything.",
				Evidence:          evidence,
			})
			countsByKind["duplicate_syfon_paths"]++
		}
	}
	seenOrphanChecksums := map[string]struct{}{}
	for checksum, matches := range allProjectRecords {
		if len(matches) == 0 {
			continue
		}
		if _, ok := repoChecksums[checksum]; ok {
			continue
		}
		if _, ok := seenOrphanChecksums[checksum]; ok {
			continue
		}
		seenOrphanChecksums[checksum] = struct{}{}
		sourcePaths := record.RecordSourcePaths(matches)
		evidence := record.BuildFindingEvidence(checksum, nil, matches, "not_checked")
		findings = append(findings, model.ProjectDiffFinding{
			Kind:              "syfon_missing_in_repo",
			NormalizedPath:    record.OrphanDisplayPath(checksum, sourcePaths),
			Checksum:          checksum,
			SourcePaths:       sourcePaths,
			ObjectIDs:         record.RecordObjectIDs(matches),
			RecordCount:       len(matches),
			SizeBytes:         record.AggregateMatchedSize(matches, 0),
			DownloadCount:     record.AggregateMatchedDownloads(matches),
			LastDownload:      record.FormatOptionalTime(record.LatestMatchedDownload(matches)),
			RecommendedAction: "Prepare delete to verify storage before removing Syfon-only records.",
			Evidence:          evidence,
		})
		countsByKind["syfon_missing_in_repo"]++
	}
	return &projectDiffAuditModel{
		Findings: findings,
		Summary: model.ProjectDiffSummary{
			CountsByKind:         countsByKind,
			TotalFindings:        len(findings),
			IndexedPathCount:     matchedPathCount,
			ExpectedPathCount:    len(inventory),
			MatchedPathCount:     matchedPathCount,
			IncludesRepoManifest: true,
			ScannedRecordCount:   scannedRecordCount,
		},
		PathPrefix: record.NormalizeRepoSubpath(gitSubpath),
	}
}

func cloneDirectoryChildren(children []model.RepoAnalyticsChild) []record.Aggregate {
	out := make([]record.Aggregate, 0, len(children))
	for _, child := range children {
		out = append(out, record.Aggregate{
			Name:       child.Name,
			Path:       child.Path,
			RowType:    child.Type,
			FileCount:  child.FileCount,
			TotalBytes: child.TotalBytes,
		})
	}
	return out
}
