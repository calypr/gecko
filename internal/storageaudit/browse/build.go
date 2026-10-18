package browse

import (
	"context"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (service *Service) BuildStorageSummary(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) (*model.StorageSummaryResponse, error) {
	state, err := service.store.LoadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, false)
	if err != nil {
		return nil, err
	}
	index, inventory, recordsByChecksum, usageByObjectID := state.Index, state.Inventory, state.RecordsByChecksum, state.UsageByObjectID
	directory, err := repoindex.RepoDirectoryAggregate(index, gitSubpath)
	if err != nil {
		return nil, err
	}
	summaryAgg := summarizeSubtree(gitSubpath, inventory, recordsByChecksum, usageByObjectID, directory.DirectChildCount)
	return &model.StorageSummaryResponse{
		Path:               summaryAgg.Path,
		FileCount:          summaryAgg.FileCount,
		RecordCount:        summaryAgg.RecordCount,
		DirectChildCount:   directory.DirectChildCount,
		TotalBytes:         summaryAgg.TotalBytes,
		DownloadCount:      summaryAgg.DownloadCount,
		LastDownloadTime:   record.FormatOptionalTime(summaryAgg.LastDownload),
		LatestUpdateTime:   record.FormatOptionalTime(summaryAgg.LatestUpdate),
		DuplicatePathCount: summaryAgg.DuplicateCount,
	}, nil
}

func (service *Service) BuildStorageChildren(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string) (*model.StorageChildrenResponse, error) {
	index, err := service.store.LoadRepoIndex(ctx, mirrorPath, ref, repo, hash, nil)
	if err != nil {
		return nil, err
	}
	directory, err := repoindex.RepoDirectoryAggregate(index, gitSubpath)
	if err != nil {
		return nil, err
	}
	aggregates := cloneDirectoryChildren(directory.Children)
	record.SortStorageAggregates(aggregates, sortBy, sortOrder)
	page, err := storageChildrenPageForRequest(aggregates, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
	if err != nil {
		return nil, err
	}
	inventory := filterInventoryForStorageChildren(index.Sidecar.Files, page.items)
	enriched, err := service.enrichStorageChildrenPage(ctx, authorizationHeader, organization, project, gitSubpath, inventory, page.items)
	if err != nil {
		return nil, err
	}
	return &model.StorageChildrenResponse{
		Items:      storageChildrenItemsFromAggregates(enriched),
		HasMore:    page.hasMore,
		NextCursor: page.nextCursor,
	}, nil
}

func (service *Service) BuildStorageFolder(ctx context.Context, authorizationHeader string, organization string, project string, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, limit int, sortBy string, sortOrder string, cursor string, summaryMode string, forceRefresh bool, timings *model.StorageFolderTimings) (*model.StorageFolderResponse, error) {
	if strings.EqualFold(strings.TrimSpace(summaryMode), model.StorageFolderSummaryModeExact) {
		exactStart := time.Now()
		state, err := service.store.LoadJoinState(ctx, authorizationHeader, organization, project, ref, gitSubpath, mirrorPath, repo, hash, forceRefresh)
		timings.Record("exact_join", time.Since(exactStart))
		if err != nil {
			return nil, err
		}
		index, inventory, recordsByChecksum, usageByObjectID := state.Index, state.Inventory, state.RecordsByChecksum, state.UsageByObjectID
		directoryStart := time.Now()
		directory, err := repoindex.RepoDirectoryAggregate(index, gitSubpath)
		timings.Record("directory_aggregate", time.Since(directoryStart))
		if err != nil {
			return nil, err
		}
		summaryAgg := summarizeSubtree(gitSubpath, inventory, recordsByChecksum, usageByObjectID, directory.DirectChildCount)
		pageStart := time.Now()
		aggregates := cloneDirectoryChildren(directory.Children)
		record.SortStorageAggregates(aggregates, sortBy, sortOrder)
		page, err := storageChildrenPageForRequest(aggregates, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
		timings.Record("child_pagination", time.Since(pageStart))
		if err != nil {
			return nil, err
		}
		enrichStart := time.Now()
		pageInventory := filterInventoryForStorageChildren(inventory, page.items)
		enriched := aggregateImmediateChildren(gitSubpath, pageInventory, recordsByChecksum, usageByObjectID, page.items)
		timings.Record("enrich_children_page", time.Since(enrichStart))
		return &model.StorageFolderResponse{
			Summary: model.StorageSummaryResponse{
				Path:               summaryAgg.Path,
				Source:             model.StorageFolderSummarySourceExactJoin,
				FileCount:          summaryAgg.FileCount,
				RecordCount:        summaryAgg.RecordCount,
				DirectChildCount:   directory.DirectChildCount,
				TotalBytes:         summaryAgg.TotalBytes,
				DownloadCount:      summaryAgg.DownloadCount,
				LastDownloadTime:   record.FormatOptionalTime(summaryAgg.LastDownload),
				LatestUpdateTime:   record.FormatOptionalTime(summaryAgg.LatestUpdate),
				DuplicatePathCount: summaryAgg.DuplicateCount,
			},
			Children: model.StorageChildrenResponse{
				Items:      storageChildrenItemsFromAggregates(enriched),
				HasMore:    page.hasMore,
				NextCursor: page.nextCursor,
			},
		}, nil
	}

	indexStart := time.Now()
	index, err := service.store.LoadRepoIndex(ctx, mirrorPath, ref, repo, hash, timings)
	timings.Record("load_repo_index", time.Since(indexStart))
	if err != nil {
		return nil, err
	}
	directoryStart := time.Now()
	directory, err := repoindex.RepoDirectoryServingIndex(index, gitSubpath)
	timings.Record("directory_aggregate", time.Since(directoryStart))
	if err != nil {
		return nil, err
	}
	pageStart := time.Now()
	children, err := storageChildrenResponseForServingIndex(directory, hash, gitSubpath, sortBy, sortOrder, limit, cursor)
	timings.Record("child_pagination", time.Since(pageStart))
	if err != nil {
		return nil, err
	}
	timings.Record("git_index_remote_enrichment", 0)
	normalizedPath := record.NormalizeRepoSubpath(gitSubpath)
	return &model.StorageFolderResponse{
		Summary: model.StorageSummaryResponse{
			Path:             normalizedPath,
			Source:           model.StorageFolderSummarySourceGitIndex,
			FileCount:        directory.Directory.FileCount,
			DirectChildCount: directory.Directory.DirectChildCount,
			TotalBytes:       directory.Directory.TotalBytes,
		},
		Children: children,
	}, nil
}

func summarizeSubtree(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]record.State, usageByObjectID map[string]gintegrationsyfon.FileUsage, directChildCount int) record.Aggregate {
	agg := record.Aggregate{
		Path:    record.NormalizeRepoSubpath(gitSubpath),
		RowType: "directory",
	}
	for _, item := range inventory {
		agg.FileCount++
		agg.TotalBytes += item.Size
		matches := recordsByChecksum[record.NormalizeAnalyticsChecksum(item.Checksum)]
		agg.RecordCount += len(matches)
		if len(matches) > 1 {
			agg.DuplicateCount++
		}
		for _, rec := range matches {
			record.ApplyUsage(&agg, rec)
		}
	}
	_ = usageByObjectID
	_ = directChildCount
	return agg
}

func aggregateImmediateChildren(gitSubpath string, inventory []model.RepoInventoryFile, recordsByChecksum map[string][]record.State, usageByObjectID map[string]gintegrationsyfon.FileUsage, aggregates []record.Aggregate) []record.Aggregate {
	root := record.NormalizeRepoSubpath(gitSubpath)
	if aggregates == nil {
		aggregates = make([]record.Aggregate, 0)
	}
	aggregateLookup := make(map[string]*record.Aggregate, len(aggregates))
	for index := range aggregates {
		aggregateLookup[aggregates[index].Path] = &aggregates[index]
	}
	for _, item := range inventory {
		childName, childPath, childType := record.ImmediateChild(root, item.RepoPath)
		if childPath == "" {
			continue
		}
		wasPrecomputed := true
		agg := aggregateLookup[childPath]
		if agg == nil {
			wasPrecomputed = false
			agg = &record.Aggregate{
				Name:    childName,
				Path:    childPath,
				RowType: childType,
			}
			aggregates = append(aggregates, *agg)
			agg = &aggregates[len(aggregates)-1]
			aggregateLookup[childPath] = agg
		}
		if !wasPrecomputed {
			agg.FileCount++
			agg.TotalBytes += item.Size
		}
		matches := recordsByChecksum[record.NormalizeAnalyticsChecksum(item.Checksum)]
		agg.RecordCount += len(matches)
		if len(matches) > 1 {
			agg.DuplicateCount++
		}
		for _, rec := range matches {
			record.ApplyUsage(agg, rec)
		}
	}
	_ = usageByObjectID
	return aggregates
}
