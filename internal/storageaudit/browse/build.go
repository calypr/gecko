package browse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

type storageChildrenCursor struct {
	CommitHash string `json:"commit_hash"`
	GitSubpath string `json:"git_subpath"`
	SortBy     string `json:"sort_by"`
	SortOrder  string `json:"sort_order"`
	Offset     int    `json:"offset"`
}

type storageChildrenPage struct {
	items      []record.Aggregate
	hasMore    bool
	nextCursor string
}

func BuildStorageChildrenCursor(hash plumbing.Hash, gitSubpath string, sortBy string, sortOrder string, offset int) string {
	cursor := storageChildrenCursor{
		CommitHash: hash.String(),
		GitSubpath: record.NormalizeRepoSubpath(gitSubpath),
		SortBy:     strings.TrimSpace(sortBy),
		SortOrder:  strings.TrimSpace(sortOrder),
		Offset:     offset,
	}
	contentBytes, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(contentBytes)
}

func decodeStorageChildrenCursor(raw string) (storageChildrenCursor, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return storageChildrenCursor{}, nil
	}
	contentBytes, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil {
		return storageChildrenCursor{}, fmt.Errorf("decode storage children cursor: %w", err)
	}
	cursor := storageChildrenCursor{}
	if err := json.Unmarshal(contentBytes, &cursor); err != nil {
		return storageChildrenCursor{}, fmt.Errorf("decode storage children cursor JSON: %w", err)
	}
	if cursor.Offset < 0 {
		return storageChildrenCursor{}, fmt.Errorf("storage children cursor offset must be non-negative")
	}
	return cursor, nil
}

func storageChildrenPageForRequest(items []record.Aggregate, hash plumbing.Hash, gitSubpath string, sortBy string, sortOrder string, limit int, rawCursor string) (storageChildrenPage, error) {
	if limit <= 0 {
		limit = len(items)
	}
	offset, err := storageChildrenCursorOffset(hash, gitSubpath, sortBy, sortOrder, rawCursor)
	if err != nil {
		return storageChildrenPage{}, err
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	pageItems := append([]record.Aggregate(nil), items[offset:end]...)
	page := storageChildrenPage{
		items:   pageItems,
		hasMore: end < len(items),
	}
	if page.hasMore {
		page.nextCursor = BuildStorageChildrenCursor(hash, gitSubpath, sortBy, sortOrder, end)
	}
	return page, nil
}

func storageChildrenCursorOffset(hash plumbing.Hash, gitSubpath string, sortBy string, sortOrder string, rawCursor string) (int, error) {
	cursor, err := decodeStorageChildrenCursor(rawCursor)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(rawCursor) == "" {
		return 0, nil
	}
	expected := storageChildrenCursor{
		CommitHash: hash.String(),
		GitSubpath: record.NormalizeRepoSubpath(gitSubpath),
		SortBy:     strings.TrimSpace(sortBy),
		SortOrder:  strings.TrimSpace(sortOrder),
	}
	if cursor.CommitHash != expected.CommitHash || cursor.GitSubpath != expected.GitSubpath || cursor.SortBy != expected.SortBy || cursor.SortOrder != expected.SortOrder {
		return 0, fmt.Errorf("storage children cursor does not match current request")
	}
	return cursor.Offset, nil
}

func storageChildrenResponseForServingIndex(directory repoindex.ServingIndex, hash plumbing.Hash, gitSubpath string, sortBy string, sortOrder string, limit int, rawCursor string) (model.StorageChildrenResponse, error) {
	order := sortedOrderForStorageChildrenRequest(directory, sortBy, sortOrder)
	if limit <= 0 {
		limit = len(order)
	}
	offset, err := storageChildrenCursorOffset(hash, gitSubpath, sortBy, sortOrder, rawCursor)
	if err != nil {
		return model.StorageChildrenResponse{}, err
	}
	if offset > len(order) {
		offset = len(order)
	}
	end := offset + limit
	if end > len(order) {
		end = len(order)
	}
	items := make([]model.StorageChildResponseItem, 0, end-offset)
	for _, childIndex := range order[offset:end] {
		child := directory.Directory.Children[childIndex]
		items = append(items, model.StorageChildResponseItem{
			Name:       child.Name,
			Path:       child.Path,
			Type:       child.Type,
			FileCount:  child.FileCount,
			TotalBytes: child.TotalBytes,
		})
	}
	response := model.StorageChildrenResponse{
		Items:   items,
		HasMore: end < len(order),
	}
	if response.HasMore {
		response.NextCursor = BuildStorageChildrenCursor(hash, gitSubpath, sortBy, sortOrder, end)
	}
	return response, nil
}

func sortedOrderForStorageChildrenRequest(directory repoindex.ServingIndex, sortBy string, sortOrder string) []int {
	desc := !strings.EqualFold(strings.TrimSpace(sortOrder), "asc")
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		if desc {
			return directory.NameDesc
		}
		return directory.NameAsc
	default:
		if desc {
			return directory.BytesDesc
		}
		return directory.BytesAsc
	}
}

func filterInventoryForStorageChildren(inventory []model.RepoInventoryFile, children []record.Aggregate) []model.RepoInventoryFile {
	if len(children) == 0 || len(inventory) == 0 {
		return nil
	}
	selectedFiles := make(map[string]struct{})
	selectedDirectories := make([]string, 0)
	for _, child := range children {
		childPath := record.NormalizeRepoSubpath(child.Path)
		if childPath == "" {
			continue
		}
		if child.RowType == "file" {
			selectedFiles[childPath] = struct{}{}
			continue
		}
		selectedDirectories = append(selectedDirectories, childPath+"/")
	}
	filtered := make([]model.RepoInventoryFile, 0)
	for _, item := range inventory {
		itemPath := record.NormalizeRepoSubpath(item.RepoPath)
		if _, ok := selectedFiles[itemPath]; ok {
			filtered = append(filtered, item)
			continue
		}
		for _, prefix := range selectedDirectories {
			if strings.HasPrefix(itemPath, prefix) {
				filtered = append(filtered, item)
				break
			}
		}
	}
	return filtered
}

func (service *Service) enrichStorageChildrenPage(ctx context.Context, authorizationHeader string, organization string, project string, gitSubpath string, inventory []model.RepoInventoryFile, pageItems []record.Aggregate) ([]record.Aggregate, error) {
	checksums := uniqueInventoryChecksums(inventory)
	if len(checksums) == 0 {
		return pageItems, nil
	}
	recordsByChecksumRaw, err := service.store.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, checksums)
	if err != nil {
		return nil, fmt.Errorf("lookup syfon project records by checksum: %w", err)
	}
	recordsByChecksum := make(map[string][]record.State, len(recordsByChecksumRaw))
	for _, records := range recordsByChecksumRaw {
		for _, rec := range records {
			normalizedChecksum := record.NormalizeAnalyticsChecksum(rec.Checksum)
			if normalizedChecksum == "" {
				continue
			}
			rec.Checksum = normalizedChecksum
			recordsByChecksum[normalizedChecksum] = append(recordsByChecksum[normalizedChecksum], record.State{
				ProjectRecord: rec,
			})
		}
	}
	return aggregateImmediateChildren(gitSubpath, inventory, recordsByChecksum, nil, pageItems), nil
}

func uniqueInventoryChecksums(inventory []model.RepoInventoryFile) []string {
	checksums := make([]string, 0, len(inventory))
	seen := make(map[string]struct{}, len(inventory))
	for _, item := range inventory {
		checksum := record.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" {
			continue
		}
		if _, ok := seen[checksum]; ok {
			continue
		}
		seen[checksum] = struct{}{}
		checksums = append(checksums, checksum)
	}
	return checksums
}

func storageChildrenItemsFromAggregates(aggregates []record.Aggregate) []model.StorageChildResponseItem {
	items := make([]model.StorageChildResponseItem, 0, len(aggregates))
	for _, agg := range aggregates {
		items = append(items, model.StorageChildResponseItem{
			Name:             agg.Name,
			Path:             agg.Path,
			Type:             agg.RowType,
			FileCount:        agg.FileCount,
			RecordCount:      agg.RecordCount,
			TotalBytes:       agg.TotalBytes,
			DownloadCount:    agg.DownloadCount,
			LastDownloadTime: record.FormatOptionalTime(agg.LastDownload),
			LatestUpdateTime: record.FormatOptionalTime(agg.LatestUpdate),
		})
	}
	return items
}

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
