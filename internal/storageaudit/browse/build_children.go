package browse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	"github.com/calypr/gecko/internal/storageaudit/repoindex"
	"github.com/go-git/go-git/v5/plumbing"
)

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
