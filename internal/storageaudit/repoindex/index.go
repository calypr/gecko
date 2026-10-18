package repoindex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

const SchemaVersion = 1

var Cache = &MemoryCache{
	Entries:  map[string]*Index{},
	Inflight: map[string]*Inflight{},
}

type MemoryCache struct {
	Mu       sync.RWMutex
	Entries  map[string]*Index
	Inflight map[string]*Inflight
}

type Inflight struct {
	Done  chan struct{}
	Index *Index
	Err   error
}

type Index struct {
	Sidecar          model.RepoAnalyticsIndexSidecar
	DirectoryLookup  map[string]model.RepoAnalyticsDirectory
	DirectoryServing map[string]ServingIndex
}

type ServingIndex struct {
	Directory model.RepoAnalyticsDirectory
	BytesAsc  []int
	BytesDesc []int
	NameAsc   []int
	NameDesc  []int
}

func RepoAnalyticsCacheKey(mirrorPath string, hash plumbing.Hash) string {
	return strings.TrimSpace(mirrorPath) + "::" + hash.String()
}

func (cache *MemoryCache) Put(mirrorPath string, hash plumbing.Hash, index *Index) {
	if index == nil {
		return
	}
	cache.Mu.Lock()
	defer cache.Mu.Unlock()
	prefix := strings.TrimSpace(mirrorPath) + "::"
	for key := range cache.Entries {
		if strings.HasPrefix(key, prefix) && key != RepoAnalyticsCacheKey(mirrorPath, hash) {
			delete(cache.Entries, key)
		}
	}
	cache.Entries[RepoAnalyticsCacheKey(mirrorPath, hash)] = index
}

func RepoAnalyticsIndexPath(mirrorPath string) string {
	return mirrorPath + ".analytics.Index.json"
}

func PersistRepoAnalyticsIndex(ctx context.Context, mirrorPath string, repo *gogit.Repository, ref string, hash plumbing.Hash) error {
	index, err := BuildRepoAnalyticsIndex(ref, repo, hash)
	if err != nil {
		return err
	}
	if err := WriteRepoAnalyticsIndexSidecar(mirrorPath, index.Sidecar); err != nil {
		return err
	}
	Cache.Put(mirrorPath, hash, index)
	_ = ctx
	return nil
}

func LoadOrBuildRepoAnalyticsIndex(ctx context.Context, mirrorPath string, ref string, repo *gogit.Repository, hash plumbing.Hash) (*Index, error) {
	return LoadOrBuildRepoAnalyticsIndexWithTimings(ctx, mirrorPath, ref, repo, hash, nil)
}

func LoadOrBuildRepoAnalyticsIndexWithTimings(_ context.Context, mirrorPath string, ref string, repo *gogit.Repository, hash plumbing.Hash, timings *model.StorageFolderTimings) (*Index, error) {
	cacheKey := RepoAnalyticsCacheKey(mirrorPath, hash)
	Cache.Mu.Lock()
	if cached := Cache.Entries[cacheKey]; cached != nil {
		Cache.Mu.Unlock()
		timings.Record("repo_index_memory_cache_hit", 0)
		return cached, nil
	}
	if inflight := Cache.Inflight[cacheKey]; inflight != nil {
		Cache.Mu.Unlock()
		waitStart := time.Now()
		<-inflight.Done
		timings.Record("repo_index_inflight_wait", time.Since(waitStart))
		return inflight.Index, inflight.Err
	}
	inflight := &Inflight{Done: make(chan struct{})}
	Cache.Inflight[cacheKey] = inflight
	Cache.Mu.Unlock()
	defer func() {
		Cache.Mu.Lock()
		delete(Cache.Inflight, cacheKey)
		close(inflight.Done)
		Cache.Mu.Unlock()
	}()

	sidecarStart := time.Now()
	sidecar, err := ReadRepoAnalyticsIndexSidecar(mirrorPath)
	timings.Record("repo_index_sidecar_read_decode", time.Since(sidecarStart))
	if err == nil && sidecar.SchemaVersion == SchemaVersion && sidecar.CommitHash == hash.String() {
		servingStart := time.Now()
		index := RepoAnalyticsIndexFromSidecar(sidecar)
		timings.Record("repo_index_sorted_order_build", time.Since(servingStart))
		Cache.Put(mirrorPath, hash, index)
		inflight.Index = index
		return index, nil
	}
	buildStart := time.Now()
	index, buildErr := BuildRepoAnalyticsIndex(ref, repo, hash)
	timings.Record("repo_index_git_tree_rebuild", time.Since(buildStart))
	if buildErr != nil {
		inflight.Err = buildErr
		return nil, buildErr
	}
	writeStart := time.Now()
	if writeErr := WriteRepoAnalyticsIndexSidecar(mirrorPath, index.Sidecar); writeErr != nil {
		timings.Record("repo_index_sidecar_write", time.Since(writeStart))
		inflight.Err = writeErr
		return nil, writeErr
	}
	timings.Record("repo_index_sidecar_write", time.Since(writeStart))
	Cache.Put(mirrorPath, hash, index)
	inflight.Index = index
	return index, nil
}

func ReadRepoAnalyticsIndexSidecar(mirrorPath string) (model.RepoAnalyticsIndexSidecar, error) {
	sidecarPath := RepoAnalyticsIndexPath(mirrorPath)
	contentBytes, err := os.ReadFile(sidecarPath)
	if err != nil {
		return model.RepoAnalyticsIndexSidecar{}, fmt.Errorf("read repo analytics index Sidecar: %w", err)
	}
	sidecar := model.RepoAnalyticsIndexSidecar{}
	if err := json.Unmarshal(contentBytes, &sidecar); err != nil {
		return model.RepoAnalyticsIndexSidecar{}, fmt.Errorf("decode repo analytics index Sidecar: %w", err)
	}
	return sidecar, nil
}

func WriteRepoAnalyticsIndexSidecar(mirrorPath string, sidecar model.RepoAnalyticsIndexSidecar) error {
	sidecarPath := RepoAnalyticsIndexPath(mirrorPath)
	if err := os.MkdirAll(filepath.Dir(sidecarPath), 0o755); err != nil {
		return fmt.Errorf("create repo analytics sidecar dir: %w", err)
	}
	contentBytes, err := json.Marshal(sidecar)
	if err != nil {
		return fmt.Errorf("encode repo analytics index Sidecar: %w", err)
	}
	tempPath := sidecarPath + ".tmp"
	if err := os.WriteFile(tempPath, contentBytes, 0o644); err != nil {
		return fmt.Errorf("write repo analytics index temp Sidecar: %w", err)
	}
	if err := os.Rename(tempPath, sidecarPath); err != nil {
		return fmt.Errorf("move repo analytics index sidecar into place: %w", err)
	}
	return nil
}

func RepoAnalyticsIndexFromSidecar(sidecar model.RepoAnalyticsIndexSidecar) *Index {
	directoryLookup := make(map[string]model.RepoAnalyticsDirectory, len(sidecar.Directories))
	directoryServing := make(map[string]ServingIndex, len(sidecar.Directories))
	for _, directory := range sidecar.Directories {
		directoryLookup[directory.Path] = directory
		directoryServing[directory.Path] = BuildRepoAnalyticsDirectoryServingIndex(directory)
	}
	return &Index{
		Sidecar:          sidecar,
		DirectoryLookup:  directoryLookup,
		DirectoryServing: directoryServing,
	}
}

func BuildRepoAnalyticsDirectoryServingIndex(directory model.RepoAnalyticsDirectory) ServingIndex {
	return ServingIndex{
		Directory: directory,
		BytesAsc:  SortedChildIndexes(directory.Children, "bytes", "asc"),
		BytesDesc: SortedChildIndexes(directory.Children, "bytes", "desc"),
		NameAsc:   SortedChildIndexes(directory.Children, "name", "asc"),
		NameDesc:  SortedChildIndexes(directory.Children, "name", "desc"),
	}
}

func SortedChildIndexes(children []model.RepoAnalyticsChild, sortBy string, sortOrder string) []int {
	indexes := make([]int, len(children))
	for index := range children {
		indexes[index] = index
	}
	desc := !strings.EqualFold(strings.TrimSpace(sortOrder), "asc")
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		sort.Slice(indexes, func(i, j int) bool {
			left := strings.ToLower(children[indexes[i]].Name)
			right := strings.ToLower(children[indexes[j]].Name)
			if desc {
				return left > right
			}
			return left < right
		})
	default:
		sort.Slice(indexes, func(i, j int) bool {
			left := children[indexes[i]]
			right := children[indexes[j]]
			if left.TotalBytes != right.TotalBytes {
				if desc {
					return left.TotalBytes > right.TotalBytes
				}
				return left.TotalBytes < right.TotalBytes
			}
			if left.Type != right.Type {
				return left.Type == "directory"
			}
			return strings.ToLower(left.Name) < strings.ToLower(right.Name)
		})
	}
	return indexes
}

func BuildRepoAnalyticsIndex(ref string, repo *gogit.Repository, hash plumbing.Hash) (*Index, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load commit for ref %s: %w", ref, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("load git tree for ref %s: %w", ref, err)
	}
	files := make([]model.RepoInventoryFile, 0)
	knownDirectories := map[string]struct{}{"": {}}
	if err := WalkGitRepoInventory("", tree, &files, knownDirectories); err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].RepoPath < files[j].RepoPath
	})
	directories := BuildRepoAnalyticsDirectories(files, knownDirectories)
	sidecar := model.RepoAnalyticsIndexSidecar{
		SchemaVersion: SchemaVersion,
		CommitHash:    hash.String(),
		RefName:       ref,
		GeneratedAt:   time.Now().UTC(),
		Files:         files,
		Directories:   directories,
	}
	return RepoAnalyticsIndexFromSidecar(sidecar), nil
}

func BuildRepoAnalyticsDirectories(files []model.RepoInventoryFile, knownDirectories map[string]struct{}) []model.RepoAnalyticsDirectory {
	type directoryBuilder struct {
		model.RepoAnalyticsDirectory
		children map[string]*model.RepoAnalyticsChild
	}
	builders := make(map[string]*directoryBuilder, len(knownDirectories))
	ensureDirectory := func(path string) *directoryBuilder {
		builder := builders[path]
		if builder != nil {
			return builder
		}
		builder = &directoryBuilder{
			RepoAnalyticsDirectory: model.RepoAnalyticsDirectory{Path: path},
			children:               map[string]*model.RepoAnalyticsChild{},
		}
		builders[path] = builder
		return builder
	}
	for path := range knownDirectories {
		ensureDirectory(path)
	}
	for _, file := range files {
		parentPath := ""
		parts := strings.Split(file.RepoPath, "/")
		for index, part := range parts {
			directory := ensureDirectory(parentPath)
			directory.FileCount++
			directory.TotalBytes += file.Size
			childPath := part
			if parentPath != "" {
				childPath = parentPath + "/" + part
			}
			childType := "file"
			if index < len(parts)-1 {
				childType = "directory"
			}
			child := directory.children[childPath]
			if child == nil {
				child = &model.RepoAnalyticsChild{
					Name: part,
					Path: childPath,
					Type: childType,
				}
				directory.children[childPath] = child
			}
			child.FileCount++
			child.TotalBytes += file.Size
			if childType == "directory" {
				parentPath = childPath
				ensureDirectory(parentPath)
			}
		}
	}
	paths := make([]string, 0, len(builders))
	for path := range builders {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	directories := make([]model.RepoAnalyticsDirectory, 0, len(paths))
	for _, path := range paths {
		builder := builders[path]
		childPaths := make([]string, 0, len(builder.children))
		for childPath := range builder.children {
			childPaths = append(childPaths, childPath)
		}
		sort.Strings(childPaths)
		builder.Children = make([]model.RepoAnalyticsChild, 0, len(childPaths))
		for _, childPath := range childPaths {
			builder.Children = append(builder.Children, *builder.children[childPath])
		}
		builder.DirectChildCount = len(builder.Children)
		directories = append(directories, builder.RepoAnalyticsDirectory)
	}
	return directories
}

func FilterRepoInventoryFiles(index *Index, gitSubpath string) ([]model.RepoInventoryFile, error) {
	normalizedPath := record.NormalizeRepoSubpath(gitSubpath)
	if _, ok := index.DirectoryLookup[normalizedPath]; !ok {
		return nil, fmt.Errorf("load git tree path %s: directory not found", normalizedPath)
	}
	if normalizedPath == "" {
		return append([]model.RepoInventoryFile(nil), index.Sidecar.Files...), nil
	}
	prefix := normalizedPath + "/"
	filtered := make([]model.RepoInventoryFile, 0)
	for _, item := range index.Sidecar.Files {
		if strings.HasPrefix(item.RepoPath, prefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func RepoDirectoryAggregate(index *Index, gitSubpath string) (model.RepoAnalyticsDirectory, error) {
	normalizedPath := record.NormalizeRepoSubpath(gitSubpath)
	directory, ok := index.DirectoryLookup[normalizedPath]
	if !ok {
		return model.RepoAnalyticsDirectory{}, fmt.Errorf("load git tree path %s: directory not found", normalizedPath)
	}
	return directory, nil
}

func RepoDirectoryServingIndex(index *Index, gitSubpath string) (ServingIndex, error) {
	normalizedPath := record.NormalizeRepoSubpath(gitSubpath)
	directory, ok := index.DirectoryServing[normalizedPath]
	if !ok {
		return ServingIndex{}, fmt.Errorf("load git tree path %s: directory not found", normalizedPath)
	}
	return directory, nil
}

func WalkGitRepoInventory(prefix string, tree *object.Tree, files *[]model.RepoInventoryFile, knownDirectories map[string]struct{}) error {
	for _, entry := range tree.Entries {
		entryPath := entry.Name
		if prefix != "" {
			entryPath = prefix + "/" + entry.Name
		}
		if entry.Mode == filemode.Dir {
			knownDirectories[entryPath] = struct{}{}
			childTree, err := tree.Tree(entry.Name)
			if err != nil {
				return fmt.Errorf("load nested git tree %s: %w", entryPath, err)
			}
			if err := WalkGitRepoInventory(entryPath, childTree, files, knownDirectories); err != nil {
				return err
			}
			continue
		}
		file, err := tree.File(entry.Name)
		if err != nil {
			continue
		}
		reader, err := file.Reader()
		if err != nil {
			continue
		}
		contentBytes, readErr := io.ReadAll(io.LimitReader(reader, 2048))
		_ = reader.Close()
		if readErr != nil {
			continue
		}
		pointer := repo.ParseLFSPointer(contentBytes)
		if pointer == nil {
			continue
		}
		*files = append(*files, model.RepoInventoryFile{
			RepoPath: entryPath,
			Name:     entry.Name,
			Checksum: pointer.OID,
			Size:     pointer.Size,
		})
	}
	return nil
}
