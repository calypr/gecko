package git

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

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

const repoAnalyticsIndexSchemaVersion = 1

var repoAnalyticsIndexCache = &repoAnalyticsIndexMemoryCache{
	entries: map[string]*repoAnalyticsIndex{},
}

type repoAnalyticsIndexMemoryCache struct {
	mu      sync.RWMutex
	entries map[string]*repoAnalyticsIndex
}

type repoAnalyticsIndex struct {
	sidecar         GitRepoAnalyticsIndexSidecar
	directoryLookup map[string]GitRepoAnalyticsDirectory
}

func repoAnalyticsCacheKey(mirrorPath string, hash plumbing.Hash) string {
	return strings.TrimSpace(mirrorPath) + "::" + hash.String()
}

func (cache *repoAnalyticsIndexMemoryCache) get(mirrorPath string, hash plumbing.Hash) *repoAnalyticsIndex {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	entry := cache.entries[repoAnalyticsCacheKey(mirrorPath, hash)]
	if entry == nil {
		return nil
	}
	return entry
}

func (cache *repoAnalyticsIndexMemoryCache) put(mirrorPath string, hash plumbing.Hash, index *repoAnalyticsIndex) {
	if index == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	prefix := strings.TrimSpace(mirrorPath) + "::"
	for key := range cache.entries {
		if strings.HasPrefix(key, prefix) && key != repoAnalyticsCacheKey(mirrorPath, hash) {
			delete(cache.entries, key)
		}
	}
	cache.entries[repoAnalyticsCacheKey(mirrorPath, hash)] = index
}

func RepoAnalyticsIndexPath(mirrorPath string) string {
	return mirrorPath + ".analytics.index.json"
}

func PersistRepoAnalyticsIndex(ctx context.Context, mirrorPath string, repo *gogit.Repository, ref string, hash plumbing.Hash) error {
	index, err := buildRepoAnalyticsIndex(ref, repo, hash)
	if err != nil {
		return err
	}
	if err := writeRepoAnalyticsIndexSidecar(mirrorPath, index.sidecar); err != nil {
		return err
	}
	repoAnalyticsIndexCache.put(mirrorPath, hash, index)
	_ = ctx
	return nil
}

func loadOrBuildRepoAnalyticsIndex(_ context.Context, mirrorPath string, ref string, repo *gogit.Repository, hash plumbing.Hash) (*repoAnalyticsIndex, error) {
	if cached := repoAnalyticsIndexCache.get(mirrorPath, hash); cached != nil {
		return cached, nil
	}
	sidecar, err := readRepoAnalyticsIndexSidecar(mirrorPath)
	if err == nil && sidecar.SchemaVersion == repoAnalyticsIndexSchemaVersion && sidecar.CommitHash == hash.String() {
		index := repoAnalyticsIndexFromSidecar(sidecar)
		repoAnalyticsIndexCache.put(mirrorPath, hash, index)
		return index, nil
	}
	index, buildErr := buildRepoAnalyticsIndex(ref, repo, hash)
	if buildErr != nil {
		return nil, buildErr
	}
	if writeErr := writeRepoAnalyticsIndexSidecar(mirrorPath, index.sidecar); writeErr != nil {
		return nil, writeErr
	}
	repoAnalyticsIndexCache.put(mirrorPath, hash, index)
	return index, nil
}

func readRepoAnalyticsIndexSidecar(mirrorPath string) (GitRepoAnalyticsIndexSidecar, error) {
	sidecarPath := RepoAnalyticsIndexPath(mirrorPath)
	contentBytes, err := os.ReadFile(sidecarPath)
	if err != nil {
		return GitRepoAnalyticsIndexSidecar{}, fmt.Errorf("read repo analytics index sidecar: %w", err)
	}
	sidecar := GitRepoAnalyticsIndexSidecar{}
	if err := json.Unmarshal(contentBytes, &sidecar); err != nil {
		return GitRepoAnalyticsIndexSidecar{}, fmt.Errorf("decode repo analytics index sidecar: %w", err)
	}
	return sidecar, nil
}

func writeRepoAnalyticsIndexSidecar(mirrorPath string, sidecar GitRepoAnalyticsIndexSidecar) error {
	sidecarPath := RepoAnalyticsIndexPath(mirrorPath)
	if err := os.MkdirAll(filepath.Dir(sidecarPath), 0o755); err != nil {
		return fmt.Errorf("create repo analytics sidecar dir: %w", err)
	}
	contentBytes, err := json.Marshal(sidecar)
	if err != nil {
		return fmt.Errorf("encode repo analytics index sidecar: %w", err)
	}
	tempPath := sidecarPath + ".tmp"
	if err := os.WriteFile(tempPath, contentBytes, 0o644); err != nil {
		return fmt.Errorf("write repo analytics index temp sidecar: %w", err)
	}
	if err := os.Rename(tempPath, sidecarPath); err != nil {
		return fmt.Errorf("move repo analytics index sidecar into place: %w", err)
	}
	return nil
}

func repoAnalyticsIndexFromSidecar(sidecar GitRepoAnalyticsIndexSidecar) *repoAnalyticsIndex {
	directoryLookup := make(map[string]GitRepoAnalyticsDirectory, len(sidecar.Directories))
	for _, directory := range sidecar.Directories {
		directoryLookup[directory.Path] = directory
	}
	return &repoAnalyticsIndex{
		sidecar:         sidecar,
		directoryLookup: directoryLookup,
	}
}

func buildRepoAnalyticsIndex(ref string, repo *gogit.Repository, hash plumbing.Hash) (*repoAnalyticsIndex, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load commit for ref %s: %w", ref, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("load git tree for ref %s: %w", ref, err)
	}
	files := make([]RepoInventoryFile, 0)
	knownDirectories := map[string]struct{}{"": {}}
	if err := walkGitRepoInventory("", tree, &files, knownDirectories); err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].RepoPath < files[j].RepoPath
	})
	directories := buildRepoAnalyticsDirectories(files, knownDirectories)
	sidecar := GitRepoAnalyticsIndexSidecar{
		SchemaVersion: repoAnalyticsIndexSchemaVersion,
		CommitHash:    hash.String(),
		RefName:       ref,
		GeneratedAt:   time.Now().UTC(),
		Files:         files,
		Directories:   directories,
	}
	return repoAnalyticsIndexFromSidecar(sidecar), nil
}

func buildRepoAnalyticsDirectories(files []RepoInventoryFile, knownDirectories map[string]struct{}) []GitRepoAnalyticsDirectory {
	type directoryBuilder struct {
		GitRepoAnalyticsDirectory
		children map[string]*GitRepoAnalyticsChild
	}
	builders := make(map[string]*directoryBuilder, len(knownDirectories))
	ensureDirectory := func(path string) *directoryBuilder {
		builder := builders[path]
		if builder != nil {
			return builder
		}
		builder = &directoryBuilder{
			GitRepoAnalyticsDirectory: GitRepoAnalyticsDirectory{Path: path},
			children:                  map[string]*GitRepoAnalyticsChild{},
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
				child = &GitRepoAnalyticsChild{
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
	directories := make([]GitRepoAnalyticsDirectory, 0, len(paths))
	for _, path := range paths {
		builder := builders[path]
		childPaths := make([]string, 0, len(builder.children))
		for childPath := range builder.children {
			childPaths = append(childPaths, childPath)
		}
		sort.Strings(childPaths)
		builder.Children = make([]GitRepoAnalyticsChild, 0, len(childPaths))
		for _, childPath := range childPaths {
			builder.Children = append(builder.Children, *builder.children[childPath])
		}
		builder.DirectChildCount = len(builder.Children)
		directories = append(directories, builder.GitRepoAnalyticsDirectory)
	}
	return directories
}

func filterRepoInventoryFiles(index *repoAnalyticsIndex, gitSubpath string) ([]RepoInventoryFile, error) {
	normalizedPath := normalizeRepoSubpath(gitSubpath)
	if _, ok := index.directoryLookup[normalizedPath]; !ok {
		return nil, fmt.Errorf("load git tree path %s: directory not found", normalizedPath)
	}
	if normalizedPath == "" {
		return append([]RepoInventoryFile(nil), index.sidecar.Files...), nil
	}
	prefix := normalizedPath + "/"
	filtered := make([]RepoInventoryFile, 0)
	for _, item := range index.sidecar.Files {
		if strings.HasPrefix(item.RepoPath, prefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func repoDirectoryAggregate(index *repoAnalyticsIndex, gitSubpath string) (GitRepoAnalyticsDirectory, error) {
	normalizedPath := normalizeRepoSubpath(gitSubpath)
	directory, ok := index.directoryLookup[normalizedPath]
	if !ok {
		return GitRepoAnalyticsDirectory{}, fmt.Errorf("load git tree path %s: directory not found", normalizedPath)
	}
	return directory, nil
}

func cloneDirectoryChildren(children []GitRepoAnalyticsChild) []storageAggregate {
	out := make([]storageAggregate, 0, len(children))
	for _, child := range children {
		out = append(out, storageAggregate{
			name:       child.Name,
			path:       child.Path,
			rowType:    child.Type,
			fileCount:  child.FileCount,
			totalBytes: child.TotalBytes,
		})
	}
	return out
}

func walkGitRepoInventory(prefix string, tree *object.Tree, files *[]RepoInventoryFile, knownDirectories map[string]struct{}) error {
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
			if err := walkGitRepoInventory(entryPath, childTree, files, knownDirectories); err != nil {
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
		pointer := ParseGitLFSPointer(contentBytes)
		if pointer == nil {
			continue
		}
		*files = append(*files, RepoInventoryFile{
			RepoPath: entryPath,
			Name:     entry.Name,
			Checksum: pointer.OID,
			Size:     pointer.Size,
		})
	}
	return nil
}
