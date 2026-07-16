package git

import (
	"io"
	"sort"
	"strings"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func enrichTreeEntries(tree *object.Tree, entries []GitTreeEntry, options GitTreeResponseOptions) {
	if !options.IncludeSize && !options.IncludeLFSPointer {
		return
	}
	for index := range entries {
		entry := &entries[index]
		if entry.Type != "blob" {
			continue
		}

		file, err := tree.File(entry.Name)
		if err != nil {
			continue
		}
		if options.IncludeSize {
			entry.Size = file.Size
		}
		if options.IncludeLFSPointer {
			if reader, err := file.Reader(); err == nil {
				contentBytes, readErr := io.ReadAll(io.LimitReader(reader, 2048))
				_ = reader.Close()
				if readErr == nil {
					entry.LFSPointer = ParseGitLFSPointer(contentBytes)
				}
			}
		}
	}
}

func lookupGitPathsLastModified(repo *gogit.Repository, from plumbing.Hash, entries []GitTreeEntry) (map[string]time.Time, error) {
	targets := buildGitTreeLastModifiedTargets(entries)
	if targets.remaining == 0 {
		return map[string]time.Time{}, nil
	}

	iter, err := repo.Log(&gogit.LogOptions{
		From:  from,
		Order: gogit.LogOrderCommitterTime,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for targets.remaining > 0 {
		commit, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return targets.found, nil
			}
			return nil, err
		}
		modifiedAt := commit.Committer.When.UTC()
		if commit.NumParents() == 0 {
			targets.markAll(modifiedAt)
			return targets.found, nil
		}
		changedPaths, err := commitChangedPaths(commit)
		if err != nil {
			return nil, err
		}
		for _, changedPath := range changedPaths {
			targets.mark(changedPath, modifiedAt)
			if targets.remaining == 0 {
				return targets.found, nil
			}
		}
	}
	return targets.found, nil
}

type gitTreeLastModifiedTargets struct {
	exact     map[string]string
	dirs      []string
	found     map[string]time.Time
	remaining int
}

func buildGitTreeLastModifiedTargets(entries []GitTreeEntry) *gitTreeLastModifiedTargets {
	targets := &gitTreeLastModifiedTargets{
		exact: make(map[string]string, len(entries)),
		found: make(map[string]time.Time, len(entries)),
	}
	for _, entry := range entries {
		normalizedPath := strings.Trim(strings.TrimSpace(entry.Path), "/")
		if normalizedPath == "" {
			continue
		}
		targets.exact[normalizedPath] = normalizedPath
		if entry.Type == "tree" {
			targets.dirs = append(targets.dirs, normalizedPath)
		}
		targets.remaining++
	}
	sort.Slice(targets.dirs, func(i, j int) bool {
		return len(targets.dirs[i]) > len(targets.dirs[j])
	})
	return targets
}

func (targets *gitTreeLastModifiedTargets) mark(changedPath string, modifiedAt time.Time) {
	normalizedPath := strings.Trim(strings.TrimSpace(changedPath), "/")
	if normalizedPath == "" {
		return
	}
	if targetPath, ok := targets.exact[normalizedPath]; ok {
		targets.setFound(targetPath, modifiedAt)
	}
	for _, dirPath := range targets.dirs {
		if normalizedPath == dirPath || strings.HasPrefix(normalizedPath, dirPath+"/") {
			targets.setFound(dirPath, modifiedAt)
		}
	}
}

func (targets *gitTreeLastModifiedTargets) setFound(path string, modifiedAt time.Time) {
	if _, exists := targets.found[path]; exists {
		return
	}
	targets.found[path] = modifiedAt
	targets.remaining--
}

func (targets *gitTreeLastModifiedTargets) markAll(modifiedAt time.Time) {
	for targetPath := range targets.exact {
		targets.setFound(targetPath, modifiedAt)
	}
}

func commitChangedPaths(commit *object.Commit) ([]string, error) {
	if commit.NumParents() == 0 {
		return nil, nil
	}
	parent, err := commit.Parent(0)
	if err != nil {
		return nil, err
	}
	parentTree, err := parent.Tree()
	if err != nil {
		return nil, err
	}
	commitTree, err := commit.Tree()
	if err != nil {
		return nil, err
	}
	changes, err := parentTree.Diff(commitTree)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(changes))
	for _, change := range changes {
		if change.From.Name != "" {
			paths = append(paths, change.From.Name)
		}
		if change.To.Name != "" && change.To.Name != change.From.Name {
			paths = append(paths, change.To.Name)
		}
	}
	return paths, nil
}
