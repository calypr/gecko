package repo

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/go-github/v87/github"
)

var ErrInvalidManifestCursor = errors.New("invalid manifest cursor")

func BuildTreeResponse(projectID string, ref string, path string, repo *gogit.Repository, hash plumbing.Hash, options TreeResponseOptions) (*TreeResponse, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load commit for ref %s: %w", ref, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("load git tree for ref %s: %w", ref, err)
	}
	normalizedPath := strings.Trim(strings.TrimSpace(path), "/")
	if normalizedPath != "" {
		tree, err = tree.Tree(normalizedPath)
		if err != nil {
			return nil, fmt.Errorf("load git tree path %s: %w", normalizedPath, err)
		}
	}
	entries := make([]TreeEntry, 0, len(tree.Entries))
	for _, entry := range tree.Entries {
		entryPath := entry.Name
		if normalizedPath != "" {
			entryPath = normalizedPath + "/" + entry.Name
		}
		gitEntry := TreeEntry{Name: entry.Name, Path: entryPath, Hash: entry.Hash.String()}
		if entry.Mode == filemode.Dir {
			gitEntry.Type = "tree"
		} else {
			gitEntry.Type = "blob"
		}
		entries = append(entries, gitEntry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Type != entries[j].Type {
			return entries[i].Type == "tree"
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})

	entryCount := len(entries)
	truncated := false
	if options.Limit > 0 && len(entries) > options.Limit {
		entries = entries[:options.Limit]
		truncated = true
	}

	enrichTreeEntries(tree, entries, options)
	if options.IncludeLastModified {
		lastModifiedByPath, err := LookupPathsLastModified(repo, hash, entries)
		if err != nil {
			return nil, fmt.Errorf("lookup git tree last modified times: %w", err)
		}
		for index := range entries {
			if lastModifiedAt, ok := lastModifiedByPath[entries[index].Path]; ok {
				entries[index].LastModifiedAt = &lastModifiedAt
			}
		}
	}

	return &TreeResponse{
		ProjectID:  projectID,
		Ref:        ref,
		Path:       normalizedPath,
		EntryCount: entryCount,
		Truncated:  truncated,
		Entries:    entries,
	}, nil
}

type gitManifestCursor struct {
	Ref    string `json:"ref"`
	Path   string `json:"path"`
	Offset int    `json:"offset"`
}

func BuildManifestResponse(projectID string, ref string, path string, repo *gogit.Repository, hash plumbing.Hash, options ManifestResponseOptions) (*ManifestResponse, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load commit for ref %s: %w", ref, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("load git tree for ref %s: %w", ref, err)
	}
	normalizedPath := strings.Trim(strings.TrimSpace(path), "/")
	if normalizedPath != "" {
		tree, err = tree.Tree(normalizedPath)
		if err != nil {
			return nil, fmt.Errorf("load git tree path %s: %w", normalizedPath, err)
		}
	}

	offset, err := parseGitManifestCursor(options.Cursor, ref, normalizedPath)
	if err != nil {
		return nil, err
	}

	entries := make([]TreeEntry, 0, len(tree.Entries))
	if err := walkGitManifestTree(normalizedPath, tree, options.FilesOnly, &entries); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if !options.FilesOnly && entries[i].Type != entries[j].Type {
			return entries[i].Type == "tree"
		}
		return entries[i].Path < entries[j].Path
	})
	if offset > len(entries) {
		return nil, ErrInvalidManifestCursor
	}

	end := len(entries)
	if options.Limit > 0 && offset+options.Limit < end {
		end = offset + options.Limit
	}
	pageEntries := append([]TreeEntry(nil), entries[offset:end]...)
	hasMore := end < len(entries)
	nextCursor := ""
	if hasMore {
		nextCursor, err = encodeGitManifestCursor(ref, normalizedPath, end)
		if err != nil {
			return nil, err
		}
	}

	return &ManifestResponse{
		ProjectID:  projectID,
		Ref:        ref,
		Path:       normalizedPath,
		EntryCount: len(pageEntries),
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Entries:    pageEntries,
	}, nil
}

func walkGitManifestTree(prefix string, tree *object.Tree, filesOnly bool, entries *[]TreeEntry) error {
	for _, entry := range tree.Entries {
		entryPath := entry.Name
		if prefix != "" {
			entryPath = prefix + "/" + entry.Name
		}
		entryType := "blob"
		if entry.Mode == filemode.Dir {
			entryType = "tree"
		}
		if !filesOnly || entryType == "blob" {
			*entries = append(*entries, TreeEntry{
				Name: entry.Name,
				Path: entryPath,
				Type: entryType,
				Hash: entry.Hash.String(),
			})
		}
		if entryType == "tree" {
			childTree, err := tree.Tree(entry.Name)
			if err != nil {
				return fmt.Errorf("load nested git tree %s: %w", entryPath, err)
			}
			if err := walkGitManifestTree(entryPath, childTree, filesOnly, entries); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseGitManifestCursor(raw string, ref string, path string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return 0, ErrInvalidManifestCursor
	}
	var payload gitManifestCursor
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return 0, ErrInvalidManifestCursor
	}
	if payload.Ref != ref || payload.Path != path || payload.Offset < 0 {
		return 0, ErrInvalidManifestCursor
	}
	return payload.Offset, nil
}

func encodeGitManifestCursor(ref string, path string, offset int) (string, error) {
	payload, err := json.Marshal(gitManifestCursor{
		Ref:    ref,
		Path:   path,
		Offset: offset,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func BuildRefsResponse(projectID string, defaultBranch string, repo *gogit.Repository) (*RefsResponse, error) {
	refs := make([]Ref, 0)
	seenBranches := make(map[string]struct{})
	seenTags := make(map[string]struct{})
	iter, err := repo.References()
	if err != nil {
		return nil, fmt.Errorf("list git refs: %w", err)
	}
	err = iter.ForEach(func(reference *plumbing.Reference) error {
		name := reference.Name()
		switch {
		case name.IsBranch():
			branchName := name.Short()
			if _, ok := seenBranches[branchName]; ok {
				return nil
			}
			seenBranches[branchName] = struct{}{}
			refs = append(refs, Ref{Name: branchName, Type: "branch", Hash: reference.Hash().String(), Default: branchName == defaultBranch})
		case name.IsRemote() && strings.HasPrefix(name.String(), "refs/remotes/origin/"):
			branchName := strings.TrimPrefix(name.String(), "refs/remotes/origin/")
			if branchName == "HEAD" {
				return nil
			}
			if _, ok := seenBranches[branchName]; ok {
				return nil
			}
			seenBranches[branchName] = struct{}{}
			refs = append(refs, Ref{Name: branchName, Type: "branch", Hash: reference.Hash().String(), Default: branchName == defaultBranch})
		case name.IsTag():
			tagName := name.Short()
			if _, ok := seenTags[tagName]; ok {
				return nil
			}
			seenTags[tagName] = struct{}{}
			refs = append(refs, Ref{Name: tagName, Type: "tag", Hash: reference.Hash().String()})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterate git refs: %w", err)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Default != refs[j].Default {
			return refs[i].Default
		}
		if refs[i].Type != refs[j].Type {
			return refs[i].Type == "branch"
		}
		return strings.ToLower(refs[i].Name) < strings.ToLower(refs[j].Name)
	})
	return &RefsResponse{ProjectID: projectID, DefaultBranch: defaultBranch, Refs: refs}, nil
}

func BuildFileResponse(projectID string, ref string, path string, metadata *github.RepositoryContent, contentBytes []byte) *FileResponse {
	name := filepath.Base(strings.Trim(strings.TrimSpace(path), "/"))
	hash := ""
	size := int64(len(contentBytes))
	htmlURL := ""
	downloadURL := ""
	if metadata != nil {
		if metadata.GetName() != "" {
			name = metadata.GetName()
		}
		if metadata.GetSHA() != "" {
			hash = metadata.GetSHA()
		}
		if metadata.GetSize() > 0 {
			size = int64(metadata.GetSize())
		}
		htmlURL = metadata.GetHTMLURL()
		downloadURL = metadata.GetDownloadURL()
	}
	return &FileResponse{
		ProjectID:   projectID,
		Ref:         ref,
		Path:        strings.Trim(strings.TrimSpace(path), "/"),
		Name:        name,
		Hash:        hash,
		Size:        size,
		HTMLURL:     htmlURL,
		DownloadURL: downloadURL,
		LFSPointer:  ParseLFSPointer(contentBytes),
	}
}
