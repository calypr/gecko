package git

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/google/go-github/v87/github"
	servermw "github.com/calypr/gecko/internal/server/middleware"
)

func BuildGitTreeResponse(projectID string, ref string, path string, repo *gogit.Repository, hash plumbing.Hash) (*GitProjectTreeResponse, error) {
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
	entries := make([]GitTreeEntry, 0, len(tree.Entries))
	for _, entry := range tree.Entries {
		entryPath := entry.Name
		if normalizedPath != "" {
			entryPath = normalizedPath + "/" + entry.Name
		}
		gitEntry := GitTreeEntry{Name: entry.Name, Path: entryPath, Hash: entry.Hash.String()}
		if entry.Mode == filemode.Dir {
			gitEntry.Type = "tree"
		} else {
			gitEntry.Type = "blob"
			if file, err := tree.File(entry.Name); err == nil {
				gitEntry.Size = file.Size
				if reader, err := file.Reader(); err == nil {
					contentBytes, readErr := io.ReadAll(io.LimitReader(reader, 2048))
					_ = reader.Close()
					if readErr == nil {
						gitEntry.LFSPointer = ParseGitLFSPointer(contentBytes)
					}
				}
			}
		}
		if lastModifiedAt, err := lookupGitPathLastModified(repo, hash, entryPath); err == nil && lastModifiedAt != nil {
			gitEntry.LastModifiedAt = lastModifiedAt
		}
		entries = append(entries, gitEntry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Type != entries[j].Type {
			return entries[i].Type == "tree"
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	return &GitProjectTreeResponse{ProjectID: projectID, Ref: ref, Path: normalizedPath, Entries: entries}, nil
}

func BuildGitRefsResponse(projectID string, defaultBranch string, repo *gogit.Repository) (*GitProjectRefsResponse, error) {
	refs := make([]GitRef, 0)
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
			refs = append(refs, GitRef{Name: branchName, Type: "branch", Hash: reference.Hash().String(), Default: branchName == defaultBranch})
		case name.IsRemote() && strings.HasPrefix(name.String(), "refs/remotes/origin/"):
			branchName := strings.TrimPrefix(name.String(), "refs/remotes/origin/")
			if branchName == "HEAD" {
				return nil
			}
			if _, ok := seenBranches[branchName]; ok {
				return nil
			}
			seenBranches[branchName] = struct{}{}
			refs = append(refs, GitRef{Name: branchName, Type: "branch", Hash: reference.Hash().String(), Default: branchName == defaultBranch})
		case name.IsTag():
			tagName := name.Short()
			if _, ok := seenTags[tagName]; ok {
				return nil
			}
			seenTags[tagName] = struct{}{}
			refs = append(refs, GitRef{Name: tagName, Type: "tag", Hash: reference.Hash().String()})
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
	return &GitProjectRefsResponse{ProjectID: projectID, DefaultBranch: defaultBranch, Refs: refs}, nil
}

func BuildGitFileResponse(projectID string, ref string, path string, repo *gogit.Repository, hash plumbing.Hash) (*GitProjectFileResponse, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load commit for ref %s: %w", ref, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("load git tree for ref %s: %w", ref, err)
	}
	normalizedPath := strings.Trim(strings.TrimSpace(path), "/")
	if normalizedPath == "" {
		return nil, fmt.Errorf("file path is required")
	}
	file, err := tree.File(normalizedPath)
	if err != nil {
		return nil, fmt.Errorf("load git file %s: %w", normalizedPath, err)
	}
	reader, err := file.Reader()
	if err != nil {
		return nil, fmt.Errorf("open git file %s: %w", normalizedPath, err)
	}
	defer reader.Close()
	const inlineLimit = 256 * 1024
	contentBytes, err := io.ReadAll(io.LimitReader(reader, inlineLimit+1))
	if err != nil {
		return nil, fmt.Errorf("read git file content for %s: %w", normalizedPath, err)
	}
	if len(contentBytes) > inlineLimit {
		contentBytes = contentBytes[:inlineLimit]
	}
	return &GitProjectFileResponse{
		ProjectID:  projectID,
		Ref:        ref,
		Path:       normalizedPath,
		Name:       filepath.Base(normalizedPath),
		Hash:       file.Hash.String(),
		Size:       file.Size,
		LFSPointer: ParseGitLFSPointer(contentBytes),
	}, nil
}

func BuildGitHubFileResponse(projectID string, ref string, path string, metadata *github.RepositoryContent, contentBytes []byte) *GitProjectFileResponse {
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
	return &GitProjectFileResponse{
		ProjectID:   projectID,
		Ref:         ref,
		Path:        strings.Trim(strings.TrimSpace(path), "/"),
		Name:        name,
		Hash:        hash,
		Size:        size,
		HTMLURL:     htmlURL,
		DownloadURL: downloadURL,
		LFSPointer:  ParseGitLFSPointer(contentBytes),
	}
}

func (service *GitService) GetGitHubFileMetadata(ctx context.Context, authorizationHeader string, organization string, project string, identity GitRepositoryIdentity, ref string, path string) (*github.RepositoryContent, []byte, error) {
	authorizationHeader, err := servermw.ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return nil, nil, &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, "read")
	if err != nil {
		return nil, nil, err
	}
	client, err := service.githubClient(accessToken)
	if err != nil {
		return nil, nil, err
	}
	opts := &github.RepositoryContentGetOptions{}
	if strings.TrimSpace(ref) != "" {
		opts.Ref = strings.TrimSpace(ref)
	}
	metadata, _, response, err := client.Repositories.GetContents(ctx, identity.Owner, identity.Repo, path, opts)
	if err != nil {
		statusCode := http.StatusBadGateway
		if response != nil && response.StatusCode > 0 {
			statusCode = response.StatusCode
		}
		if statusCode == http.StatusBadGateway && strings.Contains(strings.ToLower(err.Error()), "not found") {
			statusCode = http.StatusNotFound
		}
		return nil, nil, &HTTPStatusError{
			StatusCode: statusCode,
			Code:       "integration_error",
			Message:    fmt.Sprintf("GitHub file lookup failed: %s", err),
		}
	}
	if response != nil && response.Response != nil && response.StatusCode >= http.StatusBadRequest {
		return metadata, nil, &HTTPStatusError{
			StatusCode: response.StatusCode,
			Code:       "integration_error",
			Message:    fmt.Sprintf("GitHub file lookup failed with status %d", response.StatusCode),
		}
	}
	if metadata == nil {
		return nil, nil, &HTTPStatusError{
			StatusCode: http.StatusNotFound,
			Code:       "not_found",
			Message:    fmt.Sprintf("GitHub file %s was not found", path),
		}
	}
	contentString, err := metadata.GetContent()
	if err != nil || contentString == "" {
		return metadata, nil, nil
	}
	return metadata, []byte(contentString), nil
}
