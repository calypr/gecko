package git

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v87/github"
)

func ParseRepositoryIdentity(raw string) (GitRepositoryIdentity, error) {
	normalized, err := appconfig.NormalizeProjectRepositoryURL(raw)
	if err != nil {
		return GitRepositoryIdentity{}, err
	}
	parts := strings.Split(normalized, "/")
	if len(parts) != 3 {
		return GitRepositoryIdentity{}, fmt.Errorf("expected normalized host/owner/repo path, got %q", normalized)
	}
	return GitRepositoryIdentity{
		Host:  parts[0],
		Owner: parts[1],
		Repo:  parts[2],
		URL:   fmt.Sprintf("https://%s/%s/%s", parts[0], parts[1], parts[2]),
	}, nil
}

func (service *GitService) EnsureDataDir() error {
	if err := os.MkdirAll(service.config.DataDir, 0o755); err != nil {
		return fmt.Errorf("create git data dir: %w", err)
	}
	return nil
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}

func (service *GitService) MirrorPathForIdentity(identity GitRepositoryIdentity) string {
	return filepath.Join(service.config.DataDir, sanitizePathPart(identity.Host), sanitizePathPart(identity.Owner), sanitizePathPart(identity.Repo)+".git")
}

func CleanAccessToken(raw string) string {
	token := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("bearer "):])
	}
	return token
}

func ValidateAccessToken(raw string) (string, error) {
	token := CleanAccessToken(raw)
	if token == "" {
		return "", fmt.Errorf("git access token is required")
	}
	return token, nil
}

func (service *GitService) fetchRepositoryMetadata(ctx context.Context, accessToken string, identity GitRepositoryIdentity) (*githubRepositoryResponse, error) {
	client, err := service.githubClient(accessToken)
	if err != nil {
		return nil, err
	}
	repo, _, err := client.Repositories.Get(ctx, identity.Owner, identity.Repo)
	if err != nil {
		return nil, fmt.Errorf("github repository metadata lookup failed for %s/%s: %w", identity.Owner, identity.Repo, err)
	}
	defaultBranch := repo.GetDefaultBranch()
	if defaultBranch == "" {
		defaultBranch = "main"
	}
	htmlURL := repo.GetHTMLURL()
	if htmlURL == "" {
		htmlURL = identity.URL
	}
	return &githubRepositoryResponse{DefaultBranch: defaultBranch, HTMLURL: htmlURL}, nil
}

func (service *GitService) githubClient(accessToken string) (*github.Client, error) {
	options := []github.ClientOptionsFunc{
		github.WithAuthToken(accessToken),
		github.WithHTTPClient(service.client),
	}
	if strings.TrimRight(service.config.GitHubAPIBase, "/") != "https://api.github.com" {
		apiBase := strings.TrimRight(service.config.GitHubAPIBase, "/") + "/"
		options = append(options, github.WithEnterpriseURLs(apiBase, apiBase))
	}
	client, err := github.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create github client: %w", err)
	}
	return client, nil
}

func SyncRepositoryMirror(ctx context.Context, remoteURL string, mirrorPath string, auth *githttp.BasicAuth) error {
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return fmt.Errorf("create repository parent dir: %w", err)
	}
	if _, err := os.Stat(mirrorPath); errors.Is(err, os.ErrNotExist) {
		_, err = gogit.PlainCloneContext(ctx, mirrorPath, false, &gogit.CloneOptions{URL: remoteURL, Auth: auth, Tags: gogit.AllTags})
		if err != nil {
			return fmt.Errorf("clone repository: %w", err)
		}
		return nil
	}
	repo, err := gogit.PlainOpen(mirrorPath)
	if err != nil {
		return fmt.Errorf("open repository: %w", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		err = repo.FetchContext(ctx, &gogit.FetchOptions{RemoteName: gogit.DefaultRemoteName, Auth: auth, Force: true, Prune: true, Tags: gogit.AllTags})
		if err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
			return fmt.Errorf("fetch existing bare repository: %w", err)
		}
		return nil
	}
	err = repo.FetchContext(ctx, &gogit.FetchOptions{RemoteName: gogit.DefaultRemoteName, Auth: auth, Force: true, Prune: true, Tags: gogit.AllTags})
	if err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return fmt.Errorf("fetch repository: %w", err)
	}
	err = worktree.PullContext(ctx, &gogit.PullOptions{RemoteName: gogit.DefaultRemoteName, Auth: auth, Force: true})
	if err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return fmt.Errorf("pull repository: %w", err)
	}
	return nil
}

func ResolveGitReference(repo *gogit.Repository, requestedRef string, defaultBranch string) (string, plumbing.Hash, error) {
	candidates := []string{}
	if requestedRef != "" {
		candidates = append(candidates, requestedRef, "refs/heads/"+requestedRef, "refs/tags/"+requestedRef)
	}
	if defaultBranch != "" && requestedRef == "" {
		candidates = append(candidates, defaultBranch, "refs/heads/"+defaultBranch)
	}
	candidates = append(candidates, "HEAD")
	for _, candidate := range candidates {
		hash, err := repo.ResolveRevision(plumbing.Revision(candidate))
		if err == nil && hash != nil {
			resolved := candidate
			if strings.HasPrefix(candidate, "refs/heads/") {
				resolved = strings.TrimPrefix(candidate, "refs/heads/")
			}
			if strings.HasPrefix(candidate, "refs/tags/") {
				resolved = strings.TrimPrefix(candidate, "refs/tags/")
			}
			return resolved, *hash, nil
		}
	}
	return "", plumbing.ZeroHash, fmt.Errorf("could not resolve git ref %q", requestedRef)
}

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
			}
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
	iter, err := repo.References()
	if err != nil {
		return nil, fmt.Errorf("list git refs: %w", err)
	}
	err = iter.ForEach(func(reference *plumbing.Reference) error {
		name := reference.Name()
		switch {
		case name.IsBranch():
			branchName := name.Short()
			refs = append(refs, GitRef{Name: branchName, Type: "branch", Hash: reference.Hash().String(), Default: branchName == defaultBranch})
		case name.IsTag():
			refs = append(refs, GitRef{Name: name.Short(), Type: "tag", Hash: reference.Hash().String()})
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
	truncated := len(contentBytes) > inlineLimit
	if truncated {
		contentBytes = contentBytes[:inlineLimit]
	}
	encoding := "utf-8"
	content := ""
	if utf8.Valid(contentBytes) {
		content = string(contentBytes)
	} else {
		encoding = "base64"
		content = base64.StdEncoding.EncodeToString(contentBytes)
	}
	return &GitProjectFileResponse{ProjectID: projectID, Ref: ref, Path: normalizedPath, Name: filepath.Base(normalizedPath), Hash: file.Hash.String(), Size: file.Size, Encoding: encoding, Content: content, Truncated: truncated}, nil
}

func OpenRepository(path string) (*gogit.Repository, error) {
	repo, err := gogit.PlainOpen(path)
	if err != nil {
		return nil, fmt.Errorf("open git repository at %s: %w", path, err)
	}
	return repo, nil
}

func (service *GitService) RefreshProject(ctx context.Context, projectID string, identity GitRepositoryIdentity, state *geckodb.GitProjectState, accessToken string) (*GitProjectRefreshResponse, *geckodb.GitProjectState, error) {
	repoMetadata, err := service.fetchRepositoryMetadata(ctx, accessToken, identity)
	if err != nil {
		return nil, state, err
	}
	if state == nil {
		state = &geckodb.GitProjectState{ProjectID: projectID}
	}
	if state.MirrorPath == "" {
		state.MirrorPath = service.MirrorPathForIdentity(identity)
	}
	cloneURL := fmt.Sprintf("https://%s/%s/%s.git", identity.Host, identity.Owner, identity.Repo)
	if err := SyncRepositoryMirror(ctx, cloneURL, state.MirrorPath, &githttp.BasicAuth{Username: "x-access-token", Password: accessToken}); err != nil {
		return nil, state, err
	}
	updated := *state
	updated.DefaultBranch = sql.NullString{String: repoMetadata.DefaultBranch, Valid: repoMetadata.DefaultBranch != ""}
	updated.SyncState = GitSyncReady
	updated.LastRefreshedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	updated.LastError = sql.NullString{}
	return &GitProjectRefreshResponse{Success: true, ProjectID: projectID, SyncState: GitSyncReady, DefaultBranch: repoMetadata.DefaultBranch, LastFetchedRef: repoMetadata.DefaultBranch}, &updated, nil
}

func (service *GitService) StatusFromState(projectID string, organization string, project string, cfg appconfig.ProjectConfig, identity GitRepositoryIdentity, state *geckodb.GitProjectState) GitProjectStatusResponse {
	response := GitProjectStatusResponse{
		ProjectID:         projectID,
		Organization:      organization,
		Project:           project,
		ResourcePath:      fmt.Sprintf("/organization/%s/project/%s", organization, project),
		Config:            cfg,
		Repository:        identity,
		InstallationState: GitInstallationNotConnected,
		SyncState:         GitSyncNeverSynced,
	}
	if state == nil {
		return response
	}
	if state.InstallationID.Valid {
		installationID := state.InstallationID.Int64
		response.InstallationID = &installationID
		response.InstallationState = GitInstallationConnected
	}
	if state.InstallationTarget.Valid {
		response.InstallationTarget = state.InstallationTarget.String
	}
	if state.InstallationTargetType.Valid {
		response.InstallationTargetType = state.InstallationTargetType.String
	}
	if state.SyncState != "" {
		response.SyncState = state.SyncState
	}
	if state.DefaultBranch.Valid {
		response.DefaultBranch = state.DefaultBranch.String
	}
	if state.LastRefreshedAt.Valid {
		refreshedAt := state.LastRefreshedAt.Time
		response.LastRefreshedAt = &refreshedAt
	}
	if state.LastError.Valid {
		response.LastError = state.LastError.String
	}
	if state.MirrorPath != "" {
		if info, err := os.Stat(state.MirrorPath); err == nil && info.IsDir() {
			response.MirrorReady = true
		}
	}
	return response
}
