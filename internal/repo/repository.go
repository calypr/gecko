package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	appconfig "github.com/calypr/gecko/config"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

var gitLFSPointerOIDPattern = regexp.MustCompile(`^oid sha256:([a-fA-F0-9]{64})$`)

func SyncRepositoryMirror(ctx context.Context, remoteURL string, mirrorPath string, auth *githttp.BasicAuth) error {
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return fmt.Errorf("create repository parent dir: %w", err)
	}
	if _, err := os.Stat(mirrorPath); errors.Is(err, os.ErrNotExist) {
		_, err = gogit.PlainCloneContext(ctx, mirrorPath, false, &gogit.CloneOptions{URL: remoteURL, Auth: auth, Tags: gogit.AllTags})
		if err != nil {
			if isEmptyRemoteRepositoryError(err) {
				repo, initErr := gogit.PlainInit(mirrorPath, false)
				if initErr != nil {
					return fmt.Errorf("initialize empty repository: %w", initErr)
				}
				if _, remoteErr := repo.CreateRemote(&config.RemoteConfig{
					Name: gogit.DefaultRemoteName,
					URLs: []string{remoteURL},
				}); remoteErr != nil && !strings.Contains(strings.ToLower(remoteErr.Error()), "remote already exists") {
					return fmt.Errorf("create remote for empty repository: %w", remoteErr)
				}
				return nil
			}
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

func isEmptyRemoteRepositoryError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "remote repository is empty")
}

func RepositoryIsEmpty(repo *gogit.Repository) bool {
	if repo == nil {
		return true
	}
	iter, err := repo.References()
	if err != nil {
		return true
	}
	hasRefs := false
	_ = iter.ForEach(func(reference *plumbing.Reference) error {
		name := reference.Name()
		if name.IsBranch() || name.IsRemote() || name.IsTag() {
			hasRefs = true
			return storer.ErrStop
		}
		return nil
	})
	return !hasRefs
}

func ResolveGitReference(repo *gogit.Repository, requestedRef string, defaultBranch string) (string, plumbing.Hash, error) {
	candidates := []string{}
	if requestedRef != "" {
		candidates = append(
			candidates,
			requestedRef,
			"refs/heads/"+requestedRef,
			"refs/remotes/origin/"+requestedRef,
			"refs/tags/"+requestedRef,
		)
	}
	if defaultBranch != "" && requestedRef == "" {
		candidates = append(
			candidates,
			defaultBranch,
			"refs/heads/"+defaultBranch,
			"refs/remotes/origin/"+defaultBranch,
		)
	}
	candidates = append(candidates, "HEAD")
	for _, candidate := range candidates {
		hash, err := repo.ResolveRevision(plumbing.Revision(candidate))
		if err == nil && hash != nil {
			resolved := candidate
			if strings.HasPrefix(candidate, "refs/heads/") {
				resolved = strings.TrimPrefix(candidate, "refs/heads/")
			}
			if strings.HasPrefix(candidate, "refs/remotes/origin/") {
				resolved = strings.TrimPrefix(candidate, "refs/remotes/origin/")
			}
			if strings.HasPrefix(candidate, "refs/tags/") {
				resolved = strings.TrimPrefix(candidate, "refs/tags/")
			}
			return resolved, *hash, nil
		}
	}
	return "", plumbing.ZeroHash, fmt.Errorf("could not resolve git ref %q", requestedRef)
}

func OpenRepository(path string) (*gogit.Repository, error) {
	repo, err := gogit.PlainOpen(path)
	if err != nil {
		return nil, fmt.Errorf("open git repository at %s: %w", path, err)
	}
	return repo, nil
}

func BuildLFSPointerContent(checksum string, size int64) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", strings.ToLower(strings.TrimSpace(checksum)), size)
}

func ParseLFSPointer(content []byte) *LFSPointerInfo {
	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) < 3 {
		return nil
	}

	versionLine := strings.TrimSpace(lines[0])
	if versionLine != "version https://git-lfs.github.com/spec/v1" {
		return nil
	}
	oidMatch := gitLFSPointerOIDPattern.FindStringSubmatch(strings.TrimSpace(lines[1]))
	if len(oidMatch) != 2 {
		return nil
	}
	sizeLine := strings.TrimSpace(lines[2])
	if !strings.HasPrefix(sizeLine, "size ") {
		return nil
	}
	size, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(sizeLine, "size ")), 10, 64)
	if err != nil {
		return nil
	}

	return &LFSPointerInfo{
		Version: "https://git-lfs.github.com/spec/v1",
		OID:     strings.ToLower(oidMatch[1]),
		Size:    size,
	}
}

func ParseIdentity(raw string) (Identity, error) {
	normalized, err := appconfig.NormalizeProjectRepositoryURL(raw)
	if err != nil {
		return Identity{}, err
	}
	parts := strings.Split(normalized, "/")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("expected normalized host/owner/repo path, got %q", normalized)
	}
	return Identity{
		Host:  parts[0],
		Owner: parts[1],
		Repo:  parts[2],
		URL:   fmt.Sprintf("https://%s/%s/%s", parts[0], parts[1], parts[2]),
	}, nil
}
