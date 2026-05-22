package git

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	appconfig "github.com/calypr/gecko/config"
	geckodb "github.com/calypr/gecko/internal/db"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v87/github"
)

var gitLFSPointerOIDPattern = regexp.MustCompile(`^oid sha256:([a-fA-F0-9]{64})$`)

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

func ValidateAuthorizationHeader(raw string) (string, error) {
	token := strings.TrimSpace(raw)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return "", fmt.Errorf("authorization header must use bearer auth")
	}
	return token, nil
}

func (service *GitService) RequestInstallationURL(ctx context.Context, authorizationHeader string, owner string, redirectPath string) (string, error) {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for GitHub App installation",
		}
	}
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	requestBody, err := json.Marshal(map[string]string{
		"owner":         owner,
		"redirect_path": redirectPath,
	})
	if err != nil {
		return "", fmt.Errorf("marshal fence github install URL request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github/install-url",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return "", fmt.Errorf("build fence github install URL request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("Fence github install URL request failed: %s", err),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read fence github install URL response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github install URL request failed with status %d", resp.StatusCode)
		}
		code := "integration_error"
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			code = "missing_authorization"
		case http.StatusForbidden:
			code = "forbidden"
		case http.StatusNotFound:
			code = "not_found"
		}
		return "", &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Code:       code,
			Message:    message,
		}
	}

	var payload fenceGitHubInstallURLResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github install URL response: %s", err),
		}
	}
	installURL := strings.TrimSpace(payload.InstallURL)
	if installURL == "" {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence github install URL response did not include install_url",
		}
	}
	return installURL, nil
}

func (service *GitService) RequestOrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string) (GitRepositoryInstallationStatus, error) {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for git organization installation status",
		}
	}
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	requestBody, err := json.Marshal(map[string]string{"owner": organization})
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("marshal fence github organization installation status request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github/organization-installation",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("build fence github organization installation status request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{StatusCode: http.StatusBadGateway, Code: "integration_error", Message: fmt.Sprintf("Fence github organization installation status request failed: %s", err)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("read fence github organization installation status response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github organization installation status request failed with status %d", resp.StatusCode)
		}
		code := "integration_error"
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			code = "missing_authorization"
		case http.StatusForbidden:
			code = "forbidden"
		case http.StatusNotFound:
			code = "not_found"
		}
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{StatusCode: resp.StatusCode, Code: code, Message: message}
	}
	var payload fenceGitHubOrganizationInstallationStatusResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{StatusCode: http.StatusBadGateway, Code: "integration_error", Message: fmt.Sprintf("invalid Fence github organization installation status response: %s", err)}
	}
	return GitRepositoryInstallationStatus{
		Installed:           payload.Installed,
		InstallationID:      payload.InstallationID,
		Target:              strings.TrimSpace(payload.Target),
		TargetType:          strings.TrimSpace(payload.TargetType),
		HTMLURL:             strings.TrimSpace(payload.HTMLURL),
		RepositorySelection: strings.TrimSpace(payload.RepositorySelection),
	}, nil
}

func (service *GitService) RequestInstallationStatus(ctx context.Context, authorizationHeader string, identity GitRepositoryIdentity) (GitRepositoryInstallationStatus, error) {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for git installation status",
		}
	}
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}

	requestBody, err := json.Marshal(map[string]string{
		"owner": identity.Owner,
		"repo":  identity.Repo,
	})
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("marshal fence github installation status request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github/installation",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("build fence github installation status request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("Fence github installation status request failed: %s", err),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GitRepositoryInstallationStatus{}, fmt.Errorf("read fence github installation status response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github installation status request failed with status %d", resp.StatusCode)
		}
		code := "integration_error"
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			code = "missing_authorization"
		case http.StatusForbidden:
			code = "forbidden"
		case http.StatusNotFound:
			code = "not_found"
		}
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Code:       code,
			Message:    message,
		}
	}

	var payload fenceGitHubInstallationStatusResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return GitRepositoryInstallationStatus{}, &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github installation status response: %s", err),
		}
	}
	return GitRepositoryInstallationStatus{
		Installed:      payload.Installed,
		InstallationID: payload.InstallationID,
		Target:         strings.TrimSpace(payload.Target),
		TargetType:     strings.TrimSpace(payload.TargetType),
		HTMLURL:        strings.TrimSpace(payload.HTMLURL),
	}, nil
}

func (service *GitService) RequestInstallationToken(ctx context.Context, authorizationHeader string, identity GitRepositoryIdentity) (string, error) {
	if strings.TrimSpace(service.config.FenceBaseURL) == "" {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    "Fence base URL is not configured for git refresh",
		}
	}
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}

	requestBody, err := json.Marshal(map[string]string{
		"owner": identity.Owner,
		"repo":  identity.Repo,
	})
	if err != nil {
		return "", fmt.Errorf("marshal fence github token request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(service.config.FenceBaseURL, "/")+"/credentials/github/token",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return "", fmt.Errorf("build fence github token request: %w", err)
	}
	req.Header.Set("Authorization", authorizationHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := service.client.Do(req)
	if err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("Fence github token request failed: %s", err),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read fence github token response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := decodeFenceErrorResponse(body)
		if message == "" {
			message = fmt.Sprintf("Fence github token request failed with status %d", resp.StatusCode)
		}
		code := "integration_error"
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			code = "missing_authorization"
		case http.StatusForbidden:
			code = "forbidden"
		case http.StatusNotFound:
			code = "not_found"
		}
		return "", &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Code:       code,
			Message:    message,
		}
	}

	var payload fenceGitHubTokenResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", &HTTPStatusError{
			StatusCode: http.StatusBadGateway,
			Code:       "integration_error",
			Message:    fmt.Sprintf("invalid Fence github token response: %s", err),
		}
	}
	return ValidateAccessToken(payload.Token)
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

func lookupGitPathLastModified(repo *gogit.Repository, from plumbing.Hash, path string) (*time.Time, error) {
	normalizedPath := strings.Trim(strings.TrimSpace(path), "/")
	if normalizedPath == "" {
		return nil, nil
	}

	iter, err := repo.Log(&gogit.LogOptions{
		From:  from,
		Order: gogit.LogOrderCommitterTime,
		PathFilter: func(candidate string) bool {
			trimmed := strings.Trim(strings.TrimSpace(candidate), "/")
			return trimmed == normalizedPath || strings.HasPrefix(trimmed, normalizedPath+"/")
		},
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	commit, err := iter.Next()
	if err != nil {
		return nil, err
	}
	lastModifiedAt := commit.Committer.When.UTC()
	return &lastModifiedAt, nil
}

func ParseGitLFSPointer(content []byte) *GitLFSPointerInfo {
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

	return &GitLFSPointerInfo{
		Version: "https://git-lfs.github.com/spec/v1",
		OID:     strings.ToLower(oidMatch[1]),
		Size:    size,
	}
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

func (service *GitService) GetGitHubFileMetadata(ctx context.Context, authorizationHeader string, identity GitRepositoryIdentity, ref string, path string) (*github.RepositoryContent, []byte, error) {
	authorizationHeader, err := ValidateAuthorizationHeader(authorizationHeader)
	if err != nil {
		return nil, nil, &HTTPStatusError{
			StatusCode: http.StatusUnauthorized,
			Code:       "missing_authorization",
			Message:    err.Error(),
		}
	}
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, identity)
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
	updated.InstallationTarget = sql.NullString{String: identity.Owner, Valid: identity.Owner != ""}
	updated.InstallationTargetType = sql.NullString{String: "Organization", Valid: identity.Owner != ""}
	updated.DefaultBranch = sql.NullString{String: repoMetadata.DefaultBranch, Valid: repoMetadata.DefaultBranch != ""}
	updated.SyncState = GitSyncReady
	updated.LastRefreshedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	updated.LastError = sql.NullString{}
	return &GitProjectRefreshResponse{Success: true, ProjectID: projectID, SyncState: GitSyncReady, DefaultBranch: repoMetadata.DefaultBranch, LastFetchedRef: repoMetadata.DefaultBranch}, &updated, nil
}

func (service *GitService) StatusFromState(projectID string, organization string, project string, cfg appconfig.ProjectConfig, identity GitRepositoryIdentity, state *geckodb.GitProjectState, orgState *geckodb.GitOrganizationState) GitProjectStatusResponse {
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
	if orgState != nil {
		response.OrganizationAppInstalled = orgState.Installed
		if orgState.HTMLURL.Valid {
			response.OrganizationHTMLURL = orgState.HTMLURL.String
		}
		if orgState.RepositorySelection.Valid {
			response.OrganizationRepositorySelection = orgState.RepositorySelection.String
		}
	}
	if state == nil {
		return response
	}
	if state.InstallationID.Valid || state.InstallationTarget.Valid {
		response.InstallationState = GitInstallationConnected
	}
	if state.InstallationID.Valid {
		installationID := state.InstallationID.Int64
		response.InstallationID = &installationID
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

func OrganizationConfigurationState(appInstalled bool, configuredProjects int, totalProjects int) string {
	switch {
	case !appInstalled:
		return "not_connected"
	case totalProjects == 0:
		return "connected"
	case configuredProjects <= 0:
		return "installed_unconfigured"
	case configuredProjects < totalProjects:
		return "partially_configured"
	default:
		return "connected"
	}
}
