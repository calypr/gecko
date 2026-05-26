package git

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	geckodb "github.com/calypr/gecko/internal/db"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/go-github/v87/github"
	"github.com/google/uuid"
)

const (
	GitUploadSessionPending   = "pending_upload"
	GitUploadSessionReady     = "ready_for_pr"
	GitUploadSessionFinalized = "finalized"

	GitUploadFilePending   = "pending_upload"
	GitUploadFileUploaded  = "uploaded"
	GitUploadFileCollision = "collision"
)

func NormalizeGitUploadSubdirectory(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func NormalizeGitUploadFileName(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("file name is required")
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return "", fmt.Errorf("file name must not include path separators")
	}
	return trimmed, nil
}

func BuildGitUploadBranchName(project string) string {
	slug := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(project), "_", "-"))
	if slug == "" {
		slug = "upload"
	}
	short := strings.ToLower(strings.ReplaceAll(uuid.NewString(), "-", ""))[:8]
	return fmt.Sprintf("calypr/upload-%s-%s-%s", slug, time.Now().UTC().Format("20060102-150405"), short)
}

func BuildLFSPointerContent(checksum string, size int64) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", strings.ToLower(strings.TrimSpace(checksum)), size)
}

func BuildDefaultUploadPRTitle(project string, fileCount int) string {
	if fileCount == 1 {
		return fmt.Sprintf("Add 1 LFS file to %s", project)
	}
	return fmt.Sprintf("Add %d LFS files to %s", fileCount, project)
}

func BuildDefaultUploadPRBody(baseBranch string, subdirectory string) string {
	if subdirectory == "" {
		return fmt.Sprintf("Upload Git LFS-backed files into `%s`.", baseBranch)
	}
	return fmt.Sprintf("Upload Git LFS-backed files into `%s` under `%s`.", baseBranch, subdirectory)
}

func BuildGitUploadTargetPath(subdirectory string, fileName string) string {
	if subdirectory == "" {
		return fileName
	}
	return strings.Trim(strings.TrimSpace(subdirectory), "/") + "/" + fileName
}

func BuildGitUploadSessionResponse(session geckodb.GitUploadSession, files []geckodb.GitUploadSessionFile) GitUploadSessionResponse {
	response := GitUploadSessionResponse{
		SessionID:      session.ID,
		ProjectID:      session.ProjectID,
		BaseBranch:     session.BaseBranch,
		BranchName:     session.BranchName,
		PRTitle:        session.PRTitle,
		PRBody:         session.PRBody,
		Status:         session.Status,
		Files:          make([]GitUploadSessionFileStatus, 0, len(files)),
		TargetSubdir:   session.TargetSubdir.String,
		PullRequestURL: session.PullRequestURL.String,
		CommitSHA:      session.CommitSHA.String,
	}
	for _, file := range files {
		status := GitUploadSessionFileStatus{
			FileName:   file.FileName,
			TargetPath: file.TargetPath,
			Size:       file.Size,
			Status:     file.Status,
			Collision:  file.Status == GitUploadFileCollision,
		}
		if file.Checksum.Valid {
			status.Checksum = file.Checksum.String
		}
		if file.DRSObjectID.Valid {
			status.DRSObjectID = file.DRSObjectID.String
		}
		if file.Error.Valid {
			status.Error = file.Error.String
		}
		if status.Collision {
			response.HasConflicts = true
		}
		response.Files = append(response.Files, status)
	}
	return response
}

func GitPathExistsInRef(repo *gogit.Repository, hash plumbing.Hash, path string) (bool, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return false, fmt.Errorf("load commit: %w", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return false, fmt.Errorf("load tree: %w", err)
	}
	normalized := strings.Trim(strings.TrimSpace(path), "/")
	if normalized == "" {
		return false, nil
	}
	if _, err := tree.File(normalized); err == nil {
		return true, nil
	}
	if _, err := tree.Tree(normalized); err == nil {
		return true, nil
	}
	return false, nil
}

func githubWriteStatusError(message string, response *github.Response, err error) *HTTPStatusError {
	statusCode := http.StatusBadGateway
	if response != nil && response.StatusCode > 0 {
		statusCode = response.StatusCode
	}
	return &HTTPStatusError{
		StatusCode: statusCode,
		Code:       "integration_error",
		Message:    fmt.Sprintf("%s: %s", message, err),
	}
}

func (service *GitService) CreateGitHubUploadPullRequest(
	ctx context.Context,
	authorizationHeader string,
	identity GitRepositoryIdentity,
	baseBranch string,
	branchName string,
	title string,
	body string,
	files []geckodb.GitUploadSessionFile,
) (string, string, error) {
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, identity)
	if err != nil {
		return "", "", err
	}
	client, err := service.githubClient(accessToken)
	if err != nil {
		return "", "", err
	}
	baseRef, response, err := client.Git.GetRef(ctx, identity.Owner, identity.Repo, "refs/heads/"+baseBranch)
	if err != nil {
		return "", "", githubWriteStatusError("failed to load GitHub base branch ref", response, err)
	}
	baseCommitSHA := baseRef.GetObject().GetSHA()
	baseCommit, response, err := client.Git.GetCommit(ctx, identity.Owner, identity.Repo, baseCommitSHA)
	if err != nil {
		return "", "", githubWriteStatusError("failed to load GitHub base commit", response, err)
	}
	entries := make([]*github.TreeEntry, 0, len(files))
	for _, file := range files {
		if !file.Checksum.Valid {
			return "", "", fmt.Errorf("missing checksum for %s", file.TargetPath)
		}
		entries = append(entries, &github.TreeEntry{
			Path:    github.Ptr(file.TargetPath),
			Mode:    github.Ptr("100644"),
			Type:    github.Ptr("blob"),
			Content: github.Ptr(BuildLFSPointerContent(file.Checksum.String, file.Size)),
		})
	}
	tree, response, err := client.Git.CreateTree(ctx, identity.Owner, identity.Repo, baseCommit.GetTree().GetSHA(), entries)
	if err != nil {
		return "", "", githubWriteStatusError("failed to create GitHub tree", response, err)
	}
	commit, response, err := client.Git.CreateCommit(ctx, identity.Owner, identity.Repo, github.Commit{
		Message: github.Ptr(title),
		Tree:    &github.Tree{SHA: github.Ptr(tree.GetSHA())},
		Parents: []*github.Commit{{SHA: github.Ptr(baseCommitSHA)}},
	}, nil)
	if err != nil {
		return "", "", githubWriteStatusError("failed to create GitHub commit", response, err)
	}
	_, response, err = client.Git.CreateRef(ctx, identity.Owner, identity.Repo, github.CreateRef{
		Ref: "refs/heads/" + branchName,
		SHA: commit.GetSHA(),
	})
	if err != nil {
		return "", "", githubWriteStatusError("failed to create GitHub branch", response, err)
	}
	pr, response, err := client.PullRequests.Create(ctx, identity.Owner, identity.Repo, &github.NewPullRequest{
		Title: github.Ptr(title),
		Body:  github.Ptr(body),
		Base:  github.Ptr(baseBranch),
		Head:  github.Ptr(branchName),
	})
	if err != nil {
		return "", "", githubWriteStatusError("failed to create GitHub pull request", response, err)
	}
	return commit.GetSHA(), pr.GetHTMLURL(), nil
}
