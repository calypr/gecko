package repo

import (
	"context"
	"fmt"
	"net/http"

	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/google/go-github/v87/github"
)

func githubWriteStatusError(message string, response *github.Response, err error) *domain.HTTPStatusError {
	statusCode := http.StatusBadGateway
	if response != nil && response.StatusCode > 0 {
		statusCode = response.StatusCode
	}
	return &domain.HTTPStatusError{
		StatusCode: statusCode,
		Code:       "integration_error",
		Message:    fmt.Sprintf("%s: %s", message, err),
	}
}

func (service *Service) FindGitHubUploadPullRequest(
	ctx context.Context,
	authorizationHeader string,
	organization string,
	project string,
	identity Identity,
	baseBranch string,
	branchName string,
) (string, string, bool, error) {
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, "write")
	if err != nil {
		return "", "", false, err
	}
	client, err := service.githubClient(accessToken)
	if err != nil {
		return "", "", false, err
	}
	prs, response, err := client.PullRequests.List(ctx, identity.Owner, identity.Repo, &github.PullRequestListOptions{
		State: "open",
		Head:  identity.Owner + ":" + branchName,
		Base:  baseBranch,
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	})
	if err != nil {
		return "", "", false, githubWriteStatusError("failed to find GitHub pull request", response, err)
	}
	if len(prs) == 0 {
		return "", "", false, nil
	}
	pr := prs[0]
	return pr.GetHead().GetSHA(), pr.GetHTMLURL(), true, nil
}

func (service *Service) CreateGitHubUploadPullRequest(
	ctx context.Context,
	authorizationHeader string,
	organization string,
	project string,
	identity Identity,
	baseBranch string,
	branchName string,
	title string,
	body string,
	files []geckodb.GitUploadSessionFile,
) (string, string, error) {
	accessToken, err := service.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, "write")
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
