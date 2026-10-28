package upload

import (
	"fmt"
	"strings"
	"time"

	geckodb "github.com/calypr/gecko/internal/db"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/uuid"
)

const (
	SessionPending   = "pending_upload"
	SessionReady     = "ready_for_pr"
	SessionFinalized = "finalized"

	FilePending   = "pending_upload"
	FileUploaded  = "uploaded"
	FileCollision = "collision"
)

func NormalizeSubdirectory(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func NormalizeFileName(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("file name is required")
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return "", fmt.Errorf("file name must not include path separators")
	}
	return trimmed, nil
}

func BuildBranchName(project string) string {
	slug := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(project), "_", "-"))
	if slug == "" {
		slug = "upload"
	}
	short := strings.ToLower(strings.ReplaceAll(uuid.NewString(), "-", ""))[:8]
	return fmt.Sprintf("calypr/upload-%s-%s-%s", slug, time.Now().UTC().Format("20060102-150405"), short)
}

func BuildDefaultPRTitle(project string, fileCount int) string {
	if fileCount == 1 {
		return fmt.Sprintf("Add 1 LFS file to %s", project)
	}
	return fmt.Sprintf("Add %d LFS files to %s", fileCount, project)
}

func BuildDefaultPRBody(baseBranch string, subdirectory string) string {
	if subdirectory == "" {
		return fmt.Sprintf("Upload Git LFS-backed files into `%s`.", baseBranch)
	}
	return fmt.Sprintf("Upload Git LFS-backed files into `%s` under `%s`.", baseBranch, subdirectory)
}

func BuildTargetPath(subdirectory string, fileName string) string {
	if subdirectory == "" {
		return fileName
	}
	return strings.Trim(strings.TrimSpace(subdirectory), "/") + "/" + fileName
}

func BuildSessionResponse(session geckodb.GitUploadSession, files []geckodb.GitUploadSessionFile) SessionResponse {
	response := SessionResponse{
		SessionID:      session.ID,
		ProjectID:      session.ProjectID,
		BaseBranch:     session.BaseBranch,
		BranchName:     session.BranchName,
		PRTitle:        session.PRTitle,
		PRBody:         session.PRBody,
		Status:         session.Status,
		Files:          make([]FileStatus, 0, len(files)),
		TargetSubdir:   session.TargetSubdir.String,
		PullRequestURL: session.PullRequestURL.String,
		CommitSHA:      session.CommitSHA.String,
	}
	for _, file := range files {
		status := FileStatus{
			FileName:   file.FileName,
			TargetPath: file.TargetPath,
			Size:       file.Size,
			Status:     file.Status,
			Collision:  file.Status == FileCollision,
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

func PathExistsInRef(repo *gogit.Repository, hash plumbing.Hash, path string) (bool, error) {
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
