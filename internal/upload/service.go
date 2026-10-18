package upload

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/repo"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Service struct {
	db   *sqlx.DB
	repo *repo.Service
}

func NewService(db *sqlx.DB, repository *repo.Service) *Service {
	return &Service{db: db, repo: repository}
}

func (service *Service) CreateSession(ctx context.Context, authorizationHeader string, projectID string, organization string, project string, identity repo.Identity, request SessionCreateRequest) (*geckodb.GitUploadSession, []geckodb.GitUploadSessionFile, error) {
	state, err := service.ensureConnectedMirrorProject(ctx, projectID, identity)
	if err != nil {
		return nil, nil, err
	}
	baseBranch := strings.TrimSpace(request.BaseBranch)
	if baseBranch == "" {
		baseBranch = strings.TrimSpace(state.DefaultBranch.String)
	}
	if baseBranch == "" {
		return nil, nil, conflictError("repository has no default branch yet. Initialize your repo first before uploading files.", map[string]any{"project_id": projectID})
	}
	if len(request.Files) == 0 {
		return nil, nil, invalidRequestError("at least one file is required", map[string]any{"project_id": projectID})
	}
	targetSubdir := NormalizeSubdirectory(request.TargetSubdir)
	sessionID := uuid.NewString()
	prepareCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	state, err = service.ensureMirrorReadyForUpload(prepareCtx, authorizationHeader, projectID, identity, state)
	if err != nil {
		return nil, nil, uploadPreparationError(projectID, err)
	}
	files, _, err := service.sessionFilesFromManifest(sessionID, targetSubdir, baseBranch, request.Files, state)
	if err != nil {
		return nil, nil, uploadPreparationError(projectID, err)
	}
	now := time.Now().UTC()
	session := geckodb.GitUploadSession{
		ID:           sessionID,
		ProjectID:    projectID,
		Organization: organization,
		Project:      project,
		RepoHost:     identity.Host,
		RepoOwner:    identity.Owner,
		RepoName:     identity.Repo,
		BaseBranch:   baseBranch,
		TargetSubdir: sql.NullString{String: targetSubdir, Valid: targetSubdir != ""},
		BranchName:   BuildBranchName(project),
		PRTitle:      BuildDefaultPRTitle(project, len(request.Files)),
		PRBody:       BuildDefaultPRBody(baseBranch, targetSubdir),
		Status:       SessionPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := geckodb.UpsertGitUploadSession(service.db, session); err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to persist upload session: %s", err), map[string]any{"project_id": projectID})
	}
	if err := geckodb.ReplaceGitUploadSessionFiles(service.db, session.ID, files); err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to persist upload session files: %s", err), map[string]any{"project_id": projectID, "session_id": session.ID})
	}
	return &session, files, nil
}

func (service *Service) GetSession(ctx context.Context, projectID string, sessionID string) (*geckodb.GitUploadSession, []geckodb.GitUploadSessionFile, error) {
	session, err := geckodb.GitUploadSessionByID(service.db, sessionID)
	if err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to read upload session: %s", err), uploadSessionDetails(projectID, sessionID))
	}
	if session == nil || session.ProjectID != projectID {
		return nil, nil, domain.NewTypedError(
			domain.ErrorKindNotFound,
			apierror.Type("not_found"),
			http.StatusNotFound,
			"upload session was not found",
			uploadSessionDetails(projectID, sessionID),
		)
	}
	files, err := geckodb.ListGitUploadSessionFiles(service.db, sessionID)
	if err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to read upload session files: %s", err), uploadSessionDetails(projectID, sessionID))
	}
	return session, files, nil
}

func (service *Service) AttachFiles(ctx context.Context, projectID string, sessionID string, request AttachFilesRequest) (*geckodb.GitUploadSession, []geckodb.GitUploadSessionFile, error) {
	session, files, err := service.GetSession(ctx, projectID, sessionID)
	if err != nil {
		return nil, nil, err
	}
	fileMap := make(map[string]*geckodb.GitUploadSessionFile, len(files))
	for i := range files {
		fileMap[files[i].TargetPath] = &files[i]
	}
	for _, attachment := range request.Files {
		targetPath := strings.Trim(strings.TrimSpace(attachment.TargetPath), "/")
		fileState, ok := fileMap[targetPath]
		if !ok {
			return nil, nil, invalidRequestError(
				fmt.Sprintf("upload session does not contain target path %s", targetPath),
				uploadSessionDetails(projectID, sessionID),
			)
		}
		fileState.Size = attachment.Size
		fileState.Checksum = sql.NullString{String: strings.ToLower(strings.TrimSpace(attachment.Checksum)), Valid: strings.TrimSpace(attachment.Checksum) != ""}
		fileState.DRSObjectID = sql.NullString{String: strings.TrimSpace(attachment.DRSObjectID), Valid: strings.TrimSpace(attachment.DRSObjectID) != ""}
		if fileState.Status != FileCollision {
			fileState.Status = FileUploaded
			fileState.Error = sql.NullString{}
		}
	}
	session.Status = SessionReady
	for _, file := range files {
		if file.Status == FileCollision {
			session.Status = SessionPending
			break
		}
		if !file.Checksum.Valid || !file.DRSObjectID.Valid {
			session.Status = SessionPending
		}
	}
	session.UpdatedAt = time.Now().UTC()
	if err := geckodb.UpsertGitUploadSession(service.db, *session); err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to update upload session: %s", err), uploadSessionDetails(projectID, sessionID))
	}
	if err := geckodb.ReplaceGitUploadSessionFiles(service.db, sessionID, files); err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to update upload session files: %s", err), uploadSessionDetails(projectID, sessionID))
	}
	return session, files, nil
}

func (service *Service) Finalize(ctx context.Context, authorizationHeader string, projectID string, identity repo.Identity, sessionID string, request FinalizeRequest) (*geckodb.GitUploadSession, []geckodb.GitUploadSessionFile, error) {
	session, files, err := service.GetSession(ctx, projectID, sessionID)
	if err != nil {
		return nil, nil, err
	}
	if session.Status == SessionFinalized {
		if session.PullRequestURL.Valid && session.CommitSHA.Valid {
			return session, files, nil
		}
		return nil, nil, conflictError("upload session is finalized but missing pull request details", uploadSessionDetails(projectID, sessionID))
	}
	if title := strings.TrimSpace(request.PRTitle); title != "" {
		session.PRTitle = title
	}
	if body := strings.TrimSpace(request.PRBody); body != "" {
		session.PRBody = body
	}
	for _, file := range files {
		if file.Status == FileCollision {
			return nil, nil, conflictError("upload session contains target path conflicts", uploadSessionDetails(projectID, sessionID))
		}
		if !file.Checksum.Valid || !file.DRSObjectID.Valid {
			return nil, nil, conflictError(fmt.Sprintf("upload session file %s is not fully attached", file.TargetPath), uploadSessionDetails(projectID, sessionID))
		}
	}
	finalizeCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	org, project, _ := strings.Cut(projectID, "/")
	commitSHA, prURL, found, err := service.repo.FindGitHubUploadPullRequest(
		finalizeCtx,
		authorizationHeader,
		org,
		project,
		identity,
		session.BaseBranch,
		session.BranchName,
	)
	if err != nil {
		return nil, nil, uploadRemoteError(projectID, sessionID, err, "failed to find upload pull request")
	}
	if found {
		if commitSHA == "" || prURL == "" {
			return nil, nil, conflictError("existing upload pull request is missing commit or URL details", uploadSessionDetails(projectID, sessionID))
		}
		session.Status = SessionFinalized
		session.CommitSHA = sql.NullString{String: commitSHA, Valid: true}
		session.PullRequestURL = sql.NullString{String: prURL, Valid: true}
		session.UpdatedAt = time.Now().UTC()
		if err := geckodb.UpsertGitUploadSession(service.db, *session); err != nil {
			return nil, nil, databaseError(fmt.Sprintf("failed to persist finalized upload session: %s", err), uploadSessionDetails(projectID, sessionID))
		}
		return session, files, nil
	}
	commitSHA, prURL, err = service.repo.CreateGitHubUploadPullRequest(
		finalizeCtx,
		authorizationHeader,
		org,
		project,
		identity,
		session.BaseBranch,
		session.BranchName,
		session.PRTitle,
		session.PRBody,
		files,
	)
	if err != nil {
		return nil, nil, uploadRemoteError(projectID, sessionID, err, "failed to create upload pull request")
	}
	session.Status = SessionFinalized
	session.CommitSHA = sql.NullString{String: commitSHA, Valid: commitSHA != ""}
	session.PullRequestURL = sql.NullString{String: prURL, Valid: prURL != ""}
	session.UpdatedAt = time.Now().UTC()
	if err := geckodb.UpsertGitUploadSession(service.db, *session); err != nil {
		return nil, nil, databaseError(fmt.Sprintf("failed to persist finalized upload session: %s", err), uploadSessionDetails(projectID, sessionID))
	}
	return session, files, nil
}

func uploadRemoteError(projectID string, sessionID string, err error, message string) error {
	if statusErr, ok := err.(*domain.HTTPStatusError); ok {
		return domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type(statusErr.Code),
			statusErr.StatusCode,
			statusErr.Message,
			uploadSessionDetails(projectID, sessionID),
		)
	}
	return domain.NewTypedError(
		domain.ErrorKindIntegration,
		apierror.Type("integration_error"),
		http.StatusBadGateway,
		fmt.Sprintf("%s: %s", message, err),
		uploadSessionDetails(projectID, sessionID),
	)
}

func (service *Service) ensureConnectedMirrorProject(ctx context.Context, projectID string, identity repo.Identity) (*geckodb.GitProjectState, error) {
	state, err := service.repo.LoadProjectState(ctx, service.db, projectID, identity)
	if err != nil {
		return nil, databaseError(fmt.Sprintf("failed to read git project state: %s", err), map[string]any{"project_id": projectID})
	}
	if state == nil || !state.InstallationID.Valid {
		return nil, conflictError("project is not connected to the GitHub App", map[string]any{"project_id": projectID})
	}
	if strings.TrimSpace(state.MirrorPath) == "" {
		return nil, conflictError("project mirror is not ready", map[string]any{"project_id": projectID})
	}
	return state, nil
}

func (service *Service) ensureMirrorReadyForUpload(ctx context.Context, authorizationHeader string, projectID string, identity repo.Identity, state *geckodb.GitProjectState) (*geckodb.GitProjectState, error) {
	if state == nil {
		return nil, fmt.Errorf("git project state is required")
	}
	if _, err := repo.OpenRepository(state.MirrorPath); err == nil {
		return state, nil
	}
	if _, err := os.Stat(state.MirrorPath); err == nil {
		return nil, fmt.Errorf("open git repository at %s failed and mirror path exists", state.MirrorPath)
	}
	org, project, _ := strings.Cut(projectID, "/")
	accessToken, err := service.repo.RequestInstallationToken(ctx, authorizationHeader, org, project, identity, "read")
	if err != nil {
		return nil, err
	}
	_, updatedState, err := service.repo.RefreshProject(ctx, projectID, identity, state, accessToken)
	if err != nil {
		return nil, err
	}
	if err := geckodb.UpsertGitProjectStateContext(ctx, service.db, *updatedState); err != nil {
		return nil, fmt.Errorf("persist refreshed git project state: %w", err)
	}
	return updatedState, nil
}

func (service *Service) sessionFilesFromManifest(sessionID string, subdirectory string, baseBranch string, files []FileManifest, mirrorState *geckodb.GitProjectState) ([]geckodb.GitUploadSessionFile, bool, error) {
	openedRepo, err := repo.OpenRepository(mirrorState.MirrorPath)
	if err != nil {
		return nil, false, err
	}
	_, hash, err := repo.ResolveGitReference(openedRepo, baseBranch, mirrorState.DefaultBranch.String)
	if err != nil {
		return nil, false, err
	}
	sessionFiles := make([]geckodb.GitUploadSessionFile, 0, len(files))
	hasConflicts := false
	seenPaths := make(map[string]struct{}, len(files))
	for _, manifest := range files {
		fileName, err := NormalizeFileName(manifest.Name)
		if err != nil {
			return nil, false, err
		}
		targetPath := BuildTargetPath(subdirectory, fileName)
		targetPath = strings.Trim(strings.TrimSpace(targetPath), "/")
		if _, ok := seenPaths[targetPath]; ok {
			sessionFiles = append(sessionFiles, geckodb.GitUploadSessionFile{
				SessionID:  sessionID,
				FileName:   fileName,
				TargetPath: targetPath,
				Size:       manifest.Size,
				Status:     FileCollision,
				Error:      sql.NullString{String: "duplicate target path in upload batch", Valid: true},
			})
			hasConflicts = true
			continue
		}
		seenPaths[targetPath] = struct{}{}
		exists, err := PathExistsInRef(openedRepo, hash, targetPath)
		if err != nil {
			return nil, false, err
		}
		fileState := geckodb.GitUploadSessionFile{
			SessionID:  sessionID,
			FileName:   fileName,
			TargetPath: targetPath,
			Size:       manifest.Size,
			Status:     FilePending,
		}
		if exists {
			fileState.Status = FileCollision
			fileState.Error = sql.NullString{String: "target path already exists on base branch", Valid: true}
			hasConflicts = true
		}
		sessionFiles = append(sessionFiles, fileState)
	}
	return sessionFiles, hasConflicts, nil
}

func uploadPreparationError(projectID string, err error) error {
	if statusErr, ok := err.(*domain.HTTPStatusError); ok {
		return domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type(statusErr.Code),
			statusErr.StatusCode,
			statusErr.Message,
			map[string]any{"project_id": projectID},
		)
	}
	return domain.NewTypedError(
		domain.ErrorKindIntegration,
		apierror.Type("integration_error"),
		http.StatusBadGateway,
		fmt.Sprintf("failed to prepare upload session: %s", err),
		map[string]any{"project_id": projectID},
	)
}

func uploadSessionDetails(projectID string, sessionID string) map[string]any {
	return map[string]any{"project_id": projectID, "session_id": sessionID}
}

func databaseError(message string, details map[string]any) error {
	return domain.NewTypedError(domain.ErrorKindDatabase, apierror.TypeDatabaseError, http.StatusInternalServerError, message, details)
}

func conflictError(message string, details map[string]any) error {
	return domain.NewTypedError(domain.ErrorKindValidation, apierror.Type("conflict"), http.StatusConflict, message, details)
}

func invalidRequestError(message string, details map[string]any) error {
	return domain.NewTypedError(domain.ErrorKindValidation, apierror.Type("invalid_request"), http.StatusBadRequest, message, details)
}
