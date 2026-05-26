package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/apierror"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

func (handler *Handler) ensureConnectedMirrorProject(projectID string, identity git.GitRepositoryIdentity) (*geckodb.GitProjectState, *httputil.ErrorResponse) {
	state, err := handler.loadGitProjectState(projectID, identity)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read git project state: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	if state == nil || state.InstallationID.Valid == false {
		response := httputil.NewError("conflict", "project is not connected to the GitHub App", http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	if strings.TrimSpace(state.MirrorPath) == "" {
		response := httputil.NewError("conflict", "project mirror is not ready", http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return nil, response
	}
	return state, nil
}

func sessionFilesFromManifest(sessionID string, subdirectory string, baseBranch string, files []git.GitUploadSessionFileManifest, mirrorState *geckodb.GitProjectState) ([]geckodb.GitUploadSessionFile, bool, error) {
	openedRepo, err := git.OpenRepository(mirrorState.MirrorPath)
	if err != nil {
		return nil, false, err
	}
	refName, hash, err := git.ResolveGitReference(openedRepo, baseBranch, mirrorState.DefaultBranch.String)
	if err != nil {
		return nil, false, err
	}
	_ = refName
	sessionFiles := make([]geckodb.GitUploadSessionFile, 0, len(files))
	hasConflicts := false
	seenPaths := make(map[string]struct{}, len(files))
	for _, manifest := range files {
		fileName, err := git.NormalizeGitUploadFileName(manifest.Name)
		if err != nil {
			return nil, false, err
		}
		targetPath := git.BuildGitUploadTargetPath(subdirectory, fileName)
		targetPath = strings.Trim(strings.TrimSpace(targetPath), "/")
		if _, ok := seenPaths[targetPath]; ok {
			sessionFiles = append(sessionFiles, geckodb.GitUploadSessionFile{
				SessionID:  sessionID,
				FileName:   fileName,
				TargetPath: targetPath,
				Size:       manifest.Size,
				Status:     git.GitUploadFileCollision,
				Error:      sql.NullString{String: "duplicate target path in upload batch", Valid: true},
			})
			hasConflicts = true
			continue
		}
		seenPaths[targetPath] = struct{}{}
		exists, err := git.GitPathExistsInRef(openedRepo, hash, targetPath)
		if err != nil {
			return nil, false, err
		}
		fileState := geckodb.GitUploadSessionFile{
			SessionID:  sessionID,
			FileName:   fileName,
			TargetPath: targetPath,
			Size:       manifest.Size,
			Status:     git.GitUploadFilePending,
		}
		if exists {
			fileState.Status = git.GitUploadFileCollision
			fileState.Error = sql.NullString{String: "target path already exists on base branch", Valid: true}
			hasConflicts = true
		}
		sessionFiles = append(sessionFiles, fileState)
	}
	return sessionFiles, hasConflicts, nil
}

func (handler *Handler) handleGitProjectUploadSessionPOST(ctx fiber.Ctx) error {
	organization, project, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	state, errResponse := handler.ensureConnectedMirrorProject(projectID, identity)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	var requestBody git.GitUploadSessionCreateRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	baseBranch := strings.TrimSpace(requestBody.BaseBranch)
	if baseBranch == "" {
		baseBranch = strings.TrimSpace(state.DefaultBranch.String)
	}
	if baseBranch == "" {
		response := httputil.NewError("conflict", "repository has no default branch yet. Initialize your repo first before uploading files.", http.StatusConflict, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if len(requestBody.Files) == 0 {
		response := httputil.NewError("invalid_request", "at least one file is required", http.StatusBadRequest, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	targetSubdir := git.NormalizeGitUploadSubdirectory(requestBody.TargetSubdir)
	sessionID := uuid.NewString()
	files, hasConflicts, err := sessionFilesFromManifest(sessionID, targetSubdir, baseBranch, requestBody.Files, state)
	if err != nil {
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to prepare upload session: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
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
		BranchName:   git.BuildGitUploadBranchName(project),
		PRTitle:      git.BuildDefaultUploadPRTitle(project, len(requestBody.Files)),
		PRBody:       git.BuildDefaultUploadPRBody(baseBranch, targetSubdir),
		Status:       git.GitUploadSessionPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if hasConflicts {
		session.Status = git.GitUploadSessionPending
	}
	if err := geckodb.UpsertGitUploadSession(handler.db, session); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist upload session: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := geckodb.ReplaceGitUploadSessionFiles(handler.db, session.ID, files); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist upload session files: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": session.ID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(git.BuildGitUploadSessionResponse(session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) resolveGitUploadSession(projectID string, sessionID string) (*geckodb.GitUploadSession, []geckodb.GitUploadSessionFile, *httputil.ErrorResponse) {
	session, err := geckodb.GitUploadSessionByID(handler.db, sessionID)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read upload session: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return nil, nil, response
	}
	if session == nil || session.ProjectID != projectID {
		response := httputil.NewError("not_found", "upload session was not found", http.StatusNotFound, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return nil, nil, response
	}
	files, err := geckodb.ListGitUploadSessionFiles(handler.db, sessionID)
	if err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to read upload session files: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return nil, nil, response
	}
	return session, files, nil
}

func (handler *Handler) handleGitProjectUploadSessionGET(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	session, files, errResponse := handler.resolveGitUploadSession(projectID, sessionID)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	return httputil.JSON(git.BuildGitUploadSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUploadSessionFilesPOST(ctx fiber.Ctx) error {
	_, _, projectID, _, _, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	session, files, errResponse := handler.resolveGitUploadSession(projectID, sessionID)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	var requestBody git.GitUploadSessionAttachFilesRequest
	if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID, "session_id": sessionID}); errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	fileMap := make(map[string]*geckodb.GitUploadSessionFile, len(files))
	for i := range files {
		fileMap[files[i].TargetPath] = &files[i]
	}
	for _, attachment := range requestBody.Files {
		targetPath := strings.Trim(strings.TrimSpace(attachment.TargetPath), "/")
		fileState, ok := fileMap[targetPath]
		if !ok {
			response := httputil.NewError("invalid_request", fmt.Sprintf("upload session does not contain target path %s", targetPath), http.StatusBadRequest, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		fileState.Size = attachment.Size
		fileState.Checksum = sql.NullString{String: strings.ToLower(strings.TrimSpace(attachment.Checksum)), Valid: strings.TrimSpace(attachment.Checksum) != ""}
		fileState.DRSObjectID = sql.NullString{String: strings.TrimSpace(attachment.DRSObjectID), Valid: strings.TrimSpace(attachment.DRSObjectID) != ""}
		if fileState.Status != git.GitUploadFileCollision {
			fileState.Status = git.GitUploadFileUploaded
			fileState.Error = sql.NullString{}
		}
	}
	session.Status = git.GitUploadSessionReady
	for _, file := range files {
		if file.Status == git.GitUploadFileCollision {
			session.Status = git.GitUploadSessionPending
			break
		}
		if !file.Checksum.Valid || !file.DRSObjectID.Valid {
			session.Status = git.GitUploadSessionPending
		}
	}
	session.UpdatedAt = time.Now().UTC()
	if err := geckodb.UpsertGitUploadSession(handler.db, *session); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to update upload session: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	if err := geckodb.ReplaceGitUploadSessionFiles(handler.db, sessionID, files); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to update upload session files: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(git.BuildGitUploadSessionResponse(*session, files), http.StatusOK).Write(ctx)
}

func (handler *Handler) handleGitProjectUploadSessionFinalizePOST(ctx fiber.Ctx) error {
	_, _, projectID, _, identity, errResponse := handler.resolveGitProject(ctx)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		response := httputil.NewError("missing_authorization", tokenErr.Error(), http.StatusUnauthorized, map[string]any{"project_id": projectID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	sessionID := strings.TrimSpace(ctx.Params("sessionID"))
	session, files, errResponse := handler.resolveGitUploadSession(projectID, sessionID)
	if errResponse != nil {
		return errResponse.Write(ctx)
	}
	var requestBody git.GitUploadSessionFinalizeRequest
	if len(ctx.Body()) > 0 {
		if errResponse := httputil.ParseJSONBody(ctx.Body(), &requestBody, map[string]any{"project_id": projectID, "session_id": sessionID}); errResponse != nil {
			errResponse.WriteLog(handler.logger)
			return errResponse.Write(ctx)
		}
	}
	if strings.TrimSpace(requestBody.PRTitle) != "" {
		session.PRTitle = strings.TrimSpace(requestBody.PRTitle)
	}
	if strings.TrimSpace(requestBody.PRBody) != "" {
		session.PRBody = strings.TrimSpace(requestBody.PRBody)
	}
	for _, file := range files {
		if file.Status == git.GitUploadFileCollision {
			response := httputil.NewError("conflict", "upload session contains target path conflicts", http.StatusConflict, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		if !file.Checksum.Valid || !file.DRSObjectID.Valid {
			response := httputil.NewError("conflict", fmt.Sprintf("upload session file %s is not fully attached", file.TargetPath), http.StatusConflict, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	commitSHA, prURL, err := handler.gitService.CreateGitHubUploadPullRequest(finalizeCtx, authorizationHeader, identity, session.BaseBranch, session.BranchName, session.PRTitle, session.PRBody, files)
	if err != nil {
		if statusErr, ok := err.(*git.HTTPStatusError); ok {
			response := httputil.NewError(apierror.Type(statusErr.Code), statusErr.Message, statusErr.StatusCode, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
			response.WriteLog(handler.logger)
			return response.Write(ctx)
		}
		response := httputil.NewError("integration_error", fmt.Sprintf("failed to create upload pull request: %s", err), http.StatusBadGateway, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	session.Status = git.GitUploadSessionFinalized
	session.CommitSHA = sql.NullString{String: commitSHA, Valid: commitSHA != ""}
	session.PullRequestURL = sql.NullString{String: prURL, Valid: prURL != ""}
	session.UpdatedAt = time.Now().UTC()
	if err := geckodb.UpsertGitUploadSession(handler.db, *session); err != nil {
		response := httputil.NewError(apierror.TypeDatabaseError, fmt.Sprintf("failed to persist finalized upload session: %s", err), http.StatusInternalServerError, map[string]any{"project_id": projectID, "session_id": sessionID}, nil)
		response.WriteLog(handler.logger)
		return response.Write(ctx)
	}
	return httputil.JSON(git.BuildGitUploadSessionResponse(*session, files), http.StatusOK).Write(ctx)
}
