package apiserver

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apgen/server/gecko"
	"github.com/calypr/gecko/internal/repo"
	"github.com/go-git/go-git/v5/plumbing"
)

func (s *Server) GetGitProjectPointerInventory(ctx context.Context, request gecko.GetGitProjectPointerInventoryRequestObject) (gecko.GetGitProjectPointerInventoryResponseObject, error) {
	organization, project := strings.TrimSpace(request.OrgTitle), strings.TrimSpace(request.ProjectTitle)
	if err := s.requireToken(ctx); err != nil {
		return nil, err
	}
	if s.Authz == nil {
		return nil, newOpError(http.StatusServiceUnavailable, "authorization_unavailable", "Project authorization is unavailable", nil)
	}
	if err := s.projectReadAuthz(ctx, organization, project); err != nil {
		return nil, err
	}
	commit := request.Params.Commit
	if _, err := hex.DecodeString(commit); len(commit) != 40 || err != nil {
		return nil, newOpError(http.StatusBadRequest, "invalid_request", "commit must be a full Git commit SHA", nil)
	}
	resolved, opErr := s.resolveGitProject(ctx, organization, project)
	if opErr != nil {
		return nil, opErr
	}
	mirror, err := s.ProjectRead.OpenMirror(ctx, AuthorizationFromContext(ctx), resolved, false)
	if err != nil {
		return nil, fromAppError(err)
	}
	if mirror.Empty {
		return nil, newOpError(http.StatusNotFound, "not_found", "Commit is not in the project repository", nil)
	}
	revision, err := mirror.Repository.CommitObject(plumbing.NewHash(strings.ToLower(commit)))
	if errors.Is(err, plumbing.ErrObjectNotFound) {
		return nil, newOpError(http.StatusNotFound, "not_found", "Commit is not in the project repository", nil)
	}
	if err != nil {
		return nil, err
	}
	tree, err := revision.Tree()
	if err != nil {
		return nil, err
	}
	return pointerInventoryResponse{walk: func(emit func(repo.DataPointer) error) error {
		return repo.WalkDataPointers(ctx, mirror.Repository, tree, emit)
	}}, nil
}

type pointerInventoryResponse struct {
	walk func(func(repo.DataPointer) error) error
}

func (response pointerInventoryResponse) VisitGetGitProjectPointerInventoryResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	first := true
	encoder := json.NewEncoder(w)
	if err := response.walk(func(pointer repo.DataPointer) error {
		if !first {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		first = false
		return encoder.Encode(pointer)
	}); err != nil {
		return err
	}
	_, err := io.WriteString(w, "]")
	return err
}
