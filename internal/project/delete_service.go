package project

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/jmoiron/sqlx"
)

type DeleteService struct {
	db         *sqlx.DB
	setup      *SetupService
	thumbnails thumbnail.Manager
}

func NewDeleteService(db *sqlx.DB, setup *SetupService, thumbnails thumbnail.Manager) *DeleteService {
	return &DeleteService{db: db, setup: setup, thumbnails: thumbnails}
}

func (service *DeleteService) DeleteProject(ctx context.Context, authorizationHeader string, organization string, project string, configID string) error {
	resourcePath := access.ProgramProjectResourcePath(organization, project)
	log.Printf("starting project delete workflow: config_id=%s resource_path=%s organization=%s project=%s", configID, resourcePath, organization, project)
	if service.setup != nil {
		if err := service.setup.CleanupProjectStorage(ctx, authorizationHeader, organization, project); err != nil {
			log.Printf("project delete failed during storage cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v", configID, resourcePath, organization, project, err)
			return err
		}
	}
	if err := geckodb.DeleteGitProjectArtifacts(service.db, configID); err != nil {
		log.Printf("project delete failed during gecko git artifact cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v", configID, resourcePath, organization, project, err)
		return domain.WrapError(
			domain.ErrorKindDatabase,
			http.StatusInternalServerError,
			"failed during project deletion step gecko_git_artifact_cleanup",
			err,
			map[string]any{
				"config_type":   string(config.TypeProjects),
				"config_id":     configID,
				"organization":  organization,
				"project":       project,
				"resource_path": resourcePath,
				"delete_step":   "gecko_git_artifact_cleanup",
			},
		)
	}
	if service.thumbnails != nil {
		if err := service.thumbnails.Delete(organization, project); err != nil && !errors.Is(err, thumbnail.ErrNoThumbnail) {
			log.Printf("project delete failed during thumbnail cleanup: config_id=%s resource_path=%s organization=%s project=%s err=%v", configID, resourcePath, organization, project, err)
			return domain.WrapError(
				domain.ErrorKindIntegration,
				http.StatusInternalServerError,
				"failed during project deletion step thumbnail_cleanup",
				err,
				map[string]any{
					"config_type":   string(config.TypeProjects),
					"config_id":     configID,
					"organization":  organization,
					"project":       project,
					"resource_path": resourcePath,
					"delete_step":   "thumbnail_cleanup",
				},
			)
		}
	}
	if err := DeleteAuthzResource(ctx, authorizationHeader, resourcePath); err != nil {
		log.Printf("project delete failed during arborist resource delete: config_id=%s resource_path=%s organization=%s project=%s err=%v", configID, resourcePath, organization, project, err)
		return domain.WrapError(
			domain.ErrorKindIntegration,
			http.StatusBadGateway,
			"failed during project deletion step arborist_resource_delete",
			err,
			map[string]any{
				"config_type":    string(config.TypeProjects),
				"config_id":      configID,
				"organization":   organization,
				"project":        project,
				"resource_path":  resourcePath,
				"delete_step":    "arborist_resource_delete",
				"upstream_error": err.Error(),
			},
		)
	}
	log.Printf("project delete downstream cleanup succeeded: config_id=%s resource_path=%s organization=%s project=%s", configID, resourcePath, organization, project)
	return nil
}

func (service *DeleteService) DeleteOrganization(ctx context.Context, authorizationHeader string, organization string) error {
	projectIDs, err := geckodb.ConfigListByTypeContext(ctx, service.db, string(config.TypeProjects))
	if err != nil {
		return domain.NewTypedError(
			domain.ErrorKindDatabase,
			apierror.TypeDatabaseError,
			http.StatusInternalServerError,
			fmt.Sprintf("failed to list projects for organization delete: %s", err),
			map[string]any{"organization": organization},
		)
	}
	for _, projectID := range projectIDs {
		projectOrganization, projectName, found := strings.Cut(projectID, "/")
		if !found || strings.TrimSpace(projectOrganization) != organization {
			continue
		}
		projectName = strings.TrimSpace(projectName)
		if projectName == "" {
			continue
		}
		if err := service.DeleteProject(ctx, authorizationHeader, organization, projectName, projectID); err != nil {
			return err
		}
		if _, err := geckodb.ConfigDELETEGenericContext(ctx, service.db, projectID, string(config.TypeProjects)); err != nil {
			return domain.NewTypedError(
				domain.ErrorKindDatabase,
				apierror.TypeDatabaseError,
				http.StatusInternalServerError,
				fmt.Sprintf("failed to delete project config %s during organization delete: %s", projectID, err),
				map[string]any{"organization": organization, "project_id": projectID},
			)
		}
	}
	resourcePath := fmt.Sprintf("/programs/%s", organization)
	if err := DeleteAuthzResource(ctx, authorizationHeader, resourcePath); err != nil {
		return domain.NewTypedError(
			domain.ErrorKindIntegration,
			apierror.Type("integration_error"),
			http.StatusBadGateway,
			fmt.Sprintf("failed to delete arborist organization resource: %s", err),
			map[string]any{"organization": organization, "resource_path": resourcePath},
		)
	}
	return nil
}
