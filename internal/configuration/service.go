package configuration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	projectpkg "github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/jmoiron/sqlx"
)

type Service struct {
	db          *sqlx.DB
	repoService *repo.Service
}

func NewService(db *sqlx.DB, repoService *repo.Service) *Service {
	return &Service{db: db, repoService: repoService}
}

func ConfigForType(configType string) (config.Configurable, error) {
	switch configType {
	case string(config.TypeExplorer):
		return &config.Config{}, nil
	case string(config.TypeNav):
		return &config.NavPageLayoutProps{}, nil
	case string(config.TypeFileSummary):
		return &config.FilesummaryConfig{}, nil
	case string(config.TypeProject), string(config.TypeProjects):
		return &config.ProjectConfig{}, nil
	default:
		return nil, domain.NewTypedError(
			domain.ErrorKindValidation,
			apierror.TypeInvalidConfigType,
			http.StatusBadRequest,
			fmt.Sprintf("Unknown config type: %s", configType),
			map[string]any{"config_type": configType},
		)
	}
}

func (service *Service) Get(ctx context.Context, configType string, configID string) (config.Configurable, error) {
	cfg, err := ConfigForType(configType)
	if err != nil {
		return nil, err
	}
	if err := geckodb.ConfigGETGenericContext(ctx, service.db, configID, configType, cfg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewTypedError(
				domain.ErrorKindNotFound,
				apierror.TypeConfigNotFound,
				http.StatusNotFound,
				fmt.Sprintf("no config found with configId: %s of type: %s", configID, configType),
				map[string]any{"config_type": configType, "config_id": configID},
			)
		}
		return nil, configDatabaseError("config query failed", configType, configID, err)
	}
	return cfg, nil
}

func (service *Service) Put(ctx context.Context, configType string, configID string, cfg config.Configurable) error {
	if err := validateConfig(configType, configID, cfg); err != nil {
		return err
	}
	if err := geckodb.ConfigPUTGenericContext(ctx, service.db, configID, configType, cfg); err != nil {
		return configDatabaseError("configPut failed", configType, configID, err)
	}
	if configType != string(config.TypeProjects) {
		return nil
	}
	projectCfg, ok := cfg.(*config.ProjectConfig)
	if !ok || service.repoService == nil {
		return nil
	}
	identity, err := repo.ParseIdentity(projectCfg.SrcRepo)
	if err != nil {
		return nil
	}
	existingState, err := geckodb.GitProjectStateByProjectIDContext(ctx, service.db, configID)
	if err != nil {
		return configDatabaseError("read existing git project state failed", configType, configID, err)
	}
	if existingState != nil &&
		existingState.RepoHost == identity.Host &&
		existingState.RepoOwner == identity.Owner &&
		existingState.RepoName == identity.Repo {
		return nil
	}
	state := geckodb.GitProjectState{
		ProjectID:  configID,
		RepoHost:   identity.Host,
		RepoOwner:  identity.Owner,
		RepoName:   identity.Repo,
		MirrorPath: service.repoService.MirrorPathForIdentity(identity),
		SyncState:  projectpkg.GitSyncNeverSynced,
	}
	if err := geckodb.UpsertGitProjectStateContext(ctx, service.db, state); err != nil {
		return configDatabaseError("upsert git project state failed", configType, configID, err)
	}
	return nil
}

func (service *Service) ListIDs(ctx context.Context, configType string) ([]string, error) {
	ids, err := geckodb.ConfigListByTypeContext(ctx, service.db, configType)
	if err != nil {
		return nil, configDatabaseError("Database error", configType, "", err)
	}
	if ids == nil {
		return []string{}, nil
	}
	return ids, nil
}

func (service *Service) ListProjects(ctx context.Context) ([]projectpkg.Project, error) {
	projects, err := projectpkg.List(ctx, service.db)
	if err != nil {
		return nil, configDatabaseError("Database error", string(config.TypeProjects), "", err)
	}
	return projects, nil
}

func (service *Service) ListProjectIdentities(ctx context.Context) ([]projectpkg.Project, error) {
	projects, err := projectpkg.ListIdentities(ctx, service.db)
	if err != nil {
		return nil, configDatabaseError("Database error", string(config.TypeProjects), "", err)
	}
	return projects, nil
}

func (service *Service) ListProjectsForResources(ctx context.Context, allowedResources []string) ([]projectpkg.Project, error) {
	projects, err := service.ListProjects(ctx)
	if err != nil {
		return nil, err
	}
	allowedIDs := make([]string, 0, len(projects))
	for _, item := range projects {
		allowedIDs = append(allowedIDs, item.ID)
	}
	allowedIDs = access.FilterProjectIDsByAllowedResources(allowedIDs, allowedResources)
	allowed := make(map[string]struct{}, len(allowedIDs))
	for _, id := range allowedIDs {
		allowed[id] = struct{}{}
	}
	filtered := make([]projectpkg.Project, 0, len(allowedIDs))
	for _, item := range projects {
		if _, ok := allowed[item.ID]; ok {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func (service *Service) Delete(ctx context.Context, configType string, configID string) error {
	deleted, err := geckodb.ConfigDELETEGenericContext(ctx, service.db, configID, configType)
	if !deleted && err == nil {
		return domain.NewTypedError(
			domain.ErrorKindNotFound,
			apierror.TypeConfigNotFound,
			http.StatusNotFound,
			fmt.Sprintf("no configId found with configId: %s in type: %s", configID, configType),
			map[string]any{"config_type": configType, "config_id": configID},
		)
	}
	if err != nil {
		return configDatabaseError("config query failed", configType, configID, err)
	}
	return nil
}

func validateConfig(configType string, configID string, cfg config.Configurable) error {
	switch typed := cfg.(type) {
	case *config.ProjectConfig:
		if configType == string(config.TypeProjects) {
			projectOrganization, _, found := strings.Cut(configID, "/")
			if !found {
				return domain.NewTypedError(
					domain.ErrorKindValidation,
					apierror.TypeValidationFailed,
					http.StatusBadRequest,
					fmt.Sprintf("invalid project config id: %s", configID),
					map[string]any{"config_type": configType, "config_id": configID},
				)
			}
			typed.OrgTitle = strings.TrimSpace(projectOrganization)
			if err := typed.ValidateInitialization(); err != nil {
				return validationError(configType, configID, err)
			}
			return nil
		}
		if err := typed.Validate(); err != nil {
			return validationError(configType, configID, err)
		}
	default:
		if validatable, ok := cfg.(interface{ Validate() error }); ok {
			if err := validatable.Validate(); err != nil {
				return validationError(configType, configID, err)
			}
		}
	}
	return nil
}

func validationError(configType string, configID string, err error) error {
	return domain.NewTypedError(
		domain.ErrorKindValidation,
		apierror.TypeValidationFailed,
		http.StatusBadRequest,
		fmt.Sprintf("body data validation failed: %s", err),
		map[string]any{"config_type": configType, "config_id": configID},
	)
}

func configDatabaseError(operation string, configType string, configID string, err error) error {
	return domain.NewTypedError(
		domain.ErrorKindDatabase,
		apierror.TypeDatabaseError,
		http.StatusInternalServerError,
		fmt.Sprintf("%s: %s", operation, err),
		map[string]any{"config_type": configType, "config_id": configID},
	)
}
