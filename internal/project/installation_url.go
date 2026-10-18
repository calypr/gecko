package project

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/repo"
)

func NormalizeInstallationRepository(repositoryFullName string, repositories []domain.GitHubInstallationRepository, organization string, project string) (repo.Identity, bool, error) {
	requested := strings.TrimSpace(repositoryFullName)
	for _, repository := range repositories {
		if !strings.EqualFold(repository.FullName, requested) {
			continue
		}
		repoURL := strings.TrimSpace(repository.CloneURL)
		if repoURL == "" {
			repoURL = strings.TrimSpace(repository.HTMLURL)
		}
		if repoURL == "" {
			repoURL = "https://github.com/" + strings.TrimSpace(repository.FullName)
		}
		identity, err := repo.ParseIdentity(repoURL)
		if err != nil {
			return repo.Identity{}, false, domain.NewTypedError(
				domain.ErrorKindIntegration,
				apierror.Type("integration_error"),
				http.StatusBadGateway,
				fmt.Sprintf("failed to normalize installation repository %q: %s", repository.FullName, err),
				map[string]any{"organization": organization, "project": project, "repository": repository.FullName},
			)
		}
		return identity, true, nil
	}
	return repo.Identity{}, false, nil
}

func ParseRequestedRepositoryIdentity(repositoryFullName string) (repo.Identity, error) {
	repoURL := strings.TrimSpace(repositoryFullName)
	if !strings.HasPrefix(repoURL, "http://") && !strings.HasPrefix(repoURL, "https://") && !strings.HasPrefix(repoURL, "git@") {
		repoURL = "https://github.com/" + repoURL
	}
	return repo.ParseIdentity(repoURL)
}

func DecorateInstallationRedirectURL(redirectURL string, targetID int64, repoID int64) string {
	hasNew := strings.Contains(redirectURL, "/installations/new")
	hasSelectTarget := strings.Contains(redirectURL, "/installations/select_target")
	if !hasNew && !hasSelectTarget {
		return redirectURL
	}
	if hasNew {
		redirectURL = strings.Replace(redirectURL, "/installations/new", "/installations/new/permissions", 1)
	} else {
		redirectURL = strings.Replace(redirectURL, "/installations/select_target", "/installations/new/permissions", 1)
	}
	separator := "?"
	if strings.Contains(redirectURL, "?") {
		separator = "&"
	}
	redirectURL = fmt.Sprintf("%s%ssuggested_target_id=%d", redirectURL, separator, targetID)
	return fmt.Sprintf("%s&repository_ids[]=%d", redirectURL, repoID)
}

func AppInstallationRedirectURL(redirectURL string, installationID int64) string {
	if installationID <= 0 || strings.TrimSpace(redirectURL) == "" {
		return ""
	}
	request, err := http.NewRequest(http.MethodGet, redirectURL, nil)
	if err != nil || request.URL == nil {
		return ""
	}
	pathParts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(pathParts) < 2 || pathParts[0] != "apps" || strings.TrimSpace(pathParts[1]) == "" {
		return ""
	}
	return fmt.Sprintf("%s://%s/apps/%s/installations/%d", request.URL.Scheme, request.URL.Host, pathParts[1], installationID)
}
