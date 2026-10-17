package fence

import (
	"net/http"

	"github.com/calypr/gecko/internal/domain"
)

type Config struct {
	BaseURL string
}

type Client struct {
	client *http.Client
	config Config
}

type fenceGitHubInstallURLResponse struct {
	InstallURL string `json:"install_url"`
}

type fenceGitHubInstallationStatusResponse struct {
	Installed           bool   `json:"installed"`
	InstallationID      *int64 `json:"installation_id"`
	Target              string `json:"target"`
	TargetType          string `json:"target_type"`
	HTMLURL             string `json:"html_url"`
	RepositorySelection string `json:"repository_selection"`
}

type fenceGitHubInstallationRepositoriesResponse struct {
	Repositories []domain.GitHubInstallationRepository `json:"repositories"`
}

type fenceGitHubTokenResponse struct {
	Token string `json:"token"`
}
