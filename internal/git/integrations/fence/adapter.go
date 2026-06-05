package fence

import (
	"context"

	"github.com/calypr/gecko/internal/git"
	gitapp "github.com/calypr/gecko/internal/git/app"
)

type Broker struct {
	service *git.GitService
}

func NewBroker(service *git.GitService) gitapp.FenceBroker {
	return &Broker{service: service}
}

func (broker *Broker) OrganizationInstallationStatus(ctx context.Context, authorizationHeader string, organization string, owner string) (git.GitRepositoryInstallationStatus, error) {
	return broker.service.RequestOrganizationInstallationStatus(ctx, authorizationHeader, organization, owner)
}

func (broker *Broker) InstallationToken(ctx context.Context, authorizationHeader string, organization string, project string, identity git.GitRepositoryIdentity, access string) (string, error) {
	return broker.service.RequestInstallationToken(ctx, authorizationHeader, organization, project, identity, access)
}
