package project

import (
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/domain"
)

type StorageBucket = domain.StorageBucket

type StorageConfig = domain.StorageConfig

type CalyprProjectStorageIntent struct {
	Bucket              string `json:"bucket"`
	Provider            string `json:"provider"`
	Endpoint            string `json:"endpoint"`
	Region              string `json:"region"`
	AccessKey           string `json:"access_key"`
	SecretKey           string `json:"secret_key"`
	Organization        string `json:"organization"`
	ProjectID           string `json:"project_id"`
	Path                string `json:"path,omitempty"`
	PathPrefix          string `json:"path_prefix,omitempty"`
	OrganizationSubPath string `json:"organization_sub_path,omitempty"`
	ProjectSubPath      string `json:"project_sub_path,omitempty"`
}

type CalyprProjectSetupRequest struct {
	Config  appconfig.ProjectConfig     `json:"config"`
	Storage *CalyprProjectStorageIntent `json:"storage,omitempty"`
}

type CalyprProjectInitializeResponse struct {
	Success      bool   `json:"success"`
	ProjectID    string `json:"project_id"`
	ResourcePath string `json:"resource_path"`
}

type CalyprProjectStorageRequest struct {
	Storage *CalyprProjectStorageIntent `json:"storage"`
}

type CalyprProjectStorageResponse struct {
	Success      bool                    `json:"success"`
	ProjectID    string                  `json:"project_id"`
	ResourcePath string                  `json:"resource_path"`
	Storage      ProjectIntegrationCheck `json:"storage"`
}

type CalyprReadinessCheck struct {
	Pass    bool   `json:"pass"`
	Reason  string `json:"reason,omitempty"`
	Details string `json:"details,omitempty"`
}

type CalyprProjectReadiness struct {
	Git    CalyprReadinessCheck `json:"git"`
	Syfon  CalyprReadinessCheck `json:"syfon"`
	Config CalyprReadinessCheck `json:"config"`
}

type ProjectIntegrationCheck struct {
	Pass    bool   `json:"pass"`
	Reason  string `json:"reason,omitempty"`
	Details string `json:"details,omitempty"`
}

type ProjectIntegrationStatus struct {
	GitHub  ProjectIntegrationCheck `json:"github"`
	Storage ProjectIntegrationCheck `json:"storage"`
}
