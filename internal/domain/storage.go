package domain

type StorageBucket struct {
	Bucket    string
	Provider  string
	Endpoint  string
	Region    string
	Resources []string
}

type StorageBucketScope struct {
	Bucket       string
	Organization string
	ProjectID    string
	Path         string
}

type StorageConfig struct {
	Bucket              string
	Provider            string
	Endpoint            string
	Region              string
	AccessKey           string
	SecretKey           string
	Organization        string
	ProjectID           string
	Path                string
	PathPrefix          string
	OrganizationSubPath string
	ProjectSubPath      string
}
