package thumbnail

// Manager defines the interface for project thumbnail storage and retrieval.
type Manager interface {
	GetPath(organization string, project string) (path string, contentType string, err error)
	Save(organization string, project string, data []byte) (path string, contentType string, err error)
	Delete(organization string, project string) error
	ProjectThumbnailDir(organization string, project string) string
}
