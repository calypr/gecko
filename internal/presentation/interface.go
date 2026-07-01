package presentation

// Manager defines the interface for project presentation HTML storage and retrieval.
type Manager interface {
	Get(organization string, project string) (string, error)
	Save(organization string, project string, content string) error
	ProjectPresentationPath(organization string, project string) string
}
