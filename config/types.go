package config

type Type string

const (
	TypeExplorer    Type = "explorer"
	TypeNav         Type = "nav"
	TypeFileSummary Type = "file_summary"
	TypeProject     Type = "project"
	TypeProjects    Type = "projects"

	DefaultConfigID = "default"
)

var knownTypes = []Type{
	TypeExplorer,
	TypeNav,
	TypeFileSummary,
	TypeProject,
	TypeProjects,
}

func KnownTypes() []string {
	types := make([]string, 0, len(knownTypes))
	for _, t := range knownTypes {
		types = append(types, string(t))
	}
	return types
}

func IsKnownType(t string) bool {
	for _, known := range knownTypes {
		if Type(t) == known {
			return true
		}
	}
	return false
}
