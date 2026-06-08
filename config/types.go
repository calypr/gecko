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

func KnownTypes() []string {
	return []string{
		string(TypeExplorer),
		string(TypeNav),
		string(TypeFileSummary),
		string(TypeProject),
		string(TypeProjects),
	}
}

func IsKnownType(t string) bool {
	switch Type(t) {
	case TypeExplorer, TypeNav, TypeFileSummary, TypeProject, TypeProjects:
		return true
	default:
		return false
	}
}
