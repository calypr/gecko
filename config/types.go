package config

type Type string

const (
	TypeExplorer    Type = "explorer"
	TypeNav         Type = "nav"
	TypeFileSummary Type = "file_summary"
	TypeAppsPage    Type = "apps_page"
	TypeProject     Type = "project"
	TypeProjects    Type = "projects"

	DefaultConfigID  = "default"
	AppsPageConfigID = "1"
)

func KnownTypes() []string {
	return []string{
		string(TypeExplorer),
		string(TypeNav),
		string(TypeFileSummary),
		string(TypeAppsPage),
		string(TypeProject),
		string(TypeProjects),
	}
}

func IsKnownType(t string) bool {
	switch Type(t) {
	case TypeExplorer, TypeNav, TypeFileSummary, TypeAppsPage, TypeProject, TypeProjects:
		return true
	default:
		return false
	}
}
