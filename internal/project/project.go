package project

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/calypr/gecko/config"
	"github.com/jmoiron/sqlx"
)

// Project is Gecko's project identity and its core metadata. Integrations such
// as Explorer, Git, and Syfon are associated with this identity independently.
type Project struct {
	ID           string
	Organization string
	Name         string
	Config       *config.ProjectConfig
}

func FromID(id string) (Project, bool) {
	organization, name, ok := strings.Cut(id, "/")
	if !ok || strings.TrimSpace(organization) == "" || strings.TrimSpace(name) == "" || strings.Contains(name, "/") {
		return Project{}, false
	}
	return Project{ID: id, Organization: organization, Name: name}, true
}

// ListIdentities reads only the canonical project records for public discovery.
func ListIdentities(ctx context.Context, db *sqlx.DB) ([]Project, error) {
	var ids []string
	if err := db.SelectContext(ctx, &ids, "SELECT name FROM config_schema.projects ORDER BY name"); err != nil {
		return nil, fmt.Errorf("list project identities: %w", err)
	}
	projects := make([]Project, 0, len(ids))
	for _, id := range ids {
		item, ok := FromID(id)
		if ok {
			projects = append(projects, item)
		}
	}
	return projects, nil
}

// List reads the project table itself. Optional or malformed metadata never
// removes an existing project from discovery.
func List(ctx context.Context, db *sqlx.DB) ([]Project, error) {
	var rows []struct {
		Name    string `db:"name"`
		Content []byte `db:"content"`
	}
	if err := db.SelectContext(ctx, &rows, "SELECT name, content FROM config_schema.projects ORDER BY name"); err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	projects := make([]Project, 0, len(rows))
	for _, row := range rows {
		item, ok := FromID(row.Name)
		if !ok {
			continue
		}
		var cfg config.ProjectConfig
		if json.Unmarshal(row.Content, &cfg) == nil {
			item.Config = &cfg
		}
		projects = append(projects, item)
	}
	return projects, nil
}
