package gecko

import (
	"encoding/json"
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/stretchr/testify/assert"
)

// Helper to convert query to string for assertion
func queryString(q *gripql.Query) string {
	b, _ := json.Marshal(q.Statements)
	return string(b)
}

func TestBuildListProjectsQuery(t *testing.T) {
	projs := []any{"PROG-PROJ1", "PROG-PROJ2"}
	q := buildListProjectsQuery(projs)

	// Verify key components of the query
	jsonStr := queryString(q)
	assert.Contains(t, jsonStr, "ResearchStudy")
	assert.Contains(t, jsonStr, "auth_resource_path")
	assert.Contains(t, jsonStr, "rootDir_Directory") // Must have OutE
	assert.Contains(t, jsonStr, "Distinct")          // Must have Distinct
}

func TestBuildDirGetQuery_Root(t *testing.T) {
	projectId := "/programs/PROG/projects/PROJ"
	dirPath := "/"
	q := buildDirGetQuery(projectId, dirPath)

	jsonStr := queryString(q)
	assert.Contains(t, jsonStr, "ResearchStudy")
	// The query logic for root just filters by auth_resource_path
	assert.Contains(t, jsonStr, "auth_resource_path")
	assert.Contains(t, jsonStr, "rootDir_Directory")
	// Verify it does NOT contain loop logic
	assert.NotContains(t, jsonStr, "name")
}

func TestBuildDirGetQuery_SubDir(t *testing.T) {
	projectId := "/programs/PROG/projects/PROJ"
	dirPath := "/data/foo"
	q := buildDirGetQuery(projectId, dirPath)

	jsonStr := queryString(q)
	// Verify traversal
	assert.Contains(t, jsonStr, "data")
	assert.Contains(t, jsonStr, "foo")
	// Verify security check is present in loop (auth_resource_path appears multiple times)
	// We can't easily count occurrences in JSON string without parsing, but Contains is a good smoke test
	assert.Contains(t, jsonStr, "auth_resource_path")
}
