package db

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestNormalizeGitOrganizationStateHTMLURL(t *testing.T) {
	testCases := []struct {
		name     string
		input    sql.NullString
		expected sql.NullString
	}{
		{
			name:     "empty repository_ids",
			input:    sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=", Valid: true},
			expected: sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42", Valid: true},
		},
		{
			name:     "bracket repository_ids value",
			input:    sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=%5B%5D", Valid: true},
			expected: sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42", Valid: true},
		},
		{
			name:     "bracketed repository_ids key",
			input:    sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids%5B%5D=", Valid: true},
			expected: sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42", Valid: true},
		},
		{
			name:     "real repository id preserved",
			input:    sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=123", Valid: true},
			expected: sql.NullString{String: "https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=123", Valid: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			state := GitOrganizationState{HTMLURL: testCase.input}
			normalizeGitOrganizationStateHTMLURL(&state)
			if state.HTMLURL != testCase.expected {
				t.Fatalf("unexpected html url: %#v", state.HTMLURL)
			}
		})
	}
}

func TestListGitOrganizationStatesNormalizesStoredHTMLURL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	mock.ExpectQuery(`SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema\.git_organization_state`).
		WillReturnRows(sqlmock.NewRows([]string{
			"organization",
			"installed",
			"installation_id",
			"installation_target_type",
			"installation_target",
			"html_url",
			"repository_selection",
			"configured_at",
			"last_seen_at",
			"updated_at",
			"last_error",
		}).AddRow(
			"TEST",
			true,
			42,
			"Organization",
			"EllrottLab",
			"https://github.com/organizations/EllrottLab/settings/installations/42?repository_ids=",
			"selected",
			now,
			now,
			now,
			nil,
		))

	states, err := ListGitOrganizationStates(sqlx.NewDb(db, "sqlmock"))
	if err != nil {
		t.Fatalf("list git organization states: %v", err)
	}
	state, ok := states["TEST"]
	if !ok {
		t.Fatalf("expected TEST organization state, got %#v", states)
	}
	if !state.HTMLURL.Valid {
		t.Fatalf("expected html url to remain valid, got %#v", state.HTMLURL)
	}
	if state.HTMLURL.String != "https://github.com/organizations/EllrottLab/settings/installations/42" {
		t.Fatalf("unexpected html url: %q", state.HTMLURL.String)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
