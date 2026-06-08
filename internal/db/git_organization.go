package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/calypr/gecko/internal/giturl"
	"github.com/jmoiron/sqlx"
)

func GitOrganizationStateByOrganization(db *sqlx.DB, organization string) (*GitOrganizationState, error) {
	return GitOrganizationStateByOrganizationContext(context.Background(), db, organization)
}

func GitOrganizationStateByOrganizationContext(ctx context.Context, db *sqlx.DB, organization string) (*GitOrganizationState, error) {
	if db == nil {
		return nil, nil
	}
	var state GitOrganizationState
	err := db.GetContext(ctx, &state, `SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema.git_organization_state WHERE organization = $1`, organization)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	normalizeGitOrganizationStateHTMLURL(&state)
	return &state, nil
}

func UpsertGitOrganizationState(db *sqlx.DB, state GitOrganizationState) error {
	if db == nil {
		return nil
	}
	normalizeGitOrganizationStateHTMLURL(&state)
	_, err := db.NamedExec(`
		INSERT INTO config_schema.git_organization_state (
			organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error
		) VALUES (
			:organization, :installed, :installation_id, :installation_target_type, :installation_target, :html_url, :repository_selection, :configured_at, :last_seen_at, :updated_at, :last_error
		)
		ON CONFLICT (organization) DO UPDATE SET
			installed = EXCLUDED.installed,
			installation_id = EXCLUDED.installation_id,
			installation_target_type = EXCLUDED.installation_target_type,
			installation_target = EXCLUDED.installation_target,
			html_url = EXCLUDED.html_url,
			repository_selection = EXCLUDED.repository_selection,
			configured_at = EXCLUDED.configured_at,
			last_seen_at = EXCLUDED.last_seen_at,
			updated_at = EXCLUDED.updated_at,
			last_error = EXCLUDED.last_error;
	`, state)
	if err != nil {
		return fmt.Errorf("upsert git organization state: %w", err)
	}
	return nil
}

func ListGitOrganizationStates(db *sqlx.DB) (map[string]GitOrganizationState, error) {
	states := []GitOrganizationState{}
	if err := db.Select(&states, `SELECT organization, installed, installation_id, installation_target_type, installation_target, html_url, repository_selection, configured_at, last_seen_at, updated_at, last_error FROM config_schema.git_organization_state`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]GitOrganizationState{}, nil
		}
		return nil, err
	}
	indexed := make(map[string]GitOrganizationState, len(states))
	for _, state := range states {
		normalizeGitOrganizationStateHTMLURL(&state)
		indexed[state.Organization] = state
	}
	return indexed, nil
}

func normalizeGitOrganizationStateHTMLURL(state *GitOrganizationState) {
	if state == nil || !state.HTMLURL.Valid {
		return
	}
	normalized := giturl.NormalizeInstallationHTMLURL(state.HTMLURL.String)
	if normalized == "" {
		state.HTMLURL = sql.NullString{}
		return
	}
	state.HTMLURL = sql.NullString{String: normalized, Valid: true}
}
