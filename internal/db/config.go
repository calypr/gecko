package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const ConfigSchema = "config_schema"

// Document is the generic structure for configuration items in any table.
// Note: 'Name' maps to 'configId' in the request logic.
type Document struct {
	Name    string          `db:"name"`
	Content json.RawMessage `db:"content"` // Store JSON as raw bytes
}

// ConfigListByType fetches the list of all 'name' (configId) values from a specific table (configType).
func ConfigListByType(db *sqlx.DB, configType string) ([]string, error) {
	var names []string
	// NOTE: configType is validated in the handler against a fixed list, making this safe.
	stmt := fmt.Sprintf("SELECT name FROM %s.%s", ConfigSchema, configType)
	err := db.Select(&names, stmt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("error fetching config names from table %s: %w", configType, err)
	}
	return names, nil
}

// DocumentByIDAndTable fetches the Document struct (ID, Name, Content) by name (configId) from a specific table (configType).
// Returns nil, nil if no rows are found.
func DocumentByIDAndTable(db *sqlx.DB, configId string, configType string) (*Document, error) {
	return DocumentByIDAndTableContext(context.Background(), db, configId, configType)
}

func DocumentByIDAndTableContext(ctx context.Context, db *sqlx.DB, configId string, configType string) (*Document, error) {
	if db == nil {
		return nil, nil
	}
	stmt := fmt.Sprintf("SELECT name, content FROM %s.%s WHERE name=$1", ConfigSchema, configType)
	doc := &Document{}

	err := db.GetContext(ctx, doc, stmt, configId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Standard 404 case (config ID not found)
		}

		// Check for "relation does not exist" error code (SQLSTATE 42P01)
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			// Treat non-existent table (bad configType) as a 404 (resource not found)
			return nil, nil
			// NOTE: The handler logic for nil/nil means "no configs found for this type",
			// which can be treated as a 404.
		}

		// All other errors are true 500s
		return nil, fmt.Errorf("error fetching document from table %s: %w", configType, err)
	}
	return doc, nil
}

// ConfigGETGeneric fetches a document and unmarshals its JSON content into the 'target' struct.
// 'target' must be a pointer to the configuration struct (e.g., *config.AppsPageConfig).
func ConfigGETGeneric(db *sqlx.DB, configId string, configType string, target any) error {
	return ConfigGETGenericContext(context.Background(), db, configId, configType, target)
}

func ConfigGETGenericContext(ctx context.Context, db *sqlx.DB, configId string, configType string, target any) error {
	doc, err := DocumentByIDAndTableContext(ctx, db, configId, configType)
	if err != nil {
		return err
	}
	if doc == nil {
		return sql.ErrNoRows
	}
	err = json.Unmarshal(doc.Content, target)
	if err != nil {
		return fmt.Errorf("error unmarshalling content for %s from table %s: %w", configId, configType, err)
	}
	return nil
}

// ConfigPUTGeneric marshals 'data' (any Go struct) and performs an INSERT or UPDATE (upsert) in the specified table.
func ConfigPUTGeneric(db *sqlx.DB, configId string, configType string, data any) error {
	return ConfigPUTGenericContext(context.Background(), db, configId, configType, data)
}

func ConfigPUTGenericTx(tx *sqlx.Tx, configId string, configType string, data any) error {
	return ConfigPUTGenericTxContext(context.Background(), tx, configId, configType, data)
}

func ConfigPUTGenericContext(ctx context.Context, db *sqlx.DB, configId string, configType string, data any) error {
	if db == nil {
		return nil
	}
	return configPutGenericContext(ctx, db.ExecContext, configId, configType, data)
}

func ConfigPUTGenericTxContext(ctx context.Context, tx *sqlx.Tx, configId string, configType string, data any) error {
	if tx == nil {
		return nil
	}
	return configPutGenericContext(ctx, tx.ExecContext, configId, configType, data)
}

func configPutGenericContext(ctx context.Context, execFn func(context.Context, string, ...any) (sql.Result, error), configId string, configType string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshalling data for %s: %w", configId, err)
	}

	// NOTE: configType is validated in the handler against a fixed list, making this safe.
	stmt := fmt.Sprintf(`
		INSERT INTO %s.%s (name, content)
		VALUES ($1, $2)
		ON CONFLICT (name)
		DO UPDATE SET content = $2;
	`, ConfigSchema, configType)

	// $1 is 'configId', $2 is 'jsonData'
	_, err = execFn(ctx, stmt, configId, jsonData)
	if err != nil {
		return fmt.Errorf("error executing PUT for %s in table %s: %w", configId, configType, err)
	}
	return nil
}

// ConfigDELETEGeneric deletes a document by name (configId) from the specified table (configType).
// Returns true if deleted, false if not found, or an error.
func ConfigDELETEGeneric(db *sqlx.DB, configId string, configType string) (bool, error) {
	// Check existence first
	doc, err := DocumentByIDAndTable(db, configId, configType)
	if err != nil {
		return false, err
	}
	if doc == nil {
		return false, nil // Not found
	}

	// Delete the document
	// NOTE: configType is validated in the handler against a fixed list, making this safe.
	deleteStmt := fmt.Sprintf("DELETE FROM %s.%s WHERE name=$1", ConfigSchema, configType)
	_, err = db.Exec(deleteStmt, configId)
	if err != nil {
		return false, fmt.Errorf("error executing DELETE for %s in table %s: %w", configId, configType, err)
	}
	return true, nil
}
