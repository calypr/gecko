#!/bin/bash

# Start PostgreSQL service
brew services start postgresql

# Wait for PostgreSQL to start (adjust sleep time if needed)
sleep 5

psql postgres <<EOF

ALTER USER postgres WITH PASSWORD 'your_strong_password';

CREATE DATABASE testdb;

GRANT ALL PRIVILEGES ON DATABASE testdb TO postgres;

\c testdb;

DO \$$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'config_schema') THEN
        CREATE SCHEMA config_schema;
    END IF;
END \$$;

CREATE OR REPLACE FUNCTION create_config_table(schema_name TEXT, table_name TEXT)
RETURNS void AS \$$
BEGIN
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.%I (
            name VARCHAR(255) PRIMARY KEY,
            content JSONB
        );
    ', schema_name, table_name);
END;
\$\$ LANGUAGE plpgsql;

DO \$$
DECLARE
    config_tables TEXT[] := ARRAY['explorer', 'nav', 'file_summary', 'apps_page'];
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY config_tables
    LOOP
        PERFORM create_config_table('config_schema', table_name);
        RAISE NOTICE 'Table %.% created successfully.', 'config_schema', table_name;
    END LOOP;
END
\$$ LANGUAGE plpgsql;

DROP FUNCTION create_config_table(TEXT, TEXT);
\q
EOF
echo "Database initialization complete."
