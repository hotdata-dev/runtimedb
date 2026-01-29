-- Migrate to using external_id as the primary connection identifier
-- 1. Update tables.connection_id from integer to text (the external_id value)
-- 2. Drop the internal integer id from connections, rename external_id to id
-- SQLite doesn't support ALTER COLUMN or DROP COLUMN, so we recreate both tables

-- Step 1: Create new connections table with TEXT id (from external_id)
CREATE TABLE connections_new (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL,
    config_json TEXT NOT NULL,
    secret_id TEXT REFERENCES secrets(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO connections_new (id, name, source_type, config_json, secret_id, created_at)
SELECT external_id, name, source_type, config_json, secret_id, created_at
FROM connections;

-- Step 2: Create new tables with FK to connections_new
CREATE TABLE tables_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    parquet_path TEXT,
    last_sync TIMESTAMP,
    arrow_schema_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (connection_id) REFERENCES connections_new(id),
    UNIQUE (connection_id, schema_name, table_name)
);

INSERT INTO tables_new (id, connection_id, schema_name, table_name, parquet_path, last_sync, arrow_schema_json, created_at)
SELECT t.id, c.external_id, t.schema_name, t.table_name, t.parquet_path, t.last_sync, t.arrow_schema_json, t.created_at
FROM tables t
JOIN connections c ON c.id = t.connection_id;

-- Step 3: Drop old tables and rename new ones
DROP TABLE tables;
DROP TABLE connections;
ALTER TABLE connections_new RENAME TO connections;
ALTER TABLE tables_new RENAME TO tables;

-- Add index for efficient lookups by connection_id
CREATE INDEX idx_tables_connection_id ON tables(connection_id);
