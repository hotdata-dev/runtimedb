-- Migrate tables.connection_id from internal integer ID to external text ID
-- SQLite doesn't support ALTER COLUMN, so we recreate the table

CREATE TABLE tables_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    parquet_path TEXT,
    last_sync TIMESTAMP,
    arrow_schema_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (connection_id) REFERENCES connections(external_id),
    UNIQUE (connection_id, schema_name, table_name)
);

INSERT INTO tables_new (id, connection_id, schema_name, table_name, parquet_path, last_sync, arrow_schema_json, created_at)
SELECT t.id, c.external_id, t.schema_name, t.table_name, t.parquet_path, t.last_sync, t.arrow_schema_json, t.created_at
FROM tables t
JOIN connections c ON t.connection_id = c.id;

DROP TABLE tables;
ALTER TABLE tables_new RENAME TO tables;
