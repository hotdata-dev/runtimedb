-- Add indexes table for storing projection/sorted index metadata
CREATE TABLE indexes (
    id SERIAL PRIMARY KEY,
    connection_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    sort_columns TEXT NOT NULL,  -- JSON array of column names
    parquet_path TEXT,           -- Path to sorted projection parquet
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (connection_id) REFERENCES connections(id) ON DELETE CASCADE,
    UNIQUE (connection_id, schema_name, table_name, index_name)
);

CREATE INDEX idx_indexes_table ON indexes(connection_id, schema_name, table_name);
