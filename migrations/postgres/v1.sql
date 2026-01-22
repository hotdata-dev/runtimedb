-- Initial schema for PostgreSQL catalog

CREATE TABLE secrets (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    provider TEXT NOT NULL,
    provider_ref TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE encrypted_secret_values (
    secret_id TEXT PRIMARY KEY REFERENCES secrets(id) ON DELETE CASCADE,
    encrypted_value BYTEA NOT NULL
);

CREATE TABLE connections (
    id SERIAL PRIMARY KEY,
    external_id TEXT UNIQUE NOT NULL,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL,
    config_json TEXT NOT NULL,
    secret_id TEXT REFERENCES secrets(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tables (
    id SERIAL PRIMARY KEY,
    connection_id INTEGER NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    parquet_path TEXT,
    last_sync TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    arrow_schema_json TEXT,
    FOREIGN KEY (connection_id) REFERENCES connections(id),
    UNIQUE (connection_id, schema_name, table_name)
);

-- Pending deletions for deferred file cleanup with retry tracking
CREATE TABLE pending_deletions (
    id SERIAL PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    delete_after TIMESTAMPTZ NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_pending_deletions_due ON pending_deletions(delete_after);

-- Query results persistence table
CREATE TABLE results (
    id TEXT PRIMARY KEY,
    parquet_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_results_created_at ON results(created_at);
