-- Initial schema for PostgreSQL catalog

CREATE TABLE connections (
    id SERIAL PRIMARY KEY,
    external_id TEXT UNIQUE NOT NULL,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL,
    config_json TEXT NOT NULL,
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

CREATE TABLE secrets (
    name TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_ref TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE encrypted_secret_values (
    name TEXT PRIMARY KEY,
    encrypted_value BYTEA NOT NULL
);
