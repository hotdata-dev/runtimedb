-- Uploads table for pending file uploads
CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    storage_url TEXT NOT NULL,
    content_type TEXT,
    content_encoding TEXT,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed_at TIMESTAMPTZ
);

CREATE INDEX idx_uploads_status ON uploads(status);

-- Datasets table for user-curated data
CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    schema_name TEXT NOT NULL DEFAULT 'default',
    table_name TEXT NOT NULL,
    parquet_url TEXT NOT NULL,
    arrow_schema_json TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_config TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(schema_name, table_name)
);

CREATE INDEX idx_datasets_table_name ON datasets(table_name);
