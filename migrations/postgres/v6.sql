-- Saved queries with versioned SQL snapshots.
-- SQL text is stored in a content-addressable sql_snapshots table,
-- referenced by ID from saved_query_versions and query_runs.

-- 1) Create sql_snapshots table (content-addressable SQL store)
CREATE TABLE sql_snapshots (
    id TEXT PRIMARY KEY,
    sql_hash TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ux_sql_snapshots_hash_text ON sql_snapshots (sql_hash, sql_text);
CREATE INDEX idx_sql_snapshots_hash ON sql_snapshots (sql_hash);

-- 2) Create saved_queries table
-- Note: name is intentionally NOT UNIQUE — duplicate names are allowed.
CREATE TABLE saved_queries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    latest_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) Create saved_query_versions table
CREATE TABLE saved_query_versions (
    saved_query_id TEXT NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL REFERENCES sql_snapshots(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (saved_query_id, version)
);

-- 4) Backfill sql_snapshots from existing query_runs.
-- IDs use the 'snap' prefix + 26 hex chars from md5, matching the 30-char
-- length of runtime nanoid-based IDs. The hex charset (0-9a-f) is narrower
-- than the full nanoid alphabet but functionally equivalent since IDs are
-- opaque. random() provides uniqueness without requiring pgcrypto.
INSERT INTO sql_snapshots (id, sql_hash, sql_text, created_at)
SELECT
    'snap' || substr(md5(sql_hash || sql_text || random()::text), 1, 26),
    sql_hash,
    sql_text,
    MIN(created_at)
FROM query_runs
WHERE sql_text IS NOT NULL
GROUP BY sql_hash, sql_text;

-- 5) Add snapshot_id to query_runs and populate
ALTER TABLE query_runs ADD COLUMN snapshot_id TEXT;

UPDATE query_runs qr
SET snapshot_id = s.id
FROM sql_snapshots s
WHERE s.sql_hash = qr.sql_hash AND s.sql_text = qr.sql_text;

-- Make snapshot_id NOT NULL and add FK
ALTER TABLE query_runs ALTER COLUMN snapshot_id SET NOT NULL;
ALTER TABLE query_runs
    ADD CONSTRAINT fk_qr_snapshot FOREIGN KEY (snapshot_id) REFERENCES sql_snapshots(id);

-- 6) Drop old columns from query_runs
ALTER TABLE query_runs DROP COLUMN sql_text;
ALTER TABLE query_runs DROP COLUMN sql_hash;
DROP INDEX IF EXISTS idx_query_runs_sql_hash_created;

-- 7) Add new indexes
ALTER TABLE query_runs ADD COLUMN saved_query_id TEXT;
ALTER TABLE query_runs ADD COLUMN saved_query_version INTEGER;
CREATE INDEX idx_query_runs_saved_query_id ON query_runs(saved_query_id);
CREATE INDEX idx_query_runs_snapshot_id ON query_runs(snapshot_id);
