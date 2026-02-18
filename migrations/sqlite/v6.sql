-- Saved queries with versioned SQL snapshots.
-- SQL text is stored in a content-addressable sql_snapshots table,
-- referenced by ID from saved_query_versions and query_runs.

-- 1) Create sql_snapshots table (content-addressable SQL store)
CREATE TABLE sql_snapshots (
    id TEXT PRIMARY KEY,
    sql_hash TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX ux_sql_snapshots_hash_text ON sql_snapshots (sql_hash, sql_text);
CREATE INDEX idx_sql_snapshots_hash ON sql_snapshots (sql_hash);

-- 2) Create saved_queries table
-- Note: name is intentionally NOT UNIQUE — duplicate names are allowed.
CREATE TABLE saved_queries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    latest_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3) Create saved_query_versions table
CREATE TABLE saved_query_versions (
    saved_query_id TEXT NOT NULL REFERENCES saved_queries(id),
    version INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL REFERENCES sql_snapshots(id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (saved_query_id, version)
);

-- 4) Backfill sql_snapshots from existing query_runs.
-- IDs use the 'snap' prefix + 26 hex chars from randomblob, matching the
-- 30-char length of runtime nanoid-based IDs. The hex charset (0-9a-f) is
-- narrower than the full nanoid alphabet but functionally equivalent since
-- IDs are opaque.
INSERT INTO sql_snapshots (id, sql_hash, sql_text, created_at)
SELECT
    'snap' || lower(hex(randomblob(13))),
    sql_hash,
    sql_text,
    MIN(created_at)
FROM query_runs
WHERE sql_text IS NOT NULL
GROUP BY sql_hash, sql_text;

-- 5) Recreate query_runs with snapshot_id, without sql_text/sql_hash
CREATE TABLE query_runs_new (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES sql_snapshots(id),
    trace_id TEXT,
    status TEXT NOT NULL,
    result_id TEXT,
    error_message TEXT,
    warning_message TEXT,
    row_count BIGINT,
    execution_time_ms BIGINT,
    saved_query_id TEXT,
    saved_query_version INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

INSERT INTO query_runs_new (
    id, snapshot_id, trace_id, status, result_id,
    error_message, warning_message, row_count, execution_time_ms,
    saved_query_id, saved_query_version, created_at, completed_at
)
SELECT
    qr.id,
    s.id,
    qr.trace_id, qr.status, qr.result_id,
    qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms,
    NULL, NULL, qr.created_at, qr.completed_at
FROM query_runs qr
JOIN sql_snapshots s ON s.sql_hash = qr.sql_hash AND s.sql_text = qr.sql_text;

DROP TABLE query_runs;
ALTER TABLE query_runs_new RENAME TO query_runs;

-- Recreate indexes on query_runs
CREATE INDEX idx_query_runs_created_id ON query_runs(created_at DESC, id DESC);
CREATE INDEX idx_query_runs_trace_id ON query_runs(trace_id);
CREATE INDEX idx_query_runs_saved_query_id ON query_runs(saved_query_id);
CREATE INDEX idx_query_runs_snapshot_id ON query_runs(snapshot_id);
