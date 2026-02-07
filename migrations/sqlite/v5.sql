-- Create query_runs table for query execution history
-- Status values: 'running', 'succeeded', 'failed'

CREATE TABLE query_runs (
    id TEXT PRIMARY KEY,
    sql_text TEXT NOT NULL,
    sql_hash TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    trace_id TEXT,
    status TEXT NOT NULL,
    result_id TEXT,
    error_message TEXT,
    warning_message TEXT,
    row_count BIGINT,
    execution_time_ms BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Keyset pagination index: newest first with tie-breaking on id
CREATE INDEX idx_query_runs_created_id ON query_runs(created_at DESC, id DESC);

-- Index for future grouping by sql_hash
CREATE INDEX idx_query_runs_sql_hash_created ON query_runs(sql_hash, created_at DESC);

-- Index for trace_id lookups
CREATE INDEX idx_query_runs_trace_id ON query_runs(trace_id);
