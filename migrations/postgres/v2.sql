-- Add pending_deletions table for deferred file cleanup

CREATE TABLE IF NOT EXISTS pending_deletions (
    id SERIAL PRIMARY KEY,
    path TEXT NOT NULL,
    delete_after TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_deletions_due ON pending_deletions(delete_after);
