-- Add pending_deletions table for deferred file cleanup

CREATE TABLE IF NOT EXISTS pending_deletions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    delete_after TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_deletions_due ON pending_deletions(delete_after);
