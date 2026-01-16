-- Add UNIQUE constraint on path to prevent duplicate deletion records

CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_deletions_path ON pending_deletions(path);
