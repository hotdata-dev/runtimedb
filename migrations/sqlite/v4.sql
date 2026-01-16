-- Add retry_count column for tracking failed deletion attempts

ALTER TABLE pending_deletions ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
