-- Add status column, error_message, and make parquet_path nullable for async persistence tracking
-- Status values: 'processing', 'ready', 'failed'

-- Make parquet_path nullable (for 'processing' results that don't have a path yet)
ALTER TABLE results ALTER COLUMN parquet_path DROP NOT NULL;

-- Add status column
ALTER TABLE results ADD COLUMN status TEXT NOT NULL DEFAULT 'ready';

-- Add error_message column to track why results failed
ALTER TABLE results ADD COLUMN error_message TEXT;

-- Add index for filtering by status
CREATE INDEX idx_results_status ON results(status);
