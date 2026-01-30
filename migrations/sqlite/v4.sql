-- Add status column to results table for async persistence tracking
-- Status values: 'processing', 'ready', 'failed'
ALTER TABLE results ADD COLUMN status TEXT NOT NULL DEFAULT 'ready';

-- Add index for filtering by status (e.g., finding processing results)
CREATE INDEX idx_results_status ON results(status);
