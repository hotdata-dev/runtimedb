-- Add status column and make parquet_path nullable for async persistence tracking
-- Status values: 'processing', 'ready', 'failed'
-- SQLite doesn't support ALTER COLUMN, so we recreate the table

-- Step 1: Create new results table with nullable parquet_path and status column
CREATE TABLE results_new (
    id TEXT PRIMARY KEY,
    parquet_path TEXT,  -- Now nullable for 'processing' results
    status TEXT NOT NULL DEFAULT 'ready',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Copy existing data (all existing results are 'ready')
INSERT INTO results_new (id, parquet_path, status, created_at)
SELECT id, parquet_path, 'ready', created_at FROM results;

-- Step 3: Drop old table and rename new one
DROP TABLE results;
ALTER TABLE results_new RENAME TO results;

-- Step 4: Recreate indexes
CREATE INDEX idx_results_created_at ON results(created_at);
CREATE INDEX idx_results_status ON results(status);
