-- Add error_message column to track why results failed
ALTER TABLE results ADD COLUMN error_message TEXT;
