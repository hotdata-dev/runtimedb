-- Migrate tables.connection_id from internal integer ID to external text ID
-- Use a join-based approach since USING clause cannot contain subqueries

-- Step 1: Add a temporary column for the new external_id
ALTER TABLE tables ADD COLUMN connection_external_id TEXT;

-- Step 2: Populate the new column via UPDATE with JOIN
UPDATE tables t
SET connection_external_id = c.external_id
FROM connections c
WHERE t.connection_id::integer = c.id;

-- Step 3: Drop the old constraints
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_fkey;
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_schema_name_table_name_key;

-- Step 4: Drop the old column and rename the new one
ALTER TABLE tables DROP COLUMN connection_id;
ALTER TABLE tables RENAME COLUMN connection_external_id TO connection_id;

-- Step 5: Make the column NOT NULL (after data migration)
ALTER TABLE tables ALTER COLUMN connection_id SET NOT NULL;

-- Step 6: Add the new constraints
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_fkey
    FOREIGN KEY (connection_id) REFERENCES connections(external_id);
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_schema_name_table_name_key
    UNIQUE (connection_id, schema_name, table_name);
