-- Migrate to using external_id as the primary connection identifier
-- 1. Update tables.connection_id from integer to text (the external_id value)
-- 2. Drop the internal integer id from connections, rename external_id to id

-- Step 1: Drop FK constraint on tables
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_fkey;
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_schema_name_table_name_key;

-- Step 2: Add temporary column and populate it via join (Postgres doesn't allow subquery in USING)
ALTER TABLE tables ADD COLUMN connection_id_new TEXT;
UPDATE tables t SET connection_id_new = c.external_id FROM connections c WHERE t.connection_id = c.id;
ALTER TABLE tables DROP COLUMN connection_id;
ALTER TABLE tables RENAME COLUMN connection_id_new TO connection_id;
ALTER TABLE tables ALTER COLUMN connection_id SET NOT NULL;

-- Step 3: Drop the old integer primary key from connections and rename external_id to id
ALTER TABLE connections DROP COLUMN id;
ALTER TABLE connections RENAME COLUMN external_id TO id;
ALTER TABLE connections ADD PRIMARY KEY (id);

-- Step 4: Re-add constraints on tables pointing to the renamed connections.id
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_fkey
    FOREIGN KEY (connection_id) REFERENCES connections(id);
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_schema_name_table_name_key
    UNIQUE (connection_id, schema_name, table_name);

-- Add index for efficient lookups by connection_id
CREATE INDEX idx_tables_connection_id ON tables(connection_id);
