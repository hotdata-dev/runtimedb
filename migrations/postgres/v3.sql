-- Migrate tables.connection_id from internal integer ID to external text ID

-- Step 1: Drop the existing constraints
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_fkey;
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_schema_name_table_name_key;

-- Step 2: Convert the column type, looking up external_id for each row
ALTER TABLE tables
    ALTER COLUMN connection_id TYPE TEXT
    USING (SELECT external_id FROM connections WHERE id = connection_id);

-- Step 3: Re-add constraints pointing to external_id
ALTER TABLE tables ALTER COLUMN connection_id SET NOT NULL;
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_fkey
    FOREIGN KEY (connection_id) REFERENCES connections(external_id);
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_schema_name_table_name_key
    UNIQUE (connection_id, schema_name, table_name);

-- Add index for efficient lookups by connection_id
CREATE INDEX idx_tables_connection_id ON tables(connection_id);
