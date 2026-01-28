-- Migrate tables.connection_id from internal integer ID to external text ID
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_fkey;
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_schema_name_table_name_key;

ALTER TABLE tables ALTER COLUMN connection_id TYPE TEXT
USING (SELECT external_id FROM connections WHERE connections.id = tables.connection_id::integer);

ALTER TABLE tables ADD CONSTRAINT tables_connection_id_fkey
    FOREIGN KEY (connection_id) REFERENCES connections(external_id);
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_schema_name_table_name_key
    UNIQUE (connection_id, schema_name, table_name);
