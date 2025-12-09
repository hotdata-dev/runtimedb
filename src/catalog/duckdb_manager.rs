use super::TableInfo;
use crate::catalog::manager::{CatalogManager, ConnectionInfo};
use anyhow::Result;
use duckdb::{params, AccessMode, Config, Connection};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub struct DuckdbCatalogManager {
    conn: Arc<Mutex<Option<Connection>>>,
    catalog_path: String,
    readonly: bool,
}

impl DuckdbCatalogManager {
    pub fn new(db_path: &str) -> Result<Self> {
        Self::new_readonly(db_path, false)
    }

    pub fn new_readonly(db_path: &str, readonly: bool) -> Result<Self> {
        let conn = if readonly {
            // Open in readonly mode using Config and AccessMode
            let config = Config::default().access_mode(AccessMode::ReadOnly)?;
            Connection::open_with_flags(db_path, config)?
        } else {
            Connection::open(db_path)?
        };

        // Only initialize schema if not in readonly mode
        // In readonly mode, we expect the database to already exist with proper schema
        if !readonly {
            DuckdbCatalogManager::initialize_schema(&conn)?;
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(Some(conn))),
            catalog_path: db_path.to_string(),
            readonly,
        })
    }

    /// Get connection guard after verifying it's open. Returns error if closed.
    /// Caller can safely unwrap the Option since we've verified it's Some.
    fn get_connection_guard(&self) -> Result<std::sync::MutexGuard<'_, Option<Connection>>> {
        let guard = self.conn.lock().unwrap();
        if guard.is_none() {
            anyhow::bail!("Catalog connection is closed");
        }
        Ok(guard)
    }

    fn initialize_schema(conn: &Connection) -> Result<()> {
        // Create migrations tracking table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        // Create sequences for auto-increment
        conn.execute("CREATE SEQUENCE IF NOT EXISTS connections_seq START 1", [])?;
        conn.execute("CREATE SEQUENCE IF NOT EXISTS tables_seq START 1", [])?;

        // Create connections table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY DEFAULT nextval('connections_seq'),
            name TEXT UNIQUE NOT NULL,
            source_type TEXT NOT NULL,
            config_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
            [],
        )?;

        // Create tables table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tables (
            id INTEGER PRIMARY KEY DEFAULT nextval('tables_seq'),
            connection_id INTEGER NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            parquet_path TEXT,
            state_path TEXT,
            last_sync TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (connection_id) REFERENCES connections(id),
            UNIQUE (connection_id, schema_name, table_name)
        )",
            [],
        )?;

        // Run migrations
        Self::run_migrations(conn)?;

        Ok(())
    }

    fn run_migrations(conn: &Connection) -> Result<()> {
        // Get current schema version
        let current_version: i32 = conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        // Migration 1: Add type field to existing postgres connections
        if current_version < 1 {
            // Update existing connections to have type field in config_json
            // DuckDB doesn't have json_insert, so we use json_merge_patch to add the type field
            conn.execute(
                r#"
                UPDATE connections
                SET config_json = json_merge_patch(config_json, '{"type":"postgres"}')
                WHERE config_json NOT LIKE '%"type":%'
                "#,
                [],
            )?;

            conn.execute("INSERT INTO schema_migrations (version) VALUES (1)", [])?;
        }

        // Migration 2: Add arrow_schema_json column to tables
        if current_version < 2 {
            conn.execute(
                "ALTER TABLE tables ADD COLUMN IF NOT EXISTS arrow_schema_json TEXT",
                [],
            )?;
            conn.execute("INSERT INTO schema_migrations (version) VALUES (2)", [])?;
        }

        Ok(())
    }

    fn table_mapper(row: &duckdb::Row) -> duckdb::Result<TableInfo> {
        Ok(TableInfo {
            id: row.get(0)?,
            connection_id: row.get(1)?,
            schema_name: row.get(2)?,
            table_name: row.get(3)?,
            parquet_path: row.get(4)?,
            state_path: row.get(5)?,
            last_sync: row.get(6)?,
            arrow_schema_json: row.get(7)?,
        })
    }
}

impl CatalogManager for DuckdbCatalogManager {
    /// Close the catalog connection. This is idempotent and can be called multiple times.
    /// After closing, all operations on this catalog will return an error.
    fn close(&self) -> Result<()> {
        let mut conn_guard = self.conn.lock().unwrap();
        if let Some(conn) = conn_guard.as_ref() {
            // Force a checkpoint to ensure all data is written to disk
            let _ = conn.execute("CHECKPOINT", params![])?;
        }
        if let Some(conn) = conn_guard.take() {
            let _ = conn.close();
        }
        Ok(())
    }

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        let mut stmt = conn
            .prepare("SELECT id, name, source_type, config_json FROM connections ORDER BY name")?;

        let rows = stmt.query_map([], |row| {
            Ok(ConnectionInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                source_type: row.get(2)?,
                config_json: row.get(3)?,
            })
        })?;

        let mut connections = Vec::new();
        for conn_result in rows {
            connections.push(conn_result?);
        }

        Ok(connections)
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        if self.readonly {
            anyhow::bail!("Cannot add connection in readonly mode");
        }

        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        conn.execute(
            "INSERT INTO connections (name, source_type, config_json) VALUES (?, ?, ?)",
            params![name, source_type, config_json],
        )?;

        let id: i32 = conn.query_row(
            "SELECT id FROM connections WHERE name = ?",
            params![name],
            |row| row.get(0),
        )?;

        Ok(id)
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        let mut stmt = conn
            .prepare("SELECT id, name, source_type, config_json FROM connections WHERE name = ?")?;

        let mut rows = stmt.query(params![name])?;

        if let Some(row) = rows.next()? {
            Ok(Some(ConnectionInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                source_type: row.get(2)?,
                config_json: row.get(3)?,
            }))
        } else {
            Ok(None)
        }
    }

    fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        if self.readonly {
            anyhow::bail!("Cannot add table in readonly mode");
        }

        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        // Insert or update table with schema
        conn.execute(
            "INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json) VALUES (?, ?, ?, ?)
             ON CONFLICT (connection_id, schema_name, table_name) DO UPDATE SET arrow_schema_json = excluded.arrow_schema_json",
            params![connection_id, schema_name, table_name, arrow_schema_json],
        )?;

        let id: i32 = conn.query_row(
            "SELECT id FROM tables WHERE connection_id = ? AND schema_name = ? AND table_name = ?",
            params![connection_id, schema_name, table_name],
            |row| row.get(0),
        )?;

        Ok(id)
    }

    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        let mut tables = Vec::new();

        if let Some(conn_id) = connection_id {
            let mut stmt = conn.prepare(
                "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                 CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
                 FROM tables WHERE connection_id = ? ORDER BY schema_name, table_name",
            )?;
            let rows = stmt.query_map(params![conn_id], DuckdbCatalogManager::table_mapper)?;

            for table in rows {
                tables.push(table?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                 CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
                 FROM tables ORDER BY schema_name, table_name",
            )?;
            let rows = stmt.query_map([], DuckdbCatalogManager::table_mapper)?;

            for table in rows {
                tables.push(table?);
            }
        }

        Ok(tables)
    }

    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
             CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
             FROM tables WHERE connection_id = ? AND schema_name = ? AND table_name = ?",
        )?;

        let mut rows = stmt.query_map(
            params![connection_id, schema_name, table_name],
            DuckdbCatalogManager::table_mapper,
        )?;
        if let Some(table) = rows.next() {
            Ok(Some(table?))
        } else {
            Ok(None)
        }
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        if self.readonly {
            anyhow::bail!("Cannot update table sync in readonly mode");
        }

        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap(); // Safe: get_connection_guard verified it's Some

        conn.execute(
            "UPDATE tables SET parquet_path = ?, state_path = ?, last_sync = CURRENT_TIMESTAMP WHERE id = ?",
            params![parquet_path, state_path, table_id],
        )?;
        Ok(())
    }

    /// Clear table cache metadata (set paths to NULL) without deleting files.
    /// This should be called before re-registering the catalog to avoid file handle issues.
    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        // Get table info before clearing
        let table_info = self
            .get_table(connection_id, schema_name, table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}.{}' not found", schema_name, table_name))?;

        // Update table entry to NULL out paths and sync time
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap();

        conn.execute(
            "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE id = ?",
            params![table_info.id],
        )?;

        Ok(table_info)
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        if self.readonly {
            anyhow::bail!("Cannot clear connection cache metadata in readonly mode");
        }

        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Update all table entries to NULL out paths and sync time
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap();

        conn.execute(
            "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE connection_id = ?",
            params![conn_info.id],
        )?;

        Ok(())
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        if self.readonly {
            anyhow::bail!("Cannot delete connection in readonly mode");
        }

        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Delete from database
        let conn_guard = self.get_connection_guard()?;
        let conn = conn_guard.as_ref().unwrap();

        conn.execute(
            "DELETE FROM tables WHERE connection_id = ?",
            params![conn_info.id],
        )?;

        conn.execute(
            "DELETE FROM connections WHERE id = ?",
            params![conn_info.id],
        )?;

        Ok(())
    }
}

impl Debug for DuckdbCatalogManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogManager")
            .field("catalog_path", &self.catalog_path)
            .field("conn", &"<DuckDB Connection>")
            .finish()
    }
}
