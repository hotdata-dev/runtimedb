use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ConnectionInfo {
    pub id: i32,
    pub name: String,
    pub source_type: String,
    pub config_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TableInfo {
    pub id: i32,
    pub connection_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_path: Option<String>,
    pub state_path: Option<String>,
    pub last_sync: Option<String>,
    pub arrow_schema_json: Option<String>,
}

pub trait CatalogManager: Debug + Send + Sync {
    /// Close the catalog connection. This is idempotent and can be called multiple times.
    /// After closing, all operations on this catalog will return an error.
    fn close(&self) -> Result<()>;

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>>;
    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32>;
    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>>;
    fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32>;
    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>>;
    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>>;
    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()>;

    /// Clear table cache metadata (set paths to NULL) without deleting files.
    /// This should be called before re-registering the catalog to avoid file handle issues.
    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo>;

    /// Clear cache metadata for all tables in a connection (set paths to NULL).
    /// This is metadata-only; file deletion should be handled by the engine.
    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()>;

    /// Delete connection and all associated table rows from metadata.
    /// This is metadata-only; file deletion should be handled by the engine.
    fn delete_connection(&self, name: &str) -> Result<()>;
}
