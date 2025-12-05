mod parquet_writer;

pub use parquet_writer::StreamingParquetWriter;

use async_trait::async_trait;

use crate::datafetch::{ConnectionConfig, DataFetchError, DataFetcher, TableMetadata};
use crate::storage::StorageManager;

/// Native Rust driver-based data fetcher
#[derive(Debug, Default)]
pub struct NativeFetcher;

impl NativeFetcher {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataFetcher for NativeFetcher {
    async fn discover_tables(
        &self,
        config: &ConnectionConfig,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        match config.source_type.as_str() {
            "duckdb" | "motherduck" => todo!("DuckDB discovery"),
            "postgres" => todo!("PostgreSQL discovery"),
            other => Err(DataFetchError::UnsupportedDriver(other.to_string())),
        }
    }

    async fn fetch_table(
        &self,
        config: &ConnectionConfig,
        _catalog: Option<&str>,
        _schema: &str,
        _table: &str,
        _storage: &dyn StorageManager,
        _connection_id: i32,
    ) -> Result<String, DataFetchError> {
        match config.source_type.as_str() {
            "duckdb" | "motherduck" => todo!("DuckDB fetch"),
            "postgres" => todo!("PostgreSQL fetch"),
            other => Err(DataFetchError::UnsupportedDriver(other.to_string())),
        }
    }
}