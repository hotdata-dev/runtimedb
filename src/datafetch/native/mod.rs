mod arrow_convert;
mod duckdb;
mod parquet_writer;
mod postgres;

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
            "duckdb" | "motherduck" => duckdb::discover_tables(config).await,
            "postgres" => postgres::discover_tables(config).await,
            other => Err(DataFetchError::UnsupportedDriver(other.to_string())),
        }
    }

    async fn fetch_table(
        &self,
        config: &ConnectionConfig,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        storage: &dyn StorageManager,
        connection_id: i32,
    ) -> Result<String, DataFetchError> {
        match config.source_type.as_str() {
            "duckdb" | "motherduck" => {
                duckdb::fetch_table(config, catalog, schema, table, storage, connection_id).await
            }
            "postgres" => {
                postgres::fetch_table(config, catalog, schema, table, storage, connection_id).await
            }
            other => Err(DataFetchError::UnsupportedDriver(other.to_string())),
        }
    }
}