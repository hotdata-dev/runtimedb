use async_trait::async_trait;

use super::native::StreamingParquetWriter;
use super::{DataFetchError, TableMetadata};

/// Configuration for connecting to a remote data source
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub source_type: String,
    pub connection_string: String,
}

/// Trait for fetching data from remote sources
#[async_trait]
pub trait DataFetcher: Send + Sync + std::fmt::Debug {
    /// Discover all tables (with columns) from the remote source
    async fn discover_tables(
        &self,
        config: &ConnectionConfig,
    ) -> Result<Vec<TableMetadata>, DataFetchError>;

    /// Fetch table data and write to the provided Parquet writer.
    /// The writer is pre-initialized with the destination path.
    /// Driver must call: writer.init(schema) -> writer.write_batch()* (but NOT close())
    async fn fetch_table(
        &self,
        config: &ConnectionConfig,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError>;
}