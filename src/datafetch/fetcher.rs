use async_trait::async_trait;

use super::native::StreamingParquetWriter;
use super::{DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Trait for fetching data from remote sources
#[async_trait]
pub trait DataFetcher: Send + Sync + std::fmt::Debug {
    /// Discover all tables (with columns) from the remote source.
    /// The secret_id is used to look up the credential in the SecretManager.
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,
        secret_id: Option<&str>,
    ) -> Result<Vec<TableMetadata>, DataFetchError>;

    /// Fetch table data and write to the provided Parquet writer.
    /// The writer is pre-initialized with the destination path.
    /// Driver must call: writer.init(schema) -> writer.write_batch()* (but NOT close())
    /// The secret_id is used to look up the credential in the SecretManager.
    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,
        secret_id: Option<&str>,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError>;
}
