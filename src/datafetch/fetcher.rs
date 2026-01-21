use async_trait::async_trait;

use super::native::StreamingParquetWriter;
use super::{DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Trait for fetching data from remote sources.
///
/// Credentials are resolved internally using `source.credential().resolve(secrets)`.
/// The Source contains the credential reference (secret ID), and fetchers resolve
/// it as needed when connecting to the remote source.
#[async_trait]
pub trait DataFetcher: Send + Sync + std::fmt::Debug {
    /// Discover all tables (with columns) from the remote source.
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,
    ) -> Result<Vec<TableMetadata>, DataFetchError>;

    /// Fetch table data and write to the provided Parquet writer.
    /// The writer is pre-initialized with the destination path.
    /// Driver must call: writer.init(schema) -> writer.write_batch()* (but NOT close())
    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError>;
}
