use async_trait::async_trait;

use super::batch_writer::BatchWriter;
use super::{DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Trait for fetching data from remote sources
#[async_trait]
pub trait DataFetcher: Send + Sync + std::fmt::Debug {
    /// Discover all tables (with columns) from the remote source
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,
    ) -> Result<Vec<TableMetadata>, DataFetchError>;

    /// Fetch table data and write to the provided writer.
    /// The writer is pre-initialized with the destination path.
    /// Driver must call: writer.init(schema) -> writer.write_batch()* (but NOT close())
    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut dyn BatchWriter,
    ) -> Result<(), DataFetchError>;

    /// Check connectivity to the remote source.
    /// Returns Ok(()) if the connection is healthy, or an error describing the failure.
    async fn check_health(
        &self,
        source: &Source,
        secrets: &SecretManager,
    ) -> Result<(), DataFetchError>;
}
