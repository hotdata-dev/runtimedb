mod arrow_convert;
mod duckdb;
mod parquet_writer;
mod postgres;

pub use parquet_writer::StreamingParquetWriter;

use async_trait::async_trait;

use crate::datafetch::{DataFetchError, DataFetcher, TableMetadata};
use crate::source::Source;

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
    async fn discover_tables(&self, source: &Source) -> Result<Vec<TableMetadata>, DataFetchError> {
        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::discover_tables(source).await
            }
            Source::Postgres { .. } => postgres::discover_tables(source).await,
            Source::Snowflake { .. } => Err(DataFetchError::UnsupportedDriver("Snowflake")),
        }
    }

    async fn fetch_table(
        &self,
        source: &Source,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError> {
        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::fetch_table(source, catalog, schema, table, writer).await
            }
            Source::Postgres { .. } => {
                postgres::fetch_table(source, catalog, schema, table, writer).await
            }
            Source::Snowflake { .. } => Err(DataFetchError::UnsupportedDriver("Snowflake")),
        }
    }
}
