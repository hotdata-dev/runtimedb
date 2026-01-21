mod duckdb;
mod iceberg;
mod mysql;
mod parquet_writer;
mod postgres;
mod snowflake;

pub use parquet_writer::StreamingParquetWriter;

use async_trait::async_trait;

use crate::datafetch::{DataFetchError, DataFetcher, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Native Rust driver-based data fetcher
#[derive(Debug, Default)]
pub struct NativeFetcher;

impl NativeFetcher {
    pub fn new() -> Self {
        Self
    }
}

/// Resolve the secret for a source from the secret manager.
/// Returns the secret value (password, token, etc.) or None if not applicable.
async fn resolve_secret(
    _source: &Source,
    secrets: &SecretManager,
    secret_id: Option<&str>,
) -> Result<Option<String>, DataFetchError> {
    // If no secret_id provided, the source doesn't require a secret
    let id = match secret_id {
        Some(id) => id,
        None => return Ok(None),
    };

    let value = secrets
        .get_string_by_id(id)
        .await
        .map_err(|e| DataFetchError::Connection(format!("Failed to resolve secret: {}", e)))?;

    Ok(Some(value))
}

#[async_trait]
impl DataFetcher for NativeFetcher {
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,
        secret_id: Option<&str>,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        let secret = resolve_secret(source, secrets, secret_id).await?;

        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::discover_tables(source, secret.as_deref()).await
            }
            Source::Postgres { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for Postgres".to_string())
                })?;
                postgres::discover_tables(source, &password).await
            }
            Source::Iceberg { .. } => {
                // Iceberg: REST may optionally use a token, Glue uses IAM (no secret needed)
                // If the catalog requires authentication, it will fail with a clear error
                iceberg::discover_tables(source, secret.as_deref()).await
            }
            Source::Mysql { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for MySQL".to_string())
                })?;
                mysql::discover_tables(source, &password).await
            }
            Source::Snowflake { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for Snowflake".to_string())
                })?;
                snowflake::discover_tables(source, &password).await
            }
        }
    }

    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,
        secret_id: Option<&str>,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError> {
        let secret = resolve_secret(source, secrets, secret_id).await?;

        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::fetch_table(source, secret.as_deref(), catalog, schema, table, writer).await
            }
            Source::Postgres { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for Postgres".to_string())
                })?;
                postgres::fetch_table(source, &password, catalog, schema, table, writer).await
            }
            Source::Iceberg { .. } => {
                // Iceberg: REST may optionally use a token, Glue uses IAM (no secret needed)
                // If the catalog requires authentication, it will fail with a clear error
                iceberg::fetch_table(source, secret.as_deref(), catalog, schema, table, writer)
                    .await
            }
            Source::Mysql { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for MySQL".to_string())
                })?;
                mysql::fetch_table(source, &password, catalog, schema, table, writer).await
            }
            Source::Snowflake { .. } => {
                let password = secret.ok_or_else(|| {
                    DataFetchError::Connection("Password required for Snowflake".to_string())
                })?;
                snowflake::fetch_table(source, &password, catalog, schema, table, writer).await
            }
        }
    }
}
