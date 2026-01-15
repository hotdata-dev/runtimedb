use anyhow::Result;
use std::sync::Arc;

use super::native::StreamingParquetWriter;
use super::{DataFetchError, DataFetcher, TableMetadata};
use crate::catalog::CatalogManager;
use crate::secrets::SecretManager;
use crate::source::Source;
use crate::storage::StorageManager;

/// Orchestrates the full table fetch workflow: fetch from source → write to storage → update catalog.
#[derive(Debug)]
pub struct FetchOrchestrator {
    fetcher: Arc<dyn DataFetcher>,
    storage: Arc<dyn StorageManager>,
    catalog: Arc<dyn CatalogManager>,
    secret_manager: Arc<SecretManager>,
}

impl FetchOrchestrator {
    pub fn new(
        fetcher: Arc<dyn DataFetcher>,
        storage: Arc<dyn StorageManager>,
        catalog: Arc<dyn CatalogManager>,
        secret_manager: Arc<SecretManager>,
    ) -> Self {
        Self {
            fetcher,
            storage,
            catalog,
            secret_manager,
        }
    }

    /// Fetch table data from source, write to cache storage, and update catalog metadata.
    ///
    /// Returns the URL of the cached parquet file.
    pub async fn cache_table(
        &self,
        source: &Source,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<String> {
        // Prepare cache write location
        let write_path = self
            .storage
            .prepare_cache_write(connection_id, schema_name, table_name);

        // Create writer
        let mut writer = StreamingParquetWriter::new(write_path.clone());

        // Fetch the table data into writer
        self.fetcher
            .fetch_table(
                source,
                &self.secret_manager,
                None, // catalog
                schema_name,
                table_name,
                &mut writer,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch table: {}", e))?;

        // Close writer
        writer
            .close()
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

        // Finalize cache write (uploads to S3 if needed, returns URL)
        let parquet_url = self
            .storage
            .finalize_cache_write(&write_path, connection_id, schema_name, table_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finalize cache write: {}", e))?;

        // Update catalog with new path
        if let Ok(Some(info)) = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await
        {
            let _ = self.catalog.update_table_sync(info.id, &parquet_url).await;
        }

        Ok(parquet_url)
    }

    /// Discover tables from a remote source.
    /// Delegates to the underlying fetcher.
    pub async fn discover_tables(
        &self,
        source: &Source,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        self.fetcher
            .discover_tables(source, &self.secret_manager)
            .await
    }

    /// Refresh table data with atomic swap semantics.
    /// Writes to new versioned path, then atomically updates catalog.
    /// Returns (new_url, old_path).
    pub async fn refresh_table(
        &self,
        source: &Source,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(String, Option<String>)> {
        // 1. Get current path (will be deleted after grace period)
        let old_info = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await?;
        let old_path = old_info.as_ref().and_then(|i| i.parquet_path.clone());

        // 2. Generate versioned new path
        let new_local_path =
            self.storage
                .prepare_versioned_cache_write(connection_id, schema_name, table_name);

        // 3. Fetch and write to new path
        let mut writer = StreamingParquetWriter::new(new_local_path.clone());
        self.fetcher
            .fetch_table(
                source,
                &self.secret_manager,
                None,
                schema_name,
                table_name,
                &mut writer,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch table: {}", e))?;

        // 4. Close writer
        writer
            .close()
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

        // 5. Finalize (upload to S3 if needed)
        let new_url = self
            .storage
            .finalize_cache_write(&new_local_path, connection_id, schema_name, table_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finalize cache write: {}", e))?;

        // 6. Atomic catalog update
        if let Some(info) = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await?
        {
            let _ = self.catalog.update_table_sync(info.id, &new_url).await;
        }

        Ok((new_url, old_path))
    }
}
