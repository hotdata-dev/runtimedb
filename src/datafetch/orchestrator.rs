use anyhow::Result;
use std::sync::Arc;

use super::native::StreamingParquetWriter;
use super::DataFetcher;
use crate::catalog::CatalogManager;
use crate::source::Source;
use crate::storage::StorageManager;

/// Orchestrates the full table fetch workflow: fetch from source → write to storage → update catalog.
#[derive(Debug)]
pub struct FetchOrchestrator {
    fetcher: Arc<dyn DataFetcher>,
    storage: Arc<dyn StorageManager>,
    catalog: Arc<dyn CatalogManager>,
}

impl FetchOrchestrator {
    pub fn new(
        fetcher: Arc<dyn DataFetcher>,
        storage: Arc<dyn StorageManager>,
        catalog: Arc<dyn CatalogManager>,
    ) -> Self {
        Self {
            fetcher,
            storage,
            catalog,
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
}
