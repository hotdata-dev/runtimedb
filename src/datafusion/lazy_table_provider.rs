use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

use crate::catalog::CatalogManager;
use crate::datafetch::DataFetcher;
use crate::source::Source;
use crate::storage::StorageManager;

/// A lazy table provider that defers data fetching until scan() is called.
///
/// This provider stores the table schema from catalog metadata and only triggers
/// the actual data fetch when a query scan is executed. This enables efficient
/// query planning without unnecessary I/O operations.
#[derive(Debug)]
pub struct LazyTableProvider {
    schema: SchemaRef,
    fetcher: Arc<dyn DataFetcher>,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    storage: Arc<dyn StorageManager>,
    connection_id: i32,
    schema_name: String,
    table_name: String,
}

impl LazyTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        fetcher: Arc<dyn DataFetcher>,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        storage: Arc<dyn StorageManager>,
        connection_id: i32,
        schema_name: String,
        table_name: String,
    ) -> Self {
        Self {
            schema,
            fetcher,
            source,
            catalog,
            storage,
            connection_id,
            schema_name,
            table_name,
        }
    }

    /// Load a parquet file and return an ExecutionPlan.
    /// Uses the catalog schema (self.schema) to ensure consistency with schema()
    /// and proper projection index alignment.
    async fn load_parquet_exec(
        &self,
        parquet_url: &str,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Parse the URL - DataFusion handles both file:// and s3://
        let table_path = ListingTableUrl::parse(parquet_url)
            .map_err(|e| DataFusionError::External(format!("Failed to parse URL: {}", e).into()))?;

        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format));

        // Use the catalog schema (self.schema) to ensure consistency.
        // This ensures projection indices from DataFusion align correctly since
        // they're based on the schema returned by schema().
        // The Parquet reader will match columns by name.
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(self.schema.clone());

        let table = ListingTable::try_new(config).map_err(|e| {
            DataFusionError::External(format!("Failed to create ListingTable: {}", e).into())
        })?;

        // Create the scan execution plan with projection, filter, and limit pushdown
        table.scan(state, projection, filters, limit).await
    }

    /// Fetch the table data and update catalog
    async fn fetch_and_cache(&self) -> Result<String, DataFusionError> {
        use crate::datafetch::native::StreamingParquetWriter;

        // Prepare cache write location
        let write_path = self.storage.prepare_cache_write(
            self.connection_id,
            &self.schema_name,
            &self.table_name,
        );

        // Create writer
        let mut writer = StreamingParquetWriter::new(write_path.clone());

        // Fetch the table data into writer using the Source directly
        self.fetcher
            .fetch_table(
                &self.source,
                None, // catalog
                &self.schema_name,
                &self.table_name,
                &mut writer,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to fetch table: {}", e).into())
            })?;

        // Close writer
        writer.close().map_err(|e| {
            DataFusionError::External(format!("Failed to close writer: {}", e).into())
        })?;

        // Finalize cache write (uploads to S3 if needed, returns URL)
        let parquet_url = self
            .storage
            .finalize_cache_write(
                &write_path,
                self.connection_id,
                &self.schema_name,
                &self.table_name,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to finalize cache write: {}", e).into())
            })?;

        // Update catalog with new path
        if let Ok(Some(info)) = self
            .catalog
            .get_table(self.connection_id, &self.schema_name, &self.table_name)
            .await
        {
            let _ = self
                .catalog
                .update_table_sync(info.id, &parquet_url, "")
                .await;
        }

        Ok(parquet_url)
    }
}

#[async_trait]
impl TableProvider for LazyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Return schema immediately without any I/O
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Check if table is already cached
        let table_info = self
            .catalog
            .get_table(self.connection_id, &self.schema_name, &self.table_name)
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to get table info: {}", e).into())
            })?
            .ok_or_else(|| {
                DataFusionError::External("Table not found in catalog".to_string().into())
            })?;

        let parquet_url = if let Some(path) = table_info.parquet_path {
            // Already cached, use existing path
            if path.starts_with("file://") || path.starts_with("s3://") {
                path
            } else {
                format!("file://{}", path)
            }
        } else {
            // Not cached, fetch now
            self.fetch_and_cache().await?
        };

        // Load the parquet file and create execution plan with projection, filter, and limit pushdown
        self.load_parquet_exec(&parquet_url, state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Parquet supports filter pushdown
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
