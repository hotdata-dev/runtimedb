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
use crate::datafetch::FetchOrchestrator;
use crate::source::Source;

/// A lazy table provider that defers data fetching until scan() is called.
///
/// This provider stores the table schema from catalog metadata and only triggers
/// the actual data fetch when a query scan is executed. This enables efficient
/// query planning without unnecessary I/O operations.
#[derive(Debug)]
pub struct LazyTableProvider {
    schema: SchemaRef,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
    connection_id: String,
    schema_name: String,
    table_name: String,
}

impl LazyTableProvider {
    pub fn new(
        schema: SchemaRef,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
        connection_id: String,
        schema_name: String,
        table_name: String,
    ) -> Self {
        Self {
            schema,
            source,
            catalog,
            orchestrator,
            connection_id,
            schema_name,
            table_name,
        }
    }

    /// Load a parquet file and return an ExecutionPlan.
    /// Uses the catalog schema (self.schema) to ensure consistency with schema()
    /// and proper projection index alignment.
    #[tracing::instrument(
        name = "load_parquet_exec",
        skip(self, state, projection, filters, limit),
        fields(runtimedb.parquet_url = %parquet_url)
    )]
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
        let (url, _row_count) = self
            .orchestrator
            .cache_table(
                &self.source,
                &self.connection_id,
                &self.schema_name,
                &self.table_name,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to cache table: {}", e).into())
            })?;
        Ok(url)
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

    #[tracing::instrument(
        name = "lazy_table_scan",
        skip(self, state, projection, filters, limit),
        fields(
            runtimedb.connection_id = %self.connection_id,
            runtimedb.schema = %self.schema_name,
            runtimedb.table = %self.table_name,
            runtimedb.cache_hit = tracing::field::Empty,
        )
    )]
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
            .get_table(&self.connection_id, &self.schema_name, &self.table_name)
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to get table info: {}", e).into())
            })?
            .ok_or_else(|| {
                DataFusionError::External("Table not found in catalog".to_string().into())
            })?;

        let (parquet_url, cache_hit) = if let Some(path) = table_info.parquet_path {
            // Already cached, use existing path
            let url = if path.starts_with("file://") || path.starts_with("s3://") {
                path
            } else {
                format!("file://{}", path)
            };
            (url, true)
        } else {
            // Not cached, fetch now
            let url = self.fetch_and_cache().await?;
            (url, false)
        };

        tracing::Span::current().record("runtimedb.cache_hit", cache_hit);

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
