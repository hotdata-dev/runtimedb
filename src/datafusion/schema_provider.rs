use anyhow::Result;
use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use std::sync::Arc;

use crate::catalog::CatalogManager;
use crate::datafetch::{ConnectionConfig, DataFetcher};
use crate::storage::StorageManager;

/// A schema provider that syncs tables on-demand from remote sources.
/// Wraps MemorySchemaProvider for caching already-loaded tables.
#[derive(Debug)]
pub struct HotDataSchemaProvider {
    connection_id: i32,
    #[allow(dead_code)]
    connection_name: String,
    schema_name: String,
    connection_config: Arc<serde_json::Value>,
    catalog: Arc<dyn CatalogManager>,
    inner: Arc<MemorySchemaProvider>,
    storage: Arc<dyn StorageManager>,
    fetcher: Arc<dyn DataFetcher>,
}

impl HotDataSchemaProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_id: i32,
        connection_name: String,
        schema_name: String,
        connection_config: serde_json::Value,
        catalog: Arc<dyn CatalogManager>,
        storage: Arc<dyn StorageManager>,
        fetcher: Arc<dyn DataFetcher>,
    ) -> Self {
        Self {
            connection_id,
            connection_name,
            schema_name,
            connection_config: Arc::new(connection_config),
            catalog,
            inner: Arc::new(MemorySchemaProvider::new()),
            storage,
            fetcher,
        }
    }

    /// Load a parquet file and return a TableProvider, filtering out dlt metadata columns.
    /// The parquet_url can be a file:// or s3:// URL.
    async fn load_parquet(&self, parquet_url: &str) -> Result<Arc<dyn TableProvider>> {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };
        use datafusion::prelude::SessionContext;

        // Parse the URL directly - DataFusion handles both file:// and s3:// if object store is registered
        let table_path = ListingTableUrl::parse(parquet_url)?;

        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format));

        // Need a temporary context to infer schema
        let temp_ctx = SessionContext::new();
        // Register storage with the temp context for S3 support
        self.storage.register_with_datafusion(&temp_ctx)?;

        let resolved_schema = listing_options
            .infer_schema(&temp_ctx.state(), &table_path)
            .await?;

        // Filter out _dlt metadata columns
        const DLT_COLUMNS: &[&str] = &["_dlt_id", "_dlt_load_id"];
        let filtered_fields: Vec<_> = resolved_schema
            .fields()
            .iter()
            .filter(|field| !DLT_COLUMNS.contains(&field.name().as_str()))
            .cloned()
            .collect();
        let filtered_schema = Arc::new(Schema::new(filtered_fields));

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(filtered_schema);

        let table = ListingTable::try_new(config)?;

        Ok(Arc::new(table) as Arc<dyn TableProvider>)
    }
}

#[async_trait]
impl SchemaProvider for HotDataSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Return all known tables from DuckDB catalog
        match self.catalog.list_tables(Some(self.connection_id)) {
            Ok(tables) => tables
                .into_iter()
                .filter(|t| t.schema_name == self.schema_name)
                .map(|t| t.table_name)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        // First check: is it already loaded in memory?
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        }

        // Second check: is it in the DuckDB catalog?
        let table_info = match self
            .catalog
            .get_table(self.connection_id, &self.schema_name, name)
        {
            Ok(Some(info)) => info,
            Ok(None) => return Ok(None), // Table doesn't exist
            Err(e) => {
                return Err(datafusion::common::DataFusionError::External(
                    format!("Failed to get table info: {}", e).into(),
                ))
            }
        };

        // Third: sync if needed, then load
        let parquet_url = if let Some(path) = table_info.parquet_path {
            // Already synced, just load it
            // Handle both URL and path formats for backward compatibility
            if path.starts_with("file://") || path.starts_with("s3://") {
                path
            } else {
                format!("file://{}", path)
            }
        } else {
            // Need to sync first - build connection config and fetch
            let source_type = self.connection_config
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("postgres");

            let connection_string = self.connection_config
                .get("connection_string")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let config = ConnectionConfig {
                source_type: source_type.to_string(),
                connection_string: connection_string.to_string(),
            };

            // Fetch the table data
            let parquet_url = self.fetcher
                .fetch_table(
                    &config,
                    None, // catalog
                    &self.schema_name,
                    name,
                    self.storage.as_ref(),
                    self.connection_id,
                )
                .await
                .map_err(|e| datafusion::common::DataFusionError::External(
                    format!("Failed to fetch table: {}", e).into(),
                ))?;

            // Update catalog with new path
            if let Ok(Some(info)) = self.catalog.get_table(self.connection_id, &self.schema_name, name) {
                let _ = self.catalog.update_table_sync(info.id, &parquet_url, "");
            }

            parquet_url
        };

        // Load the parquet file
        let provider = match self.load_parquet(&parquet_url).await {
            Ok(p) => p,
            Err(e) => {
                return Err(datafusion::common::DataFusionError::External(
                    format!("Failed to load parquet: {}", e).into(),
                ))
            }
        };

        // Cache it in memory for future queries
        self.inner
            .register_table(name.to_string(), provider.clone())
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        // Check DuckDB catalog
        matches!(
            self.catalog
                .get_table(self.connection_id, &self.schema_name, name),
            Ok(Some(_))
        )
    }
}
