//! Catalog and schema providers for user-uploaded datasets.
//!
//! The `datasets` catalog provides access to datasets created via the datasets API.
//! Each dataset is stored as a parquet file and can be queried via SQL.

use super::block_on;
use super::bounded_cache::{BoundedCache, DEFAULT_CACHE_CAPACITY};
use crate::catalog::CatalogManager;
use crate::datasets::DEFAULT_SCHEMA;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use std::any::Any;
use std::sync::{Arc, RwLock};

/// A catalog provider for the "datasets" catalog.
/// Provides a single "default" schema containing all user-uploaded datasets.
#[derive(Debug)]
pub struct DatasetsCatalogProvider {
    schema: Arc<dyn SchemaProvider>,
}

impl DatasetsCatalogProvider {
    /// Create a new DatasetsCatalogProvider.
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self {
            schema: Arc::new(DatasetsSchemaProvider::new(catalog)),
        }
    }
}

impl CatalogProvider for DatasetsCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == DEFAULT_SCHEMA {
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

/// Schema provider for datasets - lists tables from the datasets catalog table.
///
/// Caches table providers in memory since dataset schemas are immutable after creation.
/// This avoids expensive reads on repeated queries.
/// The cache is bounded to prevent unbounded memory growth.
pub struct DatasetsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Bounded cache of table providers keyed by table name.
    /// Uses FIFO eviction when capacity is reached.
    table_cache: RwLock<BoundedCache<Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for DatasetsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetsSchemaProvider")
            .field("catalog", &"...")
            .field(
                "table_cache",
                &format!(
                    "{} entries",
                    self.table_cache.read().map(|c| c.len()).unwrap_or(0)
                ),
            )
            .finish()
    }
}

impl DatasetsSchemaProvider {
    /// Create a new DatasetsSchemaProvider.
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self {
            catalog,
            table_cache: RwLock::new(BoundedCache::new(DEFAULT_CACHE_CAPACITY)),
        }
    }

    /// Invalidate a cached table by name.
    /// Call this when a dataset is deleted or its table_name is changed.
    pub fn invalidate_cache(&self, table_name: &str) {
        let mut cache = self.table_cache.write().expect("cache lock poisoned");
        cache.remove(table_name);
    }
}

#[async_trait]
impl SchemaProvider for DatasetsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        match block_on(self.catalog.list_all_datasets()) {
            Ok(datasets) => datasets.into_iter().map(|d| d.table_name).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Check cache first - dataset schemas are immutable so caching is safe
        {
            let cache = self.table_cache.read().expect("cache lock poisoned");
            if let Some(table) = cache.get(name) {
                return Ok(Some(table.clone()));
            }
        }

        // Look up dataset by table_name
        let dataset = self
            .catalog
            .get_dataset_by_table_name(DEFAULT_SCHEMA, name)
            .await
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
            })?;

        let info = match dataset {
            Some(info) => info,
            None => return Ok(None),
        };

        // Create listing table for the parquet file
        let table_path = ListingTableUrl::parse(&info.parquet_url)?;

        // Set up parquet format and listing options
        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format));

        // Use stored schema from catalog instead of re-inferring from parquet file
        let schema: datafusion::arrow::datatypes::SchemaRef =
            serde_json::from_str(&info.arrow_schema_json).map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "Failed to parse stored schema for dataset '{}': {}",
                    name, e
                ))))
            })?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);

        // Cache the table provider (bounded cache will evict oldest if at capacity)
        {
            let mut cache = self.table_cache.write().expect("cache lock poisoned");
            cache.insert(name.to_string(), table.clone());
        }

        Ok(Some(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        // This is a sync trait method, so block_on is required here
        matches!(
            block_on(self.catalog.get_dataset_by_table_name(DEFAULT_SCHEMA, name)),
            Ok(Some(_))
        )
    }
}
