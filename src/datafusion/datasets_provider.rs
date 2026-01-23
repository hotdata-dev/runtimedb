//! Catalog and schema providers for user-uploaded datasets.
//!
//! The `datasets` catalog provides access to datasets created via the datasets API.
//! Each dataset is stored as a parquet file and can be queried via SQL.

use super::block_on;
use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// Maximum number of table providers to cache.
/// Each cached entry holds schema metadata and file references (not data).
const DEFAULT_CACHE_CAPACITY: usize = 100;

/// A simple bounded cache with FIFO eviction.
/// When capacity is reached, the oldest entries are evicted.
struct BoundedCache<V> {
    /// Map from key to value
    entries: std::collections::HashMap<String, V>,
    /// Insertion order for FIFO eviction
    order: VecDeque<String>,
    /// Maximum number of entries
    capacity: usize,
}

impl<V> BoundedCache<V> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: std::collections::HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&self, key: &str) -> Option<&V> {
        self.entries.get(key)
    }

    fn insert(&mut self, key: String, value: V) {
        use std::collections::hash_map::Entry;

        // If key already exists, just update the value (don't change order)
        if let Entry::Occupied(mut e) = self.entries.entry(key.clone()) {
            e.insert(value);
            return;
        }

        // Evict oldest if at capacity
        while self.entries.len() >= self.capacity {
            if let Some(oldest_key) = self.order.pop_front() {
                self.entries.remove(&oldest_key);
            }
        }

        // Insert new entry
        self.entries.insert(key.clone(), value);
        self.order.push_back(key);
    }

    /// Remove an entry from the cache.
    fn remove(&mut self, key: &str) {
        if self.entries.remove(key).is_some() {
            // Remove from order queue (O(n) but called infrequently)
            self.order.retain(|k| k != key);
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A catalog provider for the "datasets" catalog.
/// Provides a single "default" schema containing all user-uploaded datasets.
#[derive(Debug)]
pub struct DatasetsCatalogProvider {
    schema: Arc<dyn SchemaProvider>,
}

impl DatasetsCatalogProvider {
    /// Create a new DatasetsCatalogProvider.
    ///
    /// The `ctx` parameter is used to share the RuntimeEnv (including object stores like S3)
    /// with the schema provider for parquet file access.
    pub fn new(catalog: Arc<dyn CatalogManager>, ctx: &SessionContext) -> Self {
        Self {
            schema: Arc::new(DatasetsSchemaProvider::new(catalog, ctx)),
        }
    }
}

impl CatalogProvider for DatasetsCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Datasets use a single "default" schema
        vec!["default".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "default" {
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

/// Schema provider for datasets - lists tables from the datasets catalog table.
///
/// Caches table providers in memory since dataset schemas are immutable after creation.
/// This avoids expensive Parquet metadata reads on repeated queries.
/// The cache is bounded to prevent unbounded memory growth.
pub struct DatasetsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Session state for schema inference - shares the RuntimeEnv (and object stores) with the main session
    session_state: Arc<SessionState>,
    /// Bounded cache of table providers keyed by table name.
    /// Uses FIFO eviction when capacity is reached.
    table_cache: RwLock<BoundedCache<Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for DatasetsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetsSchemaProvider")
            .field("catalog", &"...")
            .field("session_state", &"...")
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
    ///
    /// The `ctx` parameter is used to share the RuntimeEnv (including object stores like S3)
    /// with the session state for parquet schema inference.
    pub fn new(catalog: Arc<dyn CatalogManager>, ctx: &SessionContext) -> Self {
        // Create a minimal session state that shares the RuntimeEnv
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env())
            .build();
        Self {
            catalog,
            session_state: Arc::new(session_state),
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
        // Query catalog for all datasets, paginating until exhaustion
        const PAGE_SIZE: usize = 1000;
        let mut all_names = Vec::new();
        let mut offset = 0;

        while let Ok((datasets, has_more)) = block_on(self.catalog.list_datasets(PAGE_SIZE, offset))
        {
            all_names.extend(datasets.into_iter().map(|d| d.table_name));
            if !has_more {
                break;
            }
            offset += PAGE_SIZE;
        }

        all_names
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
            .get_dataset_by_table_name("default", name)
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

        // Infer schema using the shared session state (which has access to object stores like S3)
        let schema = listing_options
            .infer_schema(self.session_state.as_ref(), &table_path)
            .await?;

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
            block_on(self.catalog.get_dataset_by_table_name("default", name)),
            Ok(Some(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounded_cache_basic_operations() {
        let mut cache: BoundedCache<i32> = BoundedCache::new(3);

        // Empty cache
        assert!(cache.get("a").is_none());
        assert_eq!(cache.len(), 0);

        // Insert and retrieve
        cache.insert("a".to_string(), 1);
        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.len(), 1);

        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_bounded_cache_eviction_at_capacity() {
        let mut cache: BoundedCache<i32> = BoundedCache::new(3);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        // At capacity, inserting "d" should evict "a" (oldest)
        cache.insert("d".to_string(), 4);

        assert!(
            cache.get("a").is_none(),
            "oldest entry 'a' should be evicted"
        );
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.get("d"), Some(&4));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_bounded_cache_update_existing_key_no_eviction() {
        let mut cache: BoundedCache<i32> = BoundedCache::new(3);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        // Updating "a" should not cause eviction or change order
        cache.insert("a".to_string(), 100);

        assert_eq!(cache.get("a"), Some(&100), "value should be updated");
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.len(), 3);

        // Now insert "d" - should evict "a" since it's still oldest
        cache.insert("d".to_string(), 4);

        assert!(cache.get("a").is_none(), "'a' should be evicted as oldest");
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.get("d"), Some(&4));
    }
}
