use crate::catalog::CatalogManager;
use crate::datafusion::block_on;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
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

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Schema provider for the `runtimedb.results` schema.
/// Resolves result IDs to their parquet files.
///
/// Caches table providers in memory since result schemas are immutable after creation.
/// This avoids expensive Parquet metadata reads on repeated queries.
/// The cache is bounded to prevent unbounded memory growth.
pub struct ResultsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Session state for schema inference - shares the RuntimeEnv (and object stores) with the main session
    session_state: Arc<SessionState>,
    /// Bounded cache of table providers keyed by result ID.
    /// Uses FIFO eviction when capacity is reached.
    table_cache: RwLock<BoundedCache<Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for ResultsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultsSchemaProvider")
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

impl ResultsSchemaProvider {
    /// Create a new ResultsSchemaProvider using the RuntimeEnv from the given SessionContext.
    ///
    /// This creates a minimal session state that shares the RuntimeEnv (and thus object stores)
    /// with the provided context, avoiding the need to re-register storage backends.
    pub fn with_runtime_env(
        catalog: Arc<dyn CatalogManager>,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Self {
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
}

#[async_trait]
impl SchemaProvider for ResultsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Don't enumerate results - users should know the ID
        Vec::new()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        // Check cache first - result schemas are immutable so caching is safe
        {
            let cache = self.table_cache.read().expect("cache lock poisoned");
            if let Some(table) = cache.get(name) {
                return Ok(Some(table.clone()));
            }
        }

        // Look up result by ID - await directly since we're in an async fn
        let result = match self.catalog.get_result(name).await {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(datafusion::error::DataFusionError::External(Box::new(
                    std::io::Error::other(e.to_string()),
                )));
            }
        };

        // Create listing table for the parquet file
        let table_path = ListingTableUrl::parse(&result.parquet_path)?;

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
        matches!(block_on(self.catalog.get_result(name)), Ok(Some(_)))
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

    #[test]
    fn test_bounded_cache_capacity_one() {
        let mut cache: BoundedCache<i32> = BoundedCache::new(1);

        cache.insert("a".to_string(), 1);
        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.len(), 1);

        // Insert "b" should evict "a"
        cache.insert("b".to_string(), 2);
        assert!(cache.get("a").is_none());
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_bounded_cache_fifo_order() {
        let mut cache: BoundedCache<i32> = BoundedCache::new(3);

        cache.insert("first".to_string(), 1);
        cache.insert("second".to_string(), 2);
        cache.insert("third".to_string(), 3);

        // Insert 3 more, should evict in FIFO order
        cache.insert("fourth".to_string(), 4);
        assert!(cache.get("first").is_none());

        cache.insert("fifth".to_string(), 5);
        assert!(cache.get("second").is_none());

        cache.insert("sixth".to_string(), 6);
        assert!(cache.get("third").is_none());

        // Only the last 3 should remain
        assert_eq!(cache.get("fourth"), Some(&4));
        assert_eq!(cache.get("fifth"), Some(&5));
        assert_eq!(cache.get("sixth"), Some(&6));
    }
}
