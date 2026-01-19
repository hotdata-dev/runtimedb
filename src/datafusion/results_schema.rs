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
        // If key already exists, just update the value (don't change order)
        if self.entries.contains_key(&key) {
            self.entries.insert(key, value);
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
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
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
