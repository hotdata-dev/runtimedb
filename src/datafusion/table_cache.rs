use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::FutureExt;
use lru::LruCache;
use tokio::sync::Semaphore;

use crate::datafusion::scan_parquet_exec;
use crate::storage::StorageManager;

const FAILED_RETRY_BACKOFF_SECS: u64 = 60;
const DEFAULT_PRUNE_INTERVAL_SECS: u64 = 300;
const DEFAULT_FAILED_TTL_SECS: u64 = 600;
const DEFAULT_LOADING_TTL_SECS: u64 = 600;
const DEFAULT_TOO_LARGE_TTL_SECS: u64 = 86400;

#[derive(Debug)]
enum CacheState {
    Ready {
        table: Arc<MemTable>,
        bytes: u64,
        last_access: Instant,
    },
    Loading {
        started_at: Instant,
    },
    Failed {
        last_error: String,
        last_attempt: Instant,
    },
    TooLarge {
        bytes: u64,
        last_attempt: Instant,
    },
}

#[derive(Debug)]
struct CacheEntry {
    state: CacheState,
}

#[derive(Debug, Clone)]
enum CacheStateSnapshot {
    Ready {
        bytes: u64,
        last_access: Instant,
    },
    Loading {
        started_at: Instant,
    },
    Failed {
        last_error: String,
        last_attempt: Instant,
    },
    TooLarge {
        bytes: u64,
        last_attempt: Instant,
    },
}

#[derive(Debug, Clone)]
struct PruneConfig {
    interval: Duration,
    loading_ttl: Duration,
    failed_ttl: Duration,
    too_large_ttl: Duration,
}

impl Default for PruneConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_PRUNE_INTERVAL_SECS),
            loading_ttl: Duration::from_secs(DEFAULT_LOADING_TTL_SECS),
            failed_ttl: Duration::from_secs(DEFAULT_FAILED_TTL_SECS),
            too_large_ttl: Duration::from_secs(DEFAULT_TOO_LARGE_TTL_SECS),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    TooLarge,
}

#[derive(Debug)]
pub struct InMemoryTableCache {
    total_max_bytes: u64,
    table_max_bytes: u64,
    ttl: Option<Duration>,
    total_bytes: AtomicU64,
    inner: Mutex<LruCache<String, CacheEntry>>,
    prune_config: PruneConfig,
    last_prune: Mutex<Instant>,
}

impl InMemoryTableCache {
    pub fn new(total_max_bytes: u64, table_max_bytes: u64, ttl: Option<Duration>) -> Self {
        Self::with_prune_config(
            total_max_bytes,
            table_max_bytes,
            ttl,
            PruneConfig::default(),
        )
    }

    fn with_prune_config(
        total_max_bytes: u64,
        table_max_bytes: u64,
        ttl: Option<Duration>,
        prune_config: PruneConfig,
    ) -> Self {
        let inner = LruCache::unbounded();
        Self {
            total_max_bytes,
            table_max_bytes,
            ttl,
            total_bytes: AtomicU64::new(0),
            inner: Mutex::new(inner),
            prune_config,
            last_prune: Mutex::new(Instant::now()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.total_max_bytes > 0 && self.table_max_bytes > 0
    }

    pub fn total_bytes(&self) -> u64 {
        // Approximate size; updated without synchronization guarantees.
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn get_ready(&self, key: &str) -> Option<Arc<MemTable>> {
        let mut cache = self.inner.lock().unwrap();
        let entry = cache.get_mut(key)?;
        let should_evict = match &mut entry.state {
            CacheState::Ready {
                table,
                bytes,
                last_access,
            } => {
                if let Some(ttl) = self.ttl {
                    if last_access.elapsed() > ttl {
                        self.total_bytes.fetch_sub(*bytes, Ordering::Relaxed);
                        tracing::info!(
                            parquet_url = %key,
                            bytes = *bytes,
                            "Mem cache entry expired"
                        );
                        true
                    } else {
                        *last_access = Instant::now();
                        return Some(table.clone());
                    }
                } else {
                    *last_access = Instant::now();
                    return Some(table.clone());
                }
            }
            _ => return None,
        };

        if should_evict {
            cache.pop(key);
        }
        None
    }

    fn snapshot(&self, key: &str) -> Option<CacheStateSnapshot> {
        let mut cache = self.inner.lock().unwrap();
        let entry = cache.get_mut(key)?;
        match &mut entry.state {
            CacheState::Ready {
                bytes, last_access, ..
            } => Some(CacheStateSnapshot::Ready {
                bytes: *bytes,
                last_access: *last_access,
            }),
            CacheState::Loading { started_at } => Some(CacheStateSnapshot::Loading {
                started_at: *started_at,
            }),
            CacheState::Failed {
                last_error,
                last_attempt,
            } => Some(CacheStateSnapshot::Failed {
                last_error: last_error.clone(),
                last_attempt: *last_attempt,
            }),
            CacheState::TooLarge {
                bytes,
                last_attempt,
            } => Some(CacheStateSnapshot::TooLarge {
                bytes: *bytes,
                last_attempt: *last_attempt,
            }),
        }
    }

    fn prune_if_needed(&self) {
        let interval = self.prune_config.interval;
        if interval.is_zero() {
            return;
        }

        let now = Instant::now();
        let mut last_prune = self.last_prune.lock().unwrap();
        if now.duration_since(*last_prune) < interval {
            return;
        }
        *last_prune = now;
        drop(last_prune);

        self.prune_expired(now);
    }

    fn prune_expired(&self, now: Instant) {
        let mut cache = self.inner.lock().unwrap();
        let mut to_remove = Vec::new();

        for (key, entry) in cache.iter() {
            let expired = match &entry.state {
                CacheState::Loading { started_at } => {
                    now.duration_since(*started_at) > self.prune_config.loading_ttl
                }
                CacheState::Failed { last_attempt, .. } => {
                    now.duration_since(*last_attempt) > self.prune_config.failed_ttl
                }
                CacheState::TooLarge { last_attempt, .. } => {
                    now.duration_since(*last_attempt) > self.prune_config.too_large_ttl
                }
                CacheState::Ready { .. } => false,
            };

            if expired {
                to_remove.push(key.clone());
            }
        }

        if to_remove.is_empty() {
            return;
        }

        let mut transitioned_loading = 0u64;
        let mut removed_failed = 0u64;
        let mut removed_too_large = 0u64;

        for key in to_remove {
            if let Some(mut entry) = cache.pop(&key) {
                match entry.state {
                    CacheState::Loading { .. } => {
                        entry.state = CacheState::Failed {
                            last_error: "stale loading entry".to_string(),
                            last_attempt: now,
                        };
                        cache.put(key, entry);
                        transitioned_loading += 1;
                    }
                    CacheState::Failed { .. } => {
                        removed_failed += 1;
                    }
                    CacheState::TooLarge { .. } => {
                        removed_too_large += 1;
                    }
                    CacheState::Ready { bytes, .. } => {
                        self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    }
                }
            }
        }

        tracing::info!(
            transitioned_loading,
            removed_failed,
            removed_too_large,
            total_bytes = self.total_bytes.load(Ordering::Relaxed),
            "Mem cache pruned stale non-ready entries"
        );
    }

    pub fn mark_loading(&self, key: &str) -> bool {
        let mut cache = self.inner.lock().unwrap();
        match cache.get_mut(key) {
            Some(entry) => match &entry.state {
                CacheState::Loading { .. } => false,
                CacheState::Ready { .. } => false,
                CacheState::TooLarge { .. } => false,
                CacheState::Failed { last_attempt, .. } => {
                    if last_attempt.elapsed() < Duration::from_secs(FAILED_RETRY_BACKOFF_SECS) {
                        false
                    } else {
                        entry.state = CacheState::Loading {
                            started_at: Instant::now(),
                        };
                        tracing::info!(parquet_url = %key, "Mem cache load retry started");
                        true
                    }
                }
            },
            None => {
                cache.put(
                    key.to_string(),
                    CacheEntry {
                        state: CacheState::Loading {
                            started_at: Instant::now(),
                        },
                    },
                );
                tracing::info!(parquet_url = %key, "Mem cache load started");
                true
            }
        }
    }

    pub fn mark_failed(&self, key: &str, error: String) {
        let mut cache = self.inner.lock().unwrap();
        if let Some(entry) = cache.get_mut(key) {
            entry.state = CacheState::Failed {
                last_error: error.clone(),
                last_attempt: Instant::now(),
            };
        } else {
            cache.put(
                key.to_string(),
                CacheEntry {
                    state: CacheState::Failed {
                        last_error: error.clone(),
                        last_attempt: Instant::now(),
                    },
                },
            );
        }
        tracing::warn!(parquet_url = %key, error = %error, "Mem cache load failed");
    }

    pub fn mark_too_large(&self, key: &str, bytes: u64) {
        let mut cache = self.inner.lock().unwrap();
        if let Some(entry) = cache.get_mut(key) {
            entry.state = CacheState::TooLarge {
                bytes,
                last_attempt: Instant::now(),
            };
        } else {
            cache.put(
                key.to_string(),
                CacheEntry {
                    state: CacheState::TooLarge {
                        bytes,
                        last_attempt: Instant::now(),
                    },
                },
            );
        }
        tracing::info!(parquet_url = %key, bytes, "Mem cache skipped: table too large");
    }

    pub fn insert_ready(&self, key: &str, table: Arc<MemTable>, bytes: u64) -> InsertOutcome {
        if bytes > self.table_max_bytes {
            self.mark_too_large(key, bytes);
            return InsertOutcome::TooLarge;
        }
        if self.total_max_bytes == 0 {
            self.mark_too_large(key, bytes);
            return InsertOutcome::TooLarge;
        }

        let mut cache = self.inner.lock().unwrap();

        // Remove existing entry (if any) and adjust size.
        if let Some(existing) = cache.pop(key) {
            if let CacheState::Ready { bytes, .. } = existing.state {
                self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
        }

        // Evict LRU entries until we fit.
        let mut evicted_entries = 0u64;
        let mut evicted_bytes = 0u64;
        while self.total_bytes.load(Ordering::Relaxed) + bytes > self.total_max_bytes {
            let (evicted_key, evicted) = match cache.pop_lru() {
                Some(value) => value,
                None => break,
            };

            if let CacheState::Ready { bytes, .. } = evicted.state {
                self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
                evicted_entries += 1;
                evicted_bytes += bytes;
            }

            // Avoid potential large maps holding non-ready entries.
            if matches!(evicted.state, CacheState::Loading { .. }) {
                tracing::debug!(
                    parquet_url = %evicted_key,
                    "Evicting loading entry while enforcing cache limit"
                );
            }
        }

        if evicted_entries > 0 {
            tracing::info!(
                parquet_url = %key,
                evicted_entries,
                evicted_bytes,
                "Mem cache evicted LRU entries"
            );
        }

        if self.total_bytes.load(Ordering::Relaxed) + bytes > self.total_max_bytes {
            self.mark_too_large(key, bytes);
            return InsertOutcome::TooLarge;
        }

        cache.put(
            key.to_string(),
            CacheEntry {
                state: CacheState::Ready {
                    table,
                    bytes,
                    last_access: Instant::now(),
                },
            },
        );
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        tracing::info!(
            parquet_url = %key,
            bytes,
            total_bytes = self.total_bytes.load(Ordering::Relaxed),
            "Mem cache insert complete"
        );
        InsertOutcome::Inserted
    }

    pub fn invalidate_url(&self, key: &str) {
        let mut cache = self.inner.lock().unwrap();
        if let Some(entry) = cache.pop(key) {
            if let CacheState::Ready { bytes, .. } = entry.state {
                self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
                tracing::info!(
                    parquet_url = %key,
                    bytes,
                    total_bytes = self.total_bytes.load(Ordering::Relaxed),
                    "Mem cache invalidated URL"
                );
            }
        }
    }

    pub fn invalidate_prefix(&self, prefix: &str) {
        let mut cache = self.inner.lock().unwrap();
        let with_scheme = format!("file://{}", prefix);
        let mut to_remove = Vec::new();

        for (key, entry) in cache.iter() {
            if key.starts_with(prefix) || key.starts_with(&with_scheme) {
                if let CacheState::Ready { bytes, .. } = &entry.state {
                    self.total_bytes.fetch_sub(*bytes, Ordering::Relaxed);
                }
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            cache.pop(&key);
        }
        tracing::info!(
            prefix = %prefix,
            total_bytes = self.total_bytes.load(Ordering::Relaxed),
            "Mem cache invalidated prefix"
        );
    }
}

#[derive(Debug, Clone)]
pub struct ParquetCacheManager {
    cache: Arc<InMemoryTableCache>,
    storage: Arc<dyn StorageManager>,
    warm_semaphore: Arc<Semaphore>,
}

impl ParquetCacheManager {
    pub fn new(
        cache: Arc<InMemoryTableCache>,
        storage: Arc<dyn StorageManager>,
        warm_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            cache,
            storage,
            warm_semaphore,
        }
    }

    pub fn cache(&self) -> &Arc<InMemoryTableCache> {
        &self.cache
    }

    pub async fn scan_with_cache(
        &self,
        parquet_url: &str,
        schema: SchemaRef,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !self.cache.enabled() {
            return scan_parquet_exec(parquet_url, schema, state, projection, filters, limit).await;
        }

        self.cache.prune_if_needed();

        if let Some(table) = self.cache.get_ready(parquet_url) {
            tracing::info!(parquet_url = %parquet_url, "Mem cache hit");
            return table.scan(state, projection, filters, limit).await;
        }

        if let Some(snapshot) = self.cache.snapshot(parquet_url) {
            match snapshot {
                CacheStateSnapshot::Loading { started_at } => {
                    tracing::info!(
                        parquet_url = %parquet_url,
                        load_ms = started_at.elapsed().as_millis() as u64,
                        "Mem cache load in progress"
                    );
                }
                CacheStateSnapshot::Failed {
                    last_error,
                    last_attempt,
                } => {
                    tracing::info!(
                        parquet_url = %parquet_url,
                        last_attempt_ms = last_attempt.elapsed().as_millis() as u64,
                        error = %last_error,
                        "Mem cache load previously failed"
                    );
                }
                CacheStateSnapshot::TooLarge {
                    bytes,
                    last_attempt,
                } => {
                    tracing::info!(
                        parquet_url = %parquet_url,
                        bytes,
                        last_attempt_ms = last_attempt.elapsed().as_millis() as u64,
                        "Mem cache skipped (too large)"
                    );
                }
                CacheStateSnapshot::Ready { bytes, last_access } => {
                    tracing::info!(
                        parquet_url = %parquet_url,
                        bytes,
                        idle_ms = last_access.elapsed().as_millis() as u64,
                        "Mem cache ready"
                    );
                }
            }
        } else {
            tracing::info!(parquet_url = %parquet_url, "Mem cache miss");
        }

        // Attempt to warm in the background without blocking.
        if let Ok(permit) = self.warm_semaphore.clone().try_acquire_owned() {
            if self.cache.mark_loading(parquet_url) {
                let cache = self.cache.clone();
                let storage = self.storage.clone();
                let schema_clone = schema.clone();
                let parquet_url = parquet_url.to_string();
                tokio::spawn(async move {
                    let _permit = permit;
                    let start = Instant::now();
                    let load_result = std::panic::AssertUnwindSafe(load_parquet_into_cache(
                        &cache,
                        storage.as_ref(),
                        &parquet_url,
                        schema_clone,
                    ))
                    .catch_unwind()
                    .await;

                    match load_result {
                        Ok(Ok(())) => {
                            tracing::info!(
                                parquet_url = %parquet_url,
                                load_ms = start.elapsed().as_millis() as u64,
                                "Mem cache load completed"
                            );
                        }
                        Ok(Err(err)) => {
                            cache.mark_failed(&parquet_url, err.to_string());
                        }
                        Err(_) => {
                            cache.mark_failed(&parquet_url, "mem cache load panicked".to_string());
                        }
                    }
                });
            }
        } else {
            tracing::info!(parquet_url = %parquet_url, "Mem cache warm skipped (no permits)");
        }

        scan_parquet_exec(parquet_url, schema, state, projection, filters, limit).await
    }
}

#[derive(Debug)]
pub struct CachedParquetTableProvider {
    parquet_url: String,
    schema: SchemaRef,
    cache_manager: Option<Arc<ParquetCacheManager>>,
}

impl CachedParquetTableProvider {
    pub fn new(
        parquet_url: String,
        schema: SchemaRef,
        cache_manager: Option<Arc<ParquetCacheManager>>,
    ) -> Self {
        Self {
            parquet_url,
            schema,
            cache_manager,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for CachedParquetTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if let Some(cache_manager) = &self.cache_manager {
            return cache_manager
                .scan_with_cache(
                    &self.parquet_url,
                    self.schema.clone(),
                    state,
                    projection,
                    filters,
                    limit,
                )
                .await;
        }

        scan_parquet_exec(
            &self.parquet_url,
            self.schema.clone(),
            state,
            projection,
            filters,
            limit,
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact;
            filters.len()
        ])
    }
}

async fn load_parquet_into_cache(
    cache: &InMemoryTableCache,
    storage: &dyn StorageManager,
    parquet_url: &str,
    schema: SchemaRef,
) -> Result<()> {
    let parquet_file_url = normalize_parquet_file_url(parquet_url);
    let (local_path, is_temp) = storage.get_local_path(&parquet_file_url).await?;
    let local_path_for_cleanup = local_path.clone();
    let table_max_bytes = cache.table_max_bytes;

    let result = tokio::task::spawn_blocking(move || -> Result<LoadResult> {
        let file = std::fs::File::open(&local_path)?;
        let metadata = file.metadata()?;
        let bytes = metadata.len();

        if bytes > table_max_bytes {
            return Ok(LoadResult::TooLarge { bytes });
        }

        let options = datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions::new()
            .with_schema(schema.clone());
        let reader = datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            options,
        )?
        .build()?;

        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        let table = MemTable::try_new(schema.clone(), vec![batches])?;
        Ok(LoadResult::Loaded { table, bytes })
    })
    .await?;

    if is_temp {
        let _ = tokio::fs::remove_file(local_path_for_cleanup).await;
    }

    match result? {
        LoadResult::Loaded { table, bytes } => {
            cache.insert_ready(parquet_url, Arc::new(table), bytes);
        }
        LoadResult::TooLarge { bytes } => {
            cache.mark_too_large(parquet_url, bytes);
        }
    }

    Ok(())
}

enum LoadResult {
    Loaded { table: MemTable, bytes: u64 },
    TooLarge { bytes: u64 },
}

fn normalize_parquet_file_url(url: &str) -> String {
    // Supports both file:// and object store URLs. If a directory URL is provided,
    // append the canonical parquet filename to match cache layout.
    if url.ends_with(".parquet") {
        return url.to_string();
    }
    let trimmed = url.trim_end_matches('/');
    format!("{}/data.parquet", trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafetch::native::StreamingParquetWriter;
    use crate::datafetch::BatchWriter;
    use crate::storage::FilesystemStorage;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    fn make_memtable(schema: SchemaRef) -> Arc<MemTable> {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[test]
    fn cache_evicts_lru_when_over_limit() {
        let cache = InMemoryTableCache::new(100, 100, None);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        cache.insert_ready("a", make_memtable(schema.clone()), 60);
        cache.insert_ready("b", make_memtable(schema.clone()), 60);
        assert!(cache.get_ready("a").is_none());
        assert!(cache.get_ready("b").is_some());
    }

    #[test]
    fn cache_skips_too_large_table() {
        let cache = InMemoryTableCache::new(100, 50, None);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let outcome = cache.insert_ready("big", make_memtable(schema), 60);
        assert_eq!(outcome, InsertOutcome::TooLarge);
        assert!(cache.get_ready("big").is_none());
    }

    #[test]
    fn cache_ttl_expires_entries() {
        let cache = InMemoryTableCache::new(100, 100, Some(Duration::from_millis(10)));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        cache.insert_ready("ttl", make_memtable(schema), 10);
        std::thread::sleep(Duration::from_millis(20));
        assert!(cache.get_ready("ttl").is_none());
    }

    #[test]
    fn cache_prune_removes_stale_non_ready_entries() {
        let prune_config = PruneConfig {
            interval: Duration::from_millis(1),
            loading_ttl: Duration::from_millis(1),
            failed_ttl: Duration::from_millis(1),
            too_large_ttl: Duration::from_millis(1),
        };
        let cache = InMemoryTableCache::with_prune_config(100, 100, None, prune_config);

        cache.mark_loading("loading");
        cache.mark_failed("failed", "boom".to_string());
        cache.mark_too_large("too_large", 123);

        std::thread::sleep(Duration::from_millis(2));
        cache.prune_expired(Instant::now());

        match cache.snapshot("loading") {
            Some(CacheStateSnapshot::Failed { last_error, .. }) => {
                assert_eq!(last_error, "stale loading entry");
            }
            other => panic!("expected failed loading entry, got {:?}", other),
        }
        assert!(cache.snapshot("failed").is_none());
        assert!(cache.snapshot("too_large").is_none());
    }

    #[test]
    fn cache_invalidate_prefix_removes_file_urls() {
        let cache = InMemoryTableCache::new(100, 100, None);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        cache.insert_ready("file:///tmp/cache/conn/table", make_memtable(schema), 10);

        cache.invalidate_prefix("/tmp/cache/conn");
        assert!(cache.get_ready("file:///tmp/cache/conn/table").is_none());
    }

    #[test]
    fn cache_invalidate_url_removes_exact_match() {
        let cache = InMemoryTableCache::new(100, 100, None);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        cache.insert_ready("s3://bucket/cache/conn/table", make_memtable(schema), 10);

        cache.invalidate_url("s3://bucket/cache/conn/table");
        assert!(cache.get_ready("s3://bucket/cache/conn/table").is_none());
    }

    #[tokio::test]
    async fn scan_with_cache_warms_in_background() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let storage = Arc::new(FilesystemStorage::new(cache_dir.to_str().unwrap()));

        let parquet_path = cache_dir.join("test.parquet");
        let mut writer = StreamingParquetWriter::new(parquet_path.clone());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        writer.init(&schema).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        writer.write_batch(&batch).unwrap();
        Box::new(writer).close().unwrap();

        let cache = Arc::new(InMemoryTableCache::new(10_000_000, 10_000_000, None));
        let manager = ParquetCacheManager::new(cache.clone(), storage, Arc::new(Semaphore::new(1)));
        let ctx = SessionContext::new();

        let parquet_url = format!("file://{}", parquet_path.display());
        let state = ctx.state();
        manager
            .scan_with_cache(&parquet_url, schema.clone(), &state, None, &[], None)
            .await
            .unwrap();

        let ready = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.get_ready(&parquet_url).is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(ready.is_ok());
    }

    #[tokio::test]
    async fn scan_with_cache_handles_directory_urls() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let table_dir = cache_dir
            .join("conn")
            .join("schema")
            .join("table")
            .join("v1");
        std::fs::create_dir_all(&table_dir).unwrap();
        let storage = Arc::new(FilesystemStorage::new(cache_dir.to_str().unwrap()));

        let parquet_path = table_dir.join("data.parquet");
        let mut writer = StreamingParquetWriter::new(parquet_path.clone());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        writer.init(&schema).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        writer.write_batch(&batch).unwrap();
        Box::new(writer).close().unwrap();

        let cache = Arc::new(InMemoryTableCache::new(10_000_000, 10_000_000, None));
        let manager = ParquetCacheManager::new(cache.clone(), storage, Arc::new(Semaphore::new(1)));
        let ctx = SessionContext::new();

        let parquet_url = format!("file://{}", table_dir.display());
        let state = ctx.state();
        manager
            .scan_with_cache(&parquet_url, schema.clone(), &state, None, &[], None)
            .await
            .unwrap();

        let ready = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.get_ready(&parquet_url).is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(ready.is_ok());
    }
}
