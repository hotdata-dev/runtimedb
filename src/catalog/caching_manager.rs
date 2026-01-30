//! Redis caching layer for CatalogManager.
//!
//! Wraps any CatalogManager implementation and caches metadata calls in Redis.
//! The cache is optional - if Redis is not configured, operations pass through
//! to the inner catalog directly.

use super::{
    CatalogManager, ConnectionInfo, DatasetInfo, OptimisticLock, PendingDeletion, QueryResult,
    TableInfo, UploadInfo,
};
use crate::config::CacheConfig;
use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Wrapper around cached data with timestamp for TTL tracking.
#[derive(Debug, Serialize, Deserialize)]
struct CachedEntry<T> {
    data: T,
    cached_at: u64,
}

/// URL-encode a key segment to avoid `:` conflicts in Redis keys.
fn encode_key_segment(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Internal state for CachingCatalogManager.
struct CachingCatalogManagerInner {
    inner: Arc<dyn CatalogManager>,
    redis: ConnectionManager,
    config: CacheConfig,
    refresh_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_token: tokio_util::sync::CancellationToken,
}

/// Caching wrapper for CatalogManager implementations.
///
/// Caches metadata calls in Redis with configurable TTL.
/// Falls through to inner catalog if Redis is unavailable.
pub struct CachingCatalogManager {
    state: Arc<CachingCatalogManagerInner>,
}

impl fmt::Debug for CachingCatalogManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingCatalogManager")
            .field("inner", &self.state.inner)
            .field("redis", &"<ConnectionManager>")
            .field("config", &self.state.config)
            .finish()
    }
}

impl CachingCatalogManager {
    /// Create a new caching catalog manager.
    pub async fn new(
        inner: Arc<dyn CatalogManager>,
        redis_url: &str,
        config: CacheConfig,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis = ConnectionManager::new(client).await?;

        Ok(Self {
            state: Arc::new(CachingCatalogManagerInner {
                inner,
                redis,
                config,
                refresh_handle: std::sync::Mutex::new(None),
                shutdown_token: CancellationToken::new(),
            }),
        })
    }

    /// Get the key prefix from config.
    fn prefix(&self) -> &str {
        &self.state.config.key_prefix
    }

    /// Get the config.
    fn config(&self) -> &CacheConfig {
        &self.state.config
    }

    /// Get a clone of the redis connection manager.
    fn redis(&self) -> ConnectionManager {
        self.state.redis.clone()
    }

    /// Get the inner catalog manager.
    fn inner(&self) -> &Arc<dyn CatalogManager> {
        &self.state.inner
    }

    /// Build a cache key for connections list.
    fn key_conn_list(&self) -> String {
        format!("{}conn:list", self.prefix())
    }

    /// Build a cache key for a connection by ID.
    fn key_conn_id(&self, id: &str) -> String {
        format!("{}conn:{}", self.prefix(), encode_key_segment(id))
    }

    /// Build a cache key for a connection by name.
    fn key_conn_name(&self, name: &str) -> String {
        format!("{}conn:name:{}", self.prefix(), encode_key_segment(name))
    }

    /// Build a cache key for tables list (all or by connection).
    fn key_tbl_list(&self, connection_id: Option<&str>) -> String {
        match connection_id {
            Some(id) => format!("{}tbl:list:conn:{}", self.prefix(), encode_key_segment(id)),
            None => format!("{}tbl:list:all", self.prefix()),
        }
    }

    /// Build a cache key for a specific table.
    fn key_tbl(&self, connection_id: &str, schema: &str, table: &str) -> String {
        format!(
            "{}tbl:{}:{}:{}",
            self.prefix(),
            encode_key_segment(connection_id),
            encode_key_segment(schema),
            encode_key_segment(table)
        )
    }

    /// Build a cache key for dataset table names.
    fn key_tbl_names(&self, schema_name: &str) -> String {
        format!(
            "{}tbl:names:{}",
            self.prefix(),
            encode_key_segment(schema_name)
        )
    }

    /// Read from cache with fallback to inner catalog.
    async fn cached_read<T, F, Fut>(&self, key: &str, fetch: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Try to get from Redis
        let result: Result<Option<String>, _> = self.redis().get(key).await;
        match result {
            Ok(Some(json)) => {
                // Deserialize cached entry
                match serde_json::from_str::<CachedEntry<T>>(&json) {
                    Ok(entry) => return Ok(entry.data),
                    Err(e) => {
                        // Corrupted cache entry - log and treat as miss
                        warn!("cache: failed to deserialize {}: {}", key, e);
                    }
                }
            }
            Ok(None) => {
                // Cache miss - fall through to fetch
            }
            Err(e) => {
                // Redis unreachable - log and fall through to fetch
                warn!("cache: redis get failed for {}: {}", key, e);
            }
        }

        // Fetch from inner catalog
        let data = fetch().await?;

        // Cache the result (best-effort)
        if let Err(e) = self.cache_set(key, &data).await {
            warn!("cache: failed to set {}: {}", key, e);
        }

        Ok(data)
    }

    /// Set a value in the cache with TTL.
    async fn cache_set<T: Serialize>(&self, key: &str, data: &T) -> Result<()> {
        let entry = CachedEntry {
            data,
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        let json = serde_json::to_string(&entry)?;

        let _: () = self
            .redis()
            .set_ex(key, json, self.config().ttl_secs)
            .await
            .map_err(|e| anyhow!("redis set failed: {}", e))?;

        Ok(())
    }

    /// Delete a key from the cache.
    async fn cache_del(&self, key: &str) {
        let result: Result<(), _> = self.redis().del(key).await;
        if let Err(e) = result {
            warn!("cache: failed to delete {}: {}", key, e);
        }
    }

    /// Delete keys matching a pattern using SCAN.
    async fn cache_del_pattern(&self, pattern: &str) {
        let mut cursor = 0u64;
        loop {
            let result: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut self.redis())
                .await;

            match result {
                Ok((next_cursor, keys)) => {
                    for key in keys {
                        self.cache_del(&key).await;
                    }
                    cursor = next_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
                Err(e) => {
                    warn!("cache: SCAN failed for pattern {}: {}", pattern, e);
                    break;
                }
            }
        }
    }

    /// Start the background refresh loop (internal).
    ///
    /// Called from init() when refresh is enabled.
    fn start_refresh_loop(&self, node_id: String) {
        let state = Arc::clone(&self.state);
        let interval_secs = self.config().refresh_interval_secs;
        let shutdown_token = self.state.shutdown_token.clone();

        let handle = tokio::spawn(async move {
            Self::refresh_loop_inner(state, node_id, interval_secs, shutdown_token).await;
        });

        *self.state.refresh_handle.lock().unwrap() = Some(handle);
    }

    /// The refresh loop that periodically refreshes all cached metadata.
    async fn refresh_loop_inner(
        state: Arc<CachingCatalogManagerInner>,
        node_id: String,
        interval_secs: u64,
        shutdown_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        // Skip missed ticks to prevent back-to-back refreshes if a cycle exceeds the interval
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Note: We don't skip the first tick - we want to refresh immediately on startup.
        // The distributed lock prevents multiple nodes from refreshing simultaneously.

        let prefix = &state.config.key_prefix;
        let lock_ttl = state.config.refresh_lock_ttl_secs();

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("cache refresh: received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    // Try to acquire distributed lock
                    let lock_key = format!("{}lock:refresh", prefix);
                    let acquired = Self::try_acquire_lock_inner(&state, &lock_key, &node_id, lock_ttl).await;
                    if !acquired {
                        continue;
                    }

                    // Channel to signal lock loss
                    let (lock_lost_tx, lock_lost_rx) = watch::channel(false);

                    // Start heartbeat
                    let heartbeat_handle =
                        Self::start_heartbeat_inner(&state, lock_key.clone(), node_id.clone(), lock_lost_tx, lock_ttl);

                    // Run refresh phases
                    Self::run_refresh_phases_inner(&state, lock_lost_rx).await;

                    heartbeat_handle.abort();

                    // Release the lock so other nodes can run refresh sooner
                    Self::release_lock_inner(&state, &lock_key, &node_id).await;
                }
            }
        }
    }

    /// Try to acquire the distributed refresh lock.
    async fn try_acquire_lock_inner(
        state: &CachingCatalogManagerInner,
        lock_key: &str,
        node_id: &str,
        lock_ttl: u64,
    ) -> bool {
        let result: Result<bool, _> = redis::cmd("SET")
            .arg(lock_key)
            .arg(node_id)
            .arg("NX")
            .arg("EX")
            .arg(lock_ttl)
            .query_async(&mut state.redis.clone())
            .await;

        result.unwrap_or(false)
    }

    /// Release the refresh lock after successful completion.
    async fn release_lock_inner(state: &CachingCatalogManagerInner, lock_key: &str, node_id: &str) {
        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;
        let _: Result<i32, _> = redis::cmd("EVAL")
            .arg(script)
            .arg(1)
            .arg(lock_key)
            .arg(node_id)
            .query_async(&mut state.redis.clone())
            .await;
    }

    /// Start heartbeat task to extend lock TTL.
    fn start_heartbeat_inner(
        state: &Arc<CachingCatalogManagerInner>,
        lock_key: String,
        node_id: String,
        lock_lost_tx: watch::Sender<bool>,
        lock_ttl: u64,
    ) -> tokio::task::JoinHandle<()> {
        let redis = state.redis.clone();

        tokio::spawn(async move {
            let extend_script = r#"
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("EXPIRE", KEYS[1], ARGV[2])
                else
                    return 0
                end
            "#;

            loop {
                tokio::time::sleep(Duration::from_secs(lock_ttl / 2)).await;

                let result: Result<i32, _> = redis::cmd("EVAL")
                    .arg(extend_script)
                    .arg(1)
                    .arg(&lock_key)
                    .arg(&node_id)
                    .arg(lock_ttl)
                    .query_async(&mut redis.clone())
                    .await;

                match result {
                    Ok(1) => continue,
                    _ => {
                        warn!("cache refresh: lock lost or heartbeat failed");
                        let _ = lock_lost_tx.send(true);
                        break;
                    }
                }
            }
        })
    }

    /// Run all refresh phases with lock-loss checking.
    async fn run_refresh_phases_inner(
        state: &CachingCatalogManagerInner,
        lock_lost_rx: watch::Receiver<bool>,
    ) {
        // Helper to check if lock was lost
        macro_rules! check_lock {
            () => {
                if *lock_lost_rx.borrow() {
                    warn!("cache refresh: aborting due to lock loss");
                    return;
                }
            };
        }

        let prefix = &state.config.key_prefix;

        // Phase 1: Refresh connections
        let conns = match state.inner.list_connections().await {
            Ok(conns) => {
                check_lock!();
                let key = format!("{}conn:list", prefix);
                if let Err(e) = Self::cache_set_inner(state, &key, &conns).await {
                    warn!("cache refresh: failed to set conn:list: {}", e);
                }
                for conn in &conns {
                    check_lock!();
                    let key_id = format!("{}conn:{}", prefix, encode_key_segment(&conn.id));
                    let key_name =
                        format!("{}conn:name:{}", prefix, encode_key_segment(&conn.name));
                    let _ = Self::cache_set_inner(state, &key_id, conn).await;
                    let _ = Self::cache_set_inner(state, &key_name, conn).await;
                }
                conns
            }
            Err(e) => {
                warn!("cache refresh: failed to list connections: {}", e);
                return;
            }
        };

        // Phase 2: Refresh global table list
        check_lock!();
        if let Ok(tables) = state.inner.list_tables(None).await {
            let key = format!("{}tbl:list:all", prefix);
            let _ = Self::cache_set_inner(state, &key, &tables).await;
        }

        // Phase 3: Refresh per-connection tables
        for conn in &conns {
            check_lock!();
            if let Ok(tables) = state.inner.list_tables(Some(&conn.id)).await {
                let key = format!("{}tbl:list:conn:{}", prefix, encode_key_segment(&conn.id));
                let _ = Self::cache_set_inner(state, &key, &tables).await;
                for table in &tables {
                    check_lock!();
                    let key = format!(
                        "{}tbl:{}:{}:{}",
                        prefix,
                        encode_key_segment(&conn.id),
                        encode_key_segment(&table.schema_name),
                        encode_key_segment(&table.table_name)
                    );
                    let _ = Self::cache_set_inner(state, &key, table).await;
                }
            }
        }

        info!("cache refresh: completed successfully");
    }

    /// Set a value in the cache with TTL (static version for warmup loop).
    async fn cache_set_inner<T: Serialize>(
        state: &CachingCatalogManagerInner,
        key: &str,
        data: &T,
    ) -> Result<()> {
        let entry = CachedEntry {
            data,
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        let json = serde_json::to_string(&entry)?;

        let _: () = state
            .redis
            .clone()
            .set_ex(key, json, state.config.ttl_secs)
            .await
            .map_err(|e| anyhow!("redis set failed: {}", e))?;

        Ok(())
    }
}

#[async_trait]
impl CatalogManager for CachingCatalogManager {
    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle methods
    // ─────────────────────────────────────────────────────────────────────────

    async fn init(&self) -> Result<()> {
        // Start refresh loop if configured (guard against duplicate calls)
        if self.config().refresh_interval_secs > 0 {
            let guard = self.state.refresh_handle.lock().unwrap();
            if guard.is_some() {
                // Already initialized
                return Ok(());
            }
            let node_id = format!("runtimedb-{}", uuid::Uuid::new_v4());
            info!(
                "Starting cache refresh loop with interval {}s",
                self.config().refresh_interval_secs
            );
            drop(guard); // Release lock before spawning
            self.start_refresh_loop(node_id);
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // Signal shutdown to refresh loop
        self.state.shutdown_token.cancel();

        // Take the handle out of the mutex (drop the guard before awaiting)
        let handle = self.state.refresh_handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }

        self.inner().close().await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Passthrough methods (no caching)
    // ─────────────────────────────────────────────────────────────────────────

    async fn run_migrations(&self) -> Result<()> {
        self.inner().run_migrations().await
    }

    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        let id = self
            .inner()
            .add_connection(name, source_type, config_json, secret_id)
            .await?;

        // Write-through: update connection list cache
        if let Ok(conns) = self.inner().list_connections().await {
            let _ = self.cache_set(&self.key_conn_list(), &conns).await;
        }

        // Cache the new connection
        if let Ok(Some(conn)) = self.inner().get_connection(&id).await {
            let _ = self.cache_set(&self.key_conn_id(&id), &conn).await;
            let _ = self.cache_set(&self.key_conn_name(name), &conn).await;
        }

        Ok(id)
    }

    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        let id = self
            .inner()
            .add_table(connection_id, schema_name, table_name, arrow_schema_json)
            .await?;

        // Write-through: update table caches
        if let Ok(Some(table)) = self
            .inner()
            .get_table(connection_id, schema_name, table_name)
            .await
        {
            let _ = self
                .cache_set(
                    &self.key_tbl(connection_id, schema_name, table_name),
                    &table,
                )
                .await;
        }
        if let Ok(tables) = self.inner().list_tables(Some(connection_id)).await {
            let _ = self
                .cache_set(&self.key_tbl_list(Some(connection_id)), &tables)
                .await;
        }
        if let Ok(tables) = self.inner().list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }
        if let Ok(names) = self.inner().list_dataset_table_names(schema_name).await {
            let _ = self
                .cache_set(&self.key_tbl_names(schema_name), &names)
                .await;
        }

        Ok(id)
    }

    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.inner()
            .update_table_sync(table_id, parquet_path)
            .await?;

        // Refresh global table list and find the updated table to get its identifiers
        if let Ok(tables) = self.inner().list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;

            // Find the table by ID to get connection_id/schema/table for cache invalidation
            if let Some(table) = tables.iter().find(|t| t.id == table_id) {
                // Invalidate per-table cache
                let key = self.key_tbl(&table.connection_id, &table.schema_name, &table.table_name);
                self.cache_del(&key).await;

                // Invalidate per-connection list cache
                self.cache_del(&self.key_tbl_list(Some(&table.connection_id)))
                    .await;
            }
        }

        Ok(())
    }

    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let table = self
            .inner()
            .clear_table_cache_metadata(connection_id, schema_name, table_name)
            .await?;

        // Write-through: update table caches
        let _ = self
            .cache_set(
                &self.key_tbl(connection_id, schema_name, table_name),
                &table,
            )
            .await;
        if let Ok(tables) = self.inner().list_tables(Some(connection_id)).await {
            let _ = self
                .cache_set(&self.key_tbl_list(Some(connection_id)), &tables)
                .await;
        }
        if let Ok(tables) = self.inner().list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        Ok(table)
    }

    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.inner()
            .clear_connection_cache_metadata(connection_id)
            .await?;

        // Write-through: update table list caches
        if let Ok(tables) = self.inner().list_tables(Some(connection_id)).await {
            let _ = self
                .cache_set(&self.key_tbl_list(Some(connection_id)), &tables)
                .await;
        }
        if let Ok(tables) = self.inner().list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        // Invalidate individual table caches for this connection
        let pattern = format!(
            "{}tbl:{}:*",
            self.prefix(),
            encode_key_segment(connection_id)
        );
        self.cache_del_pattern(&pattern).await;

        Ok(())
    }

    async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        // Fetch connection info before deletion (needed for name-based cache key)
        let conn_info = self.inner().get_connection(connection_id).await?;

        self.inner().delete_connection(connection_id).await?;

        // Write-through: update connection list cache
        if let Ok(conns) = self.inner().list_connections().await {
            let _ = self.cache_set(&self.key_conn_list(), &conns).await;
        }

        // Delete connection cache keys
        self.cache_del(&self.key_conn_id(connection_id)).await;
        if let Some(conn) = conn_info {
            self.cache_del(&self.key_conn_name(&conn.name)).await;
        }

        // Write-through: update global table list
        if let Ok(tables) = self.inner().list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        // Delete all table caches for this connection
        self.cache_del(&self.key_tbl_list(Some(connection_id)))
            .await;
        // Note: We don't delete key_tbl_names here because it's schema-scoped, not connection-scoped.
        // Dataset table names cache is keyed by schema_name, not connection_id.
        let pattern = format!(
            "{}tbl:{}:*",
            self.prefix(),
            encode_key_segment(connection_id)
        );
        self.cache_del_pattern(&pattern).await;

        Ok(())
    }

    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()> {
        self.inner()
            .schedule_file_deletion(path, delete_after)
            .await
    }

    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>> {
        self.inner().get_pending_deletions().await
    }

    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        self.inner().increment_deletion_retry(id).await
    }

    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.inner().remove_pending_deletion(id).await
    }

    // Secret management methods - metadata

    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>> {
        self.inner().get_secret_metadata(name).await
    }

    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        self.inner().get_secret_metadata_any_status(name).await
    }

    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()> {
        self.inner().create_secret_metadata(metadata).await
    }

    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool> {
        self.inner().update_secret_metadata(metadata, lock).await
    }

    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        self.inner().set_secret_status(name, status).await
    }

    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        self.inner().delete_secret_metadata(name).await
    }

    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>> {
        self.inner().list_secrets().await
    }

    // Secret management methods - encrypted storage

    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        self.inner().get_encrypted_secret(secret_id).await
    }

    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()> {
        self.inner()
            .put_encrypted_secret_value(secret_id, encrypted_value)
            .await
    }

    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool> {
        self.inner().delete_encrypted_secret_value(secret_id).await
    }

    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>> {
        self.inner().get_secret_metadata_by_id(id).await
    }

    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.inner().count_connections_by_secret_id(secret_id).await
    }

    // Query result persistence methods

    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        self.inner().store_result(result).await
    }

    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner().get_result(id).await
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        self.inner().list_results(limit, offset).await
    }

    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        self.inner().store_result_pending(id, created_at).await
    }

    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        self.inner().finalize_result(id, parquet_path).await
    }

    async fn fail_result(&self, id: &str, error_message: Option<&str>) -> Result<()> {
        self.inner().fail_result(id, error_message).await
    }

    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner().get_queryable_result(id).await
    }

    async fn cleanup_stale_results(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.inner().cleanup_stale_results(cutoff).await
    }

    // Upload management methods

    async fn create_upload(&self, upload: &UploadInfo) -> Result<()> {
        self.inner().create_upload(upload).await
    }

    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>> {
        self.inner().get_upload(id).await
    }

    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>> {
        self.inner().list_uploads(status).await
    }

    async fn consume_upload(&self, id: &str) -> Result<bool> {
        self.inner().consume_upload(id).await
    }

    async fn claim_upload(&self, id: &str) -> Result<bool> {
        self.inner().claim_upload(id).await
    }

    async fn release_upload(&self, id: &str) -> Result<bool> {
        self.inner().release_upload(id).await
    }

    // Dataset management methods (with cache invalidation for list_dataset_table_names)

    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
        self.inner().create_dataset(dataset).await?;

        // Invalidate dataset table names cache for the affected schema
        self.cache_del(&self.key_tbl_names(&dataset.schema_name))
            .await;

        Ok(())
    }

    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        self.inner().get_dataset(id).await
    }

    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>> {
        self.inner()
            .get_dataset_by_table_name(schema_name, table_name)
            .await
    }

    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)> {
        self.inner().list_datasets(limit, offset).await
    }

    async fn list_all_datasets(&self) -> Result<Vec<DatasetInfo>> {
        self.inner().list_all_datasets().await
    }

    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
        // Get dataset before update to know which schema to invalidate
        let dataset = self.inner().get_dataset(id).await?;

        let result = self.inner().update_dataset(id, label, table_name).await?;

        // Invalidate dataset table names cache for the affected schema
        if let Some(ds) = dataset {
            self.cache_del(&self.key_tbl_names(&ds.schema_name)).await;
        }

        Ok(result)
    }

    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        let deleted = self.inner().delete_dataset(id).await?;

        // Invalidate dataset table names cache for the affected schema
        if let Some(ref ds) = deleted {
            self.cache_del(&self.key_tbl_names(&ds.schema_name)).await;
        }

        Ok(deleted)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cached read methods
    // ─────────────────────────────────────────────────────────────────────────

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let key = self.key_conn_list();
        self.cached_read(&key, || self.inner().list_connections())
            .await
    }

    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_id(id);
        let id_owned = id.to_string();
        self.cached_read(&key, || self.inner().get_connection(&id_owned))
            .await
    }

    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_name(name);
        let name_owned = name.to_string();
        self.cached_read(&key, || self.inner().get_connection_by_name(&name_owned))
            .await
    }

    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        let key = self.key_tbl_list(connection_id);
        let conn_id = connection_id.map(|s| s.to_string());
        self.cached_read(&key, || {
            let conn_ref = conn_id.as_deref();
            self.inner().list_tables(conn_ref)
        })
        .await
    }

    async fn get_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        let key = self.key_tbl(connection_id, schema_name, table_name);
        let conn = connection_id.to_string();
        let schema = schema_name.to_string();
        let table = table_name.to_string();
        self.cached_read(&key, || self.inner().get_table(&conn, &schema, &table))
            .await
    }

    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let key = self.key_tbl_names(schema_name);
        let schema = schema_name.to_string();
        self.cached_read(&key, || self.inner().list_dataset_table_names(&schema))
            .await
    }
}
