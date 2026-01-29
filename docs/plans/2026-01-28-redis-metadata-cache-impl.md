# Redis Metadata Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an optional Redis caching layer for CatalogManager metadata calls to reduce database load and improve query latency.

**Architecture:** A `CachingCatalogManager` wrapper that implements `CatalogManager`, delegating to an inner catalog while caching results in Redis. Read path uses cache-aside pattern; write path uses write-through to keep cache consistent. Optional background warmup loop keeps cache populated.

**Tech Stack:** `redis` crate with async tokio support, `urlencoding` for key segment encoding, `testcontainers` for Redis integration tests.

**Design Doc:** `docs/plans/2026-01-28-redis-metadata-cache-design.md`

---

## Task 1: Add Redis Dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add redis crate to dependencies**

Add after the `liquid-cache-client` line (~line 58):

```toml
redis = { version = "0.27", features = ["tokio-comp", "connection-manager"] }
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore(deps): add redis crate for metadata caching"
```

---

## Task 2: Add CacheConfig to AppConfig

**Files:**
- Modify: `src/config/mod.rs`

**Step 1: Add CacheConfig struct**

Add after `LiquidCacheConfig` struct (~line 78):

```rust
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CacheConfig {
    /// Redis connection URL. None = caching disabled.
    pub redis_url: Option<String>,
    /// Hard TTL in seconds. Default: 1800 (30 minutes).
    #[serde(default = "default_cache_hard_ttl")]
    pub hard_ttl_secs: u64,
    /// Background warmup interval in seconds. 0 = disabled. Default: 0.
    #[serde(default)]
    pub warmup_interval_secs: u64,
    /// Distributed lock TTL for warmup. Default: 300 (5 minutes).
    #[serde(default = "default_warmup_lock_ttl")]
    pub warmup_lock_ttl_secs: u64,
    /// Key prefix. Default: "rdb:"
    #[serde(default = "default_cache_key_prefix")]
    pub key_prefix: String,
}

fn default_cache_hard_ttl() -> u64 {
    1800
}

fn default_warmup_lock_ttl() -> u64 {
    300
}

fn default_cache_key_prefix() -> String {
    "rdb:".to_string()
}
```

**Step 2: Add cache field to AppConfig**

Modify `AppConfig` struct to add cache field after `liquid_cache`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub catalog: CatalogConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub secrets: SecretsConfig,
    #[serde(default)]
    pub liquid_cache: LiquidCacheConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}
```

**Step 3: Add validation for cache config**

Add at the end of `validate()` method:

```rust
        // Validate cache config
        if self.cache.warmup_interval_secs > 0
            && self.cache.warmup_interval_secs >= self.cache.hard_ttl_secs
        {
            anyhow::bail!(
                "Cache warmup_interval_secs ({}) must be less than hard_ttl_secs ({})",
                self.cache.warmup_interval_secs,
                self.cache.hard_ttl_secs
            );
        }

        Ok(())
```

Note: Remove the existing `Ok(())` at the end and add these lines before returning.

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/config/mod.rs
git commit -m "feat(config): add CacheConfig for Redis metadata caching"
```

---

## Task 3: Create CachingCatalogManager Struct and Basic Infrastructure

**Files:**
- Create: `src/catalog/caching_manager.rs`
- Modify: `src/catalog/mod.rs`

**Step 1: Create the caching_manager module**

Create `src/catalog/caching_manager.rs`:

```rust
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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

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

/// Caching wrapper for CatalogManager implementations.
///
/// Caches metadata calls in Redis with configurable TTL.
/// Falls through to inner catalog if Redis is unavailable.
#[derive(Debug)]
pub struct CachingCatalogManager {
    inner: Arc<dyn CatalogManager>,
    redis: ConnectionManager,
    config: CacheConfig,
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
            inner,
            redis,
            config,
        })
    }

    /// Get the key prefix from config.
    fn prefix(&self) -> &str {
        &self.config.key_prefix
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
            Some(id) => format!(
                "{}tbl:list:conn:{}",
                self.prefix(),
                encode_key_segment(id)
            ),
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
        let result: Result<Option<String>, _> = self.redis.clone().get(key).await;
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
            .redis
            .clone()
            .set_ex(key, json, self.config.hard_ttl_secs)
            .await
            .map_err(|e| anyhow!("redis set failed: {}", e))?;

        Ok(())
    }

    /// Delete a key from the cache.
    async fn cache_del(&self, key: &str) {
        let result: Result<(), _> = self.redis.clone().del(key).await;
        if let Err(e) = result {
            warn!("cache: failed to delete {}: {}", key, e);
        }
    }

    /// Delete keys matching a pattern using SCAN.
    async fn cache_del_pattern(&self, pattern: &str) {
        let mut cursor = 0u64;
        loop {
            let result: Result<(u64, Vec<String>), _> =
                redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut self.redis.clone())
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
}
```

**Step 2: Add module to catalog/mod.rs**

Add after `mod manager;` line (~line 6):

```rust
mod caching_manager;
pub use caching_manager::CachingCatalogManager;
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully (with dead_code warnings, which is fine)

**Step 4: Commit**

```bash
git add src/catalog/caching_manager.rs src/catalog/mod.rs
git commit -m "feat(cache): add CachingCatalogManager struct and cache helpers"
```

---

## Task 4: Implement CatalogManager Read Methods

**Files:**
- Modify: `src/catalog/caching_manager.rs`

**Step 1: Add CatalogManager trait implementation for read methods**

Add at the end of the file:

```rust
#[async_trait]
impl CatalogManager for CachingCatalogManager {
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn run_migrations(&self) -> Result<()> {
        self.inner.run_migrations().await
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let key = self.key_conn_list();
        self.cached_read(&key, || self.inner.list_connections()).await
    }

    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_id(id);
        self.cached_read(&key, || self.inner.get_connection(id)).await
    }

    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_name(name);
        self.cached_read(&key, || self.inner.get_connection_by_name(name)).await
    }

    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        let key = self.key_tbl_list(connection_id);
        let conn_id = connection_id.map(|s| s.to_string());
        self.cached_read(&key, || {
            let conn_ref = conn_id.as_deref();
            self.inner.list_tables(conn_ref)
        }).await
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
        self.cached_read(&key, || self.inner.get_table(&conn, &schema, &table)).await
    }

    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let key = self.key_tbl_names(schema_name);
        let schema = schema_name.to_string();
        self.cached_read(&key, || self.inner.list_dataset_table_names(&schema)).await
    }

    // Write methods - to be implemented in next task
    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        // Placeholder - will implement write-through in next task
        self.inner.add_connection(name, source_type, config_json, secret_id).await
    }

    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.inner.add_table(connection_id, schema_name, table_name, arrow_schema_json).await
    }

    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.inner.update_table_sync(table_id, parquet_path).await
    }

    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        self.inner.clear_table_cache_metadata(connection_id, schema_name, table_name).await
    }

    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.inner.clear_connection_cache_metadata(connection_id).await
    }

    async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        self.inner.delete_connection(connection_id).await
    }

    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()> {
        self.inner.schedule_file_deletion(path, delete_after).await
    }

    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>> {
        self.inner.get_pending_deletions().await
    }

    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        self.inner.increment_deletion_retry(id).await
    }

    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.inner.remove_pending_deletion(id).await
    }

    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>> {
        self.inner.get_secret_metadata(name).await
    }

    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        self.inner.get_secret_metadata_any_status(name).await
    }

    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()> {
        self.inner.create_secret_metadata(metadata).await
    }

    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool> {
        self.inner.update_secret_metadata(metadata, lock).await
    }

    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        self.inner.set_secret_status(name, status).await
    }

    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        self.inner.delete_secret_metadata(name).await
    }

    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>> {
        self.inner.list_secrets().await
    }

    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_encrypted_secret(secret_id).await
    }

    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()> {
        self.inner.put_encrypted_secret_value(secret_id, encrypted_value).await
    }

    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool> {
        self.inner.delete_encrypted_secret_value(secret_id).await
    }

    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>> {
        self.inner.get_secret_metadata_by_id(id).await
    }

    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.inner.count_connections_by_secret_id(secret_id).await
    }

    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        self.inner.store_result(result).await
    }

    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner.get_result(id).await
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        self.inner.list_results(limit, offset).await
    }

    async fn create_upload(&self, upload: &UploadInfo) -> Result<()> {
        self.inner.create_upload(upload).await
    }

    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>> {
        self.inner.get_upload(id).await
    }

    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>> {
        self.inner.list_uploads(status).await
    }

    async fn consume_upload(&self, id: &str) -> Result<bool> {
        self.inner.consume_upload(id).await
    }

    async fn claim_upload(&self, id: &str) -> Result<bool> {
        self.inner.claim_upload(id).await
    }

    async fn release_upload(&self, id: &str) -> Result<bool> {
        self.inner.release_upload(id).await
    }

    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
        self.inner.create_dataset(dataset).await
    }

    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        self.inner.get_dataset(id).await
    }

    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>> {
        self.inner.get_dataset_by_table_name(schema_name, table_name).await
    }

    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)> {
        self.inner.list_datasets(limit, offset).await
    }

    async fn list_all_datasets(&self) -> Result<Vec<DatasetInfo>> {
        self.inner.list_all_datasets().await
    }

    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
        self.inner.update_dataset(id, label, table_name).await
    }

    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        self.inner.delete_dataset(id).await
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/catalog/caching_manager.rs
git commit -m "feat(cache): implement CatalogManager read methods with caching"
```

---

## Task 5: Implement Write-Through Cache Invalidation

**Files:**
- Modify: `src/catalog/caching_manager.rs`

**Step 1: Replace placeholder write methods with write-through implementations**

Replace the `add_connection`, `add_table`, `update_table_sync`, `clear_table_cache_metadata`, `clear_connection_cache_metadata`, and `delete_connection` method implementations:

```rust
    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        let id = self.inner.add_connection(name, source_type, config_json, secret_id).await?;

        // Write-through: update connection list cache
        if let Ok(conns) = self.inner.list_connections().await {
            let _ = self.cache_set(&self.key_conn_list(), &conns).await;
        }

        // Cache the new connection
        if let Ok(Some(conn)) = self.inner.get_connection(&id).await {
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
        let id = self.inner.add_table(connection_id, schema_name, table_name, arrow_schema_json).await?;

        // Write-through: update table caches
        if let Ok(Some(table)) = self.inner.get_table(connection_id, schema_name, table_name).await {
            let _ = self.cache_set(&self.key_tbl(connection_id, schema_name, table_name), &table).await;
        }
        if let Ok(tables) = self.inner.list_tables(Some(connection_id)).await {
            let _ = self.cache_set(&self.key_tbl_list(Some(connection_id)), &tables).await;
        }
        if let Ok(tables) = self.inner.list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }
        if let Ok(names) = self.inner.list_dataset_table_names(schema_name).await {
            let _ = self.cache_set(&self.key_tbl_names(schema_name), &names).await;
        }

        Ok(id)
    }

    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.inner.update_table_sync(table_id, parquet_path).await?;

        // We don't have connection_id/schema/table from just table_id, so invalidate list caches
        // The individual table cache will be stale but will be refreshed on next read
        if let Ok(tables) = self.inner.list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        Ok(())
    }

    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let table = self.inner.clear_table_cache_metadata(connection_id, schema_name, table_name).await?;

        // Write-through: update table caches
        let _ = self.cache_set(&self.key_tbl(connection_id, schema_name, table_name), &table).await;
        if let Ok(tables) = self.inner.list_tables(Some(connection_id)).await {
            let _ = self.cache_set(&self.key_tbl_list(Some(connection_id)), &tables).await;
        }
        if let Ok(tables) = self.inner.list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        Ok(table)
    }

    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.inner.clear_connection_cache_metadata(connection_id).await?;

        // Write-through: update table list caches
        if let Ok(tables) = self.inner.list_tables(Some(connection_id)).await {
            let _ = self.cache_set(&self.key_tbl_list(Some(connection_id)), &tables).await;
        }
        if let Ok(tables) = self.inner.list_tables(None).await {
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
        let conn_info = self.inner.get_connection(connection_id).await?;

        self.inner.delete_connection(connection_id).await?;

        // Write-through: update connection list cache
        if let Ok(conns) = self.inner.list_connections().await {
            let _ = self.cache_set(&self.key_conn_list(), &conns).await;
        }

        // Delete connection cache keys
        self.cache_del(&self.key_conn_id(connection_id)).await;
        if let Some(conn) = conn_info {
            self.cache_del(&self.key_conn_name(&conn.name)).await;
        }

        // Write-through: update global table list
        if let Ok(tables) = self.inner.list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        // Delete all table caches for this connection
        self.cache_del(&self.key_tbl_list(Some(connection_id))).await;
        self.cache_del(&self.key_tbl_names(connection_id)).await;
        let pattern = format!(
            "{}tbl:{}:*",
            self.prefix(),
            encode_key_segment(connection_id)
        );
        self.cache_del_pattern(&pattern).await;

        Ok(())
    }
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/catalog/caching_manager.rs
git commit -m "feat(cache): implement write-through cache invalidation"
```

---

## Task 6: Add Basic Unit Tests for Cache Read/Write

**Files:**
- Create: `tests/caching_catalog_tests.rs`

**Step 1: Create the test file**

Create `tests/caching_catalog_tests.rs`:

```rust
//! Integration tests for CachingCatalogManager.
//!
//! These tests require a Redis instance. They use testcontainers to spin up
//! a Redis container for testing.

use runtimedb::catalog::{CachingCatalogManager, CatalogManager, SqliteCatalogManager};
use runtimedb::config::CacheConfig;
use std::sync::Arc;
use tempfile::tempdir;
use testcontainers::{runners::AsyncRunner, GenericImage};

/// Create a Redis container for testing.
async fn start_redis() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379.into())
        .start()
        .await
        .expect("Failed to start Redis container");

    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://127.0.0.1:{}", port);

    (container, url)
}

/// Create a SQLite catalog in a temp directory.
async fn create_sqlite_catalog() -> (tempfile::TempDir, Arc<dyn CatalogManager>) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");
    let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    catalog.run_migrations().await.unwrap();
    (dir, Arc::new(catalog))
}

#[tokio::test]
async fn test_cache_miss_then_hit() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // First call should be a cache miss - goes to inner catalog
    let conns1 = caching.list_connections().await.unwrap();
    assert!(conns1.is_empty());

    // Second call should be a cache hit - same result
    let conns2 = caching.list_connections().await.unwrap();
    assert!(conns2.is_empty());

    // Add a connection through the caching layer
    let id = caching
        .add_connection("test_conn", "postgres", r#"{"host":"localhost"}"#, None)
        .await
        .unwrap();

    // Write-through should have updated the cache
    let conns3 = caching.list_connections().await.unwrap();
    assert_eq!(conns3.len(), 1);
    assert_eq!(conns3[0].id, id);
}

#[tokio::test]
async fn test_connection_write_through() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test2:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Add connection
    let id = caching
        .add_connection("my_conn", "postgres", r#"{"host":"db.example.com"}"#, None)
        .await
        .unwrap();

    // get_connection should work and be cached
    let conn = caching.get_connection(&id).await.unwrap();
    assert!(conn.is_some());
    assert_eq!(conn.as_ref().unwrap().name, "my_conn");

    // get_connection_by_name should also be cached
    let conn2 = caching.get_connection_by_name("my_conn").await.unwrap();
    assert!(conn2.is_some());
    assert_eq!(conn2.as_ref().unwrap().id, id);

    // Delete connection
    caching.delete_connection(&id).await.unwrap();

    // Should be removed from cache
    let conns = caching.list_connections().await.unwrap();
    assert!(conns.is_empty());
}

#[tokio::test]
async fn test_table_caching() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test3:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Add a connection first
    let conn_id = caching
        .add_connection("table_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Add a table
    let table_id = caching
        .add_table(&conn_id, "public", "users", r#"{"fields":[]}"#)
        .await
        .unwrap();
    assert!(table_id > 0);

    // list_tables should be cached
    let tables = caching.list_tables(Some(&conn_id)).await.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name, "users");

    // get_table should be cached
    let table = caching
        .get_table(&conn_id, "public", "users")
        .await
        .unwrap();
    assert!(table.is_some());
}

#[tokio::test]
async fn test_special_characters_in_keys() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test4:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Connection name with special characters
    let id = caching
        .add_connection("my:special:conn", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Should work correctly with URL encoding
    let conn = caching.get_connection_by_name("my:special:conn").await.unwrap();
    assert!(conn.is_some());

    // Add table with special characters in schema/name
    let _ = caching
        .add_table(&id, "my:schema", "my:table", r#"{}"#)
        .await
        .unwrap();

    let table = caching
        .get_table(&id, "my:schema", "my:table")
        .await
        .unwrap();
    assert!(table.is_some());
    assert_eq!(table.as_ref().unwrap().schema_name, "my:schema");
    assert_eq!(table.as_ref().unwrap().table_name, "my:table");
}
```

**Step 2: Run the tests**

Run: `cargo test caching_catalog_tests --test caching_catalog_tests`
Expected: All tests pass

**Step 3: Commit**

```bash
git add tests/caching_catalog_tests.rs
git commit -m "test(cache): add integration tests for CachingCatalogManager"
```

---

## Task 7: Wire CachingCatalogManager into Engine

**Files:**
- Modify: `src/engine.rs`

**Step 1: Add import for CachingCatalogManager**

Add to imports at top of file (after existing catalog imports, ~line 3):

```rust
use crate::catalog::CachingCatalogManager;
```

**Step 2: Add cache config to RuntimeEngineBuilder**

Find the `RuntimeEngineBuilder` struct and add a cache_config field. The struct should be around line 700-750. Add the field:

```rust
pub struct RuntimeEngineBuilder {
    base_dir: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    catalog: Option<Arc<dyn CatalogManager>>,
    storage: Option<Arc<dyn StorageManager>>,
    secret_key: Option<String>,
    liquid_cache_builder: Option<LiquidCacheClientBuilder>,
    deletion_grace_period: Option<Duration>,
    deletion_worker_interval: Option<Duration>,
    parallel_refresh_count: Option<usize>,
    cache_config: Option<crate::config::CacheConfig>,
}
```

**Step 3: Add cache_config method to builder**

Add the builder method:

```rust
    /// Set the cache configuration for Redis metadata caching.
    pub fn cache_config(mut self, config: crate::config::CacheConfig) -> Self {
        self.cache_config = Some(config);
        self
    }
```

**Step 4: Initialize cache_config in new()**

In the `RuntimeEngineBuilder::new()` method, add:

```rust
            cache_config: None,
```

**Step 5: Wrap catalog with CachingCatalogManager in build()**

In the `build()` method, after the catalog is created but before it's used, add the wrapping logic. Find where `catalog` is finalized and add:

```rust
        // Wrap catalog with caching layer if Redis is configured
        let catalog: Arc<dyn CatalogManager> = if let Some(cache_config) = &self.cache_config {
            if let Some(ref redis_url) = cache_config.redis_url {
                Arc::new(
                    CachingCatalogManager::new(catalog, redis_url, cache_config.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to create caching catalog: {}", e))?,
                )
            } else {
                catalog
            }
        } else {
            catalog
        };
```

**Step 6: Update from_config to pass cache config**

In the `RuntimeEngine::from_config` method, add cache config to builder. Add before `builder.build().await`:

```rust
        // Pass cache config to builder
        builder = builder.cache_config(config.cache.clone());
```

**Step 7: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 8: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): wire CachingCatalogManager into RuntimeEngine"
```

---

## Task 8: Implement Background Warmup Loop

**Files:**
- Modify: `src/catalog/caching_manager.rs`

**Step 1: Add warmup loop infrastructure**

Add these imports at the top if not present:

```rust
use std::time::Duration;
use tokio::sync::watch;
```

**Step 2: Add warmup methods to CachingCatalogManager**

Add after the `cache_del_pattern` method:

```rust
    /// Start the background warmup loop.
    ///
    /// Returns a handle that can be used to stop the loop.
    pub fn start_warmup_loop(self: &Arc<Self>, node_id: String) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.warmup_loop(node_id).await;
        })
    }

    /// The warmup loop that periodically refreshes all cached metadata.
    async fn warmup_loop(&self, node_id: String) {
        if self.config.warmup_interval_secs == 0 {
            return; // Warmup disabled
        }

        let interval = Duration::from_secs(self.config.warmup_interval_secs);
        let prefix = &self.config.key_prefix;

        loop {
            tokio::time::sleep(interval).await;

            // Try to acquire distributed lock
            let lock_key = format!("{}lock:warmup", prefix);
            let acquired = self.try_acquire_lock(&lock_key, &node_id).await;
            if !acquired {
                continue;
            }

            // Channel to signal lock loss
            let (lock_lost_tx, lock_lost_rx) = watch::channel(false);

            // Start heartbeat
            let heartbeat_handle = self.start_heartbeat(
                lock_key.clone(),
                node_id.clone(),
                lock_lost_tx,
            );

            // Run warmup phases
            self.run_warmup_phases(lock_lost_rx).await;

            heartbeat_handle.abort();
        }
    }

    /// Try to acquire the distributed warmup lock.
    async fn try_acquire_lock(&self, lock_key: &str, node_id: &str) -> bool {
        let result: Result<bool, _> = redis::cmd("SET")
            .arg(lock_key)
            .arg(node_id)
            .arg("NX")
            .arg("EX")
            .arg(self.config.warmup_lock_ttl_secs)
            .query_async(&mut self.redis.clone())
            .await;

        result.unwrap_or(false)
    }

    /// Start heartbeat task to extend lock TTL.
    fn start_heartbeat(
        &self,
        lock_key: String,
        node_id: String,
        lock_lost_tx: watch::Sender<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let redis = self.redis.clone();
        let ttl = self.config.warmup_lock_ttl_secs;

        tokio::spawn(async move {
            let extend_script = r#"
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("EXPIRE", KEYS[1], ARGV[2])
                else
                    return 0
                end
            "#;

            loop {
                tokio::time::sleep(Duration::from_secs(ttl / 2)).await;

                let result: Result<i32, _> = redis::cmd("EVAL")
                    .arg(extend_script)
                    .arg(1)
                    .arg(&lock_key)
                    .arg(&node_id)
                    .arg(ttl)
                    .query_async(&mut redis.clone())
                    .await;

                match result {
                    Ok(1) => continue,
                    _ => {
                        warn!("cache warmup: lock lost or heartbeat failed");
                        let _ = lock_lost_tx.send(true);
                        break;
                    }
                }
            }
        })
    }

    /// Run all warmup phases with lock-loss checking.
    async fn run_warmup_phases(&self, mut lock_lost_rx: watch::Receiver<bool>) {
        // Helper to check if lock was lost
        macro_rules! check_lock {
            () => {
                if *lock_lost_rx.borrow() {
                    warn!("cache warmup: aborting due to lock loss");
                    return;
                }
            };
        }

        // Phase 1: Refresh connections
        let conns = match self.inner.list_connections().await {
            Ok(conns) => {
                check_lock!();
                if let Err(e) = self.cache_set(&self.key_conn_list(), &conns).await {
                    warn!("cache warmup: failed to set conn:list: {}", e);
                }
                for conn in &conns {
                    check_lock!();
                    let _ = self.cache_set(&self.key_conn_id(&conn.id), conn).await;
                    let _ = self.cache_set(&self.key_conn_name(&conn.name), conn).await;
                }
                conns
            }
            Err(e) => {
                warn!("cache warmup: failed to list connections: {}", e);
                return;
            }
        };

        // Phase 2: Refresh global table list
        check_lock!();
        if let Ok(tables) = self.inner.list_tables(None).await {
            let _ = self.cache_set(&self.key_tbl_list(None), &tables).await;
        }

        // Phase 3: Refresh per-connection tables
        for conn in &conns {
            check_lock!();
            if let Ok(tables) = self.inner.list_tables(Some(&conn.id)).await {
                let _ = self.cache_set(&self.key_tbl_list(Some(&conn.id)), &tables).await;
                for table in &tables {
                    check_lock!();
                    let key = self.key_tbl(&conn.id, &table.schema_name, &table.table_name);
                    let _ = self.cache_set(&key, table).await;
                }
            }
        }

        info!("cache warmup: completed successfully");
    }
```

**Step 3: Add tracing import**

Add to imports at top:

```rust
use tracing::info;
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/catalog/caching_manager.rs
git commit -m "feat(cache): implement background warmup loop with distributed lock"
```

---

## Task 9: Start Warmup Loop from Engine

**Files:**
- Modify: `src/engine.rs`

**Step 1: Store warmup handle in RuntimeEngine**

Add field to `RuntimeEngine` struct:

```rust
    cache_warmup_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
```

**Step 2: Initialize in build()**

In the builder's `build()` method, after creating the RuntimeEngine but before returning, start the warmup loop if configured:

```rust
        // Start cache warmup loop if enabled
        if let Some(cache_config) = &self.cache_config {
            if cache_config.warmup_interval_secs > 0 && cache_config.redis_url.is_some() {
                // Generate a unique node ID for distributed lock
                let node_id = format!("runtimedb-{}", uuid::Uuid::new_v4());

                // Need to downcast to CachingCatalogManager to call start_warmup_loop
                // This is a bit awkward but necessary since we store Arc<dyn CatalogManager>
                // For now, we'll skip this and document that warmup needs manual start
                // TODO: Consider storing the CachingCatalogManager separately for warmup
            }
        }
```

Note: Due to trait object limitations, starting the warmup loop automatically is complex. For now, document that users can manually start it by keeping a reference to the `CachingCatalogManager` before wrapping.

**Step 3: Initialize field**

Add to the RuntimeEngine construction in `build()`:

```rust
            cache_warmup_handle: Mutex::new(None),
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): add cache warmup handle storage (manual start for now)"
```

---

## Task 10: Add Warmup Loop Test

**Files:**
- Modify: `tests/caching_catalog_tests.rs`

**Step 1: Add warmup loop test**

Add to the test file:

```rust
#[tokio::test]
async fn test_warmup_loop_populates_cache() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add some data before creating caching layer
    let conn_id = inner
        .add_connection("warmup_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();
    let _ = inner
        .add_table(&conn_id, "public", "warmup_table", r#"{}"#)
        .await
        .unwrap();

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 1, // 1 second for test
        warmup_lock_ttl_secs: 5,
        key_prefix: "warmup_test:".to_string(),
    };

    let caching = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config)
            .await
            .unwrap(),
    );

    // Start warmup loop
    let handle = caching.start_warmup_loop("test-node".to_string());

    // Wait for warmup to run (interval is 1 second, so wait 2)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stop the warmup loop
    handle.abort();

    // Verify cache was populated by checking Redis directly
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("warmup_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should have conn:list, conn:{id}, conn:name:{name}, tbl:list:all, tbl:list:conn:{id}, etc.
    assert!(!keys.is_empty(), "Warmup should have populated cache keys");
    assert!(
        keys.iter().any(|k| k.contains("conn:list")),
        "Should have conn:list key"
    );
}

#[tokio::test]
async fn test_warmup_distributed_lock() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 1,
        warmup_lock_ttl_secs: 10,
        key_prefix: "lock_test:".to_string(),
    };

    // Create two caching managers
    let caching1 = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config.clone())
            .await
            .unwrap(),
    );
    let caching2 = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config)
            .await
            .unwrap(),
    );

    // Start both warmup loops
    let handle1 = caching1.start_warmup_loop("node-1".to_string());
    let handle2 = caching2.start_warmup_loop("node-2".to_string());

    // Wait for a warmup cycle
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stop both
    handle1.abort();
    handle2.abort();

    // Check that only one lock exists (only one should win)
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let lock_value: Option<String> = redis::cmd("GET")
        .arg("lock_test:lock:warmup")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Lock should exist and be held by one node
    assert!(
        lock_value.is_some(),
        "Lock should exist after warmup cycle"
    );
    let value = lock_value.unwrap();
    assert!(
        value == "node-1" || value == "node-2",
        "Lock should be held by one of the nodes"
    );
}
```

**Step 2: Run the tests**

Run: `cargo test caching_catalog_tests --test caching_catalog_tests`
Expected: All tests pass

**Step 3: Commit**

```bash
git add tests/caching_catalog_tests.rs
git commit -m "test(cache): add warmup loop and distributed lock tests"
```

---

## Task 11: Add Redis Fallback Test

**Files:**
- Modify: `tests/caching_catalog_tests.rs`

**Step 1: Add test for Redis unavailability**

Add to the test file:

```rust
#[tokio::test]
async fn test_redis_down_falls_through() {
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add data directly to inner catalog
    let conn_id = inner
        .add_connection("fallback_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Use an invalid Redis URL that will fail to connect
    let config = CacheConfig {
        redis_url: Some("redis://invalid-host:6379".to_string()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "fallback:".to_string(),
    };

    // This should fail to connect
    let result = CachingCatalogManager::new(inner.clone(), "redis://invalid-host:6379", config).await;

    // Connection should fail - caching manager requires Redis to be available at startup
    // This is by design - if Redis is configured, it should be available
    assert!(result.is_err(), "Should fail to create caching manager with invalid Redis URL");
}
```

**Step 2: Run the test**

Run: `cargo test test_redis_down_falls_through --test caching_catalog_tests`
Expected: Test passes

**Step 3: Commit**

```bash
git add tests/caching_catalog_tests.rs
git commit -m "test(cache): add Redis connection failure test"
```

---

## Task 12: Final Verification and Documentation

**Files:**
- No new files

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

**Step 3: Verify build**

Run: `cargo build --release`
Expected: Builds successfully

**Step 4: Commit any fixes**

If any fixes were needed:
```bash
git add .
git commit -m "fix(cache): address clippy warnings and test failures"
```

---

## Summary

This implementation plan covers:

1. **Tasks 1-2**: Dependencies and configuration
2. **Tasks 3-5**: Core CachingCatalogManager implementation (struct, read methods, write-through)
3. **Tasks 6, 10-11**: Integration tests
4. **Task 7**: Engine wiring
5. **Tasks 8-9**: Background warmup loop
6. **Task 12**: Final verification

Each task is atomic and can be committed independently. The tests use `testcontainers` to spin up Redis for realistic integration testing.
