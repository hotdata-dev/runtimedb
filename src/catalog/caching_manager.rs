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
pub struct CachingCatalogManager {
    inner: Arc<dyn CatalogManager>,
    redis: ConnectionManager,
    config: CacheConfig,
}

impl fmt::Debug for CachingCatalogManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingCatalogManager")
            .field("inner", &self.inner)
            .field("redis", &"<ConnectionManager>")
            .field("config", &self.config)
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
            let result: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
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

#[async_trait]
impl CatalogManager for CachingCatalogManager {
    // ─────────────────────────────────────────────────────────────────────────
    // Passthrough methods (no caching)
    // ─────────────────────────────────────────────────────────────────────────

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn run_migrations(&self) -> Result<()> {
        self.inner.run_migrations().await
    }

    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        self.inner
            .add_connection(name, source_type, config_json, secret_id)
            .await
    }

    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.inner
            .add_table(connection_id, schema_name, table_name, arrow_schema_json)
            .await
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
        self.inner
            .clear_table_cache_metadata(connection_id, schema_name, table_name)
            .await
    }

    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.inner
            .clear_connection_cache_metadata(connection_id)
            .await
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

    // Secret management methods - metadata

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

    // Secret management methods - encrypted storage

    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_encrypted_secret(secret_id).await
    }

    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()> {
        self.inner
            .put_encrypted_secret_value(secret_id, encrypted_value)
            .await
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

    // Query result persistence methods

    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        self.inner.store_result(result).await
    }

    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner.get_result(id).await
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        self.inner.list_results(limit, offset).await
    }

    // Upload management methods

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

    // Dataset management methods (mostly passthrough, except list_dataset_table_names)

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
        self.inner
            .get_dataset_by_table_name(schema_name, table_name)
            .await
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

    // ─────────────────────────────────────────────────────────────────────────
    // Cached read methods
    // ─────────────────────────────────────────────────────────────────────────

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let key = self.key_conn_list();
        self.cached_read(&key, || self.inner.list_connections())
            .await
    }

    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_id(id);
        let id_owned = id.to_string();
        self.cached_read(&key, || self.inner.get_connection(&id_owned))
            .await
    }

    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let key = self.key_conn_name(name);
        let name_owned = name.to_string();
        self.cached_read(&key, || self.inner.get_connection_by_name(&name_owned))
            .await
    }

    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        let key = self.key_tbl_list(connection_id);
        let conn_id = connection_id.map(|s| s.to_string());
        self.cached_read(&key, || {
            let conn_ref = conn_id.as_deref();
            self.inner.list_tables(conn_ref)
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
        self.cached_read(&key, || self.inner.get_table(&conn, &schema, &table))
            .await
    }

    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let key = self.key_tbl_names(schema_name);
        let schema = schema_name.to_string();
        self.cached_read(&key, || self.inner.list_dataset_table_names(&schema))
            .await
    }
}
