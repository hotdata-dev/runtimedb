use anyhow::Result;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use rand::RngCore;
use runtimedb::catalog::{
    CatalogManager, ConnectionInfo, CreateQueryRun, DatasetInfo, OptimisticLock, PendingDeletion,
    QueryResult, QueryRun, QueryRunCursor, QueryRunUpdate, ResultStatus, ResultUpdate, SavedQuery,
    SavedQueryVersion, SqlSnapshot, SqliteCatalogManager, TableInfo, UploadInfo,
};
use runtimedb::http::app_server::{AppServer, PATH_QUERY, PATH_RESULT, PATH_RESULTS};
use runtimedb::secrets::{SecretMetadata, SecretStatus};
use runtimedb::storage::{CacheWriteHandle, DatasetWriteHandle, FilesystemStorage, StorageManager};
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::util::ServiceExt;

fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Catalog wrapper that delegates to a real catalog but can be configured to fail at specific points.
/// Used to test error handling in the persistence pipeline.
///
/// Note: A macro-based delegation approach was considered to reduce boilerplate, but
/// async_trait's lifetime requirements prevent macro-generated async methods from
/// matching the trait's lifetime bounds.
#[derive(Debug)]
struct FailingCatalog {
    inner: SqliteCatalogManager,
    fail_update_result_ready: AtomicBool,
    fail_create_result: AtomicBool,
}

impl FailingCatalog {
    async fn new(path: &str) -> Result<Self> {
        Ok(Self {
            inner: SqliteCatalogManager::new(path).await?,
            fail_update_result_ready: AtomicBool::new(false),
            fail_create_result: AtomicBool::new(false),
        })
    }

    fn set_fail_finalize_result(&self, should_fail: bool) {
        self.fail_update_result_ready
            .store(should_fail, Ordering::SeqCst);
    }

    fn set_fail_store_result_pending(&self, should_fail: bool) {
        self.fail_create_result.store(should_fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl CatalogManager for FailingCatalog {
    async fn init(&self) -> Result<()> {
        self.inner.init().await
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn run_migrations(&self) -> Result<()> {
        self.inner.run_migrations().await
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.inner.list_connections().await
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

    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        self.inner.get_connection(id).await
    }

    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.inner.get_connection_by_name(name).await
    }

    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        self.inner.list_tables(connection_id).await
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

    async fn get_table(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableInfo>> {
        self.inner.get_table(connection_id, schema, table).await
    }

    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableInfo> {
        self.inner
            .clear_table_cache_metadata(connection_id, schema, table)
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

    async fn create_result(&self, initial_status: ResultStatus) -> Result<String> {
        if self.fail_create_result.load(Ordering::SeqCst) {
            anyhow::bail!("Injected catalog failure at create_result")
        }
        self.inner.create_result(initial_status).await
    }

    async fn update_result(&self, id: &str, update: ResultUpdate<'_>) -> Result<bool> {
        // Only fail when transitioning to Ready (simulates finalize failure)
        if matches!(update, ResultUpdate::Ready { .. })
            && self.fail_update_result_ready.load(Ordering::SeqCst)
        {
            anyhow::bail!("Injected catalog failure at update_result (Ready)")
        }
        self.inner.update_result(id, update).await
    }

    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner.get_result(id).await
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        self.inner.list_results(limit, offset).await
    }

    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner.get_queryable_result(id).await
    }

    async fn cleanup_stale_results(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.inner.cleanup_stale_results(cutoff).await
    }

    async fn delete_expired_results(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<runtimedb::catalog::QueryResult>> {
        self.inner.delete_expired_results(cutoff).await
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

    // Dataset management methods

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

    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        self.inner.list_dataset_table_names(schema_name).await
    }

    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
        self.inner.update_dataset(id, label, table_name).await
    }

    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        self.inner.delete_dataset(id).await
    }

    // Query run methods

    async fn create_query_run(&self, params: CreateQueryRun<'_>) -> Result<String> {
        self.inner.create_query_run(params).await
    }

    async fn update_query_run(&self, id: &str, update: QueryRunUpdate<'_>) -> Result<bool> {
        self.inner.update_query_run(id, update).await
    }

    async fn list_query_runs(
        &self,
        limit: usize,
        cursor: Option<&QueryRunCursor>,
    ) -> Result<(Vec<QueryRun>, bool)> {
        self.inner.list_query_runs(limit, cursor).await
    }

    async fn get_query_run(&self, id: &str) -> Result<Option<QueryRun>> {
        self.inner.get_query_run(id).await
    }

    // SQL snapshot methods

    async fn get_or_create_snapshot(&self, sql_text: &str) -> Result<SqlSnapshot> {
        self.inner.get_or_create_snapshot(sql_text).await
    }

    // Saved query methods

    async fn create_saved_query(&self, name: &str, snapshot_id: &str) -> Result<SavedQuery> {
        self.inner.create_saved_query(name, snapshot_id).await
    }

    async fn get_saved_query(&self, id: &str) -> Result<Option<SavedQuery>> {
        self.inner.get_saved_query(id).await
    }

    async fn list_saved_queries(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQuery>, bool)> {
        self.inner.list_saved_queries(limit, offset).await
    }

    async fn update_saved_query(
        &self,
        id: &str,
        name: Option<&str>,
        snapshot_id: &str,
    ) -> Result<Option<SavedQuery>> {
        self.inner.update_saved_query(id, name, snapshot_id).await
    }

    async fn delete_saved_query(&self, id: &str) -> Result<bool> {
        self.inner.delete_saved_query(id).await
    }

    async fn get_saved_query_version(
        &self,
        saved_query_id: &str,
        version: i32,
    ) -> Result<Option<SavedQueryVersion>> {
        self.inner
            .get_saved_query_version(saved_query_id, version)
            .await
    }

    async fn list_saved_query_versions(
        &self,
        saved_query_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQueryVersion>, bool)> {
        self.inner
            .list_saved_query_versions(saved_query_id, limit, offset)
            .await
    }
}

async fn setup_test() -> Result<(AppServer, TempDir)> {
    let temp_dir = tempfile::tempdir()?;

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);
    Ok((app, temp_dir))
}

/// Wait for a result to transition from "processing" to "ready" or "failed".
async fn wait_for_result_ready(
    app: &AppServer,
    result_id: &str,
    timeout_ms: u64,
) -> Result<String> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    loop {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/results/{}", result_id))
                    .body(Body::empty())?,
            )
            .await?;

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        let status = json["status"].as_str().unwrap_or("unknown");
        if status != "processing" {
            return Ok(status.to_string());
        }

        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for result to be ready");
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Configurable failure points for storage operations.
#[derive(Debug, Default)]
struct FailureConfig {
    /// Fail at finalize_cache_write
    fail_finalize: AtomicBool,
    /// Return an invalid/unwritable path from prepare_cache_write to cause writer.init() to fail
    fail_prepare_path: AtomicBool,
}

/// A storage backend that delegates to FilesystemStorage but can be configured to fail
/// at different points in the persistence flow.
#[derive(Debug)]
struct FailingStorage {
    inner: FilesystemStorage,
    config: FailureConfig,
}

impl FailingStorage {
    fn new(base_dir: &std::path::Path) -> Self {
        Self {
            inner: FilesystemStorage::new(base_dir.to_str().expect("valid UTF-8 path")),
            config: FailureConfig::default(),
        }
    }

    /// Configure failure at finalize_cache_write stage
    fn set_fail_finalize(&self, should_fail: bool) {
        self.config
            .fail_finalize
            .store(should_fail, Ordering::SeqCst);
    }

    /// Configure failure at prepare_cache_write stage by returning an unwritable path.
    /// This causes the parquet writer's init() to fail when trying to create the directory.
    fn set_fail_prepare_path(&self, should_fail: bool) {
        self.config
            .fail_prepare_path
            .store(should_fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageManager for FailingStorage {
    fn cache_url(&self, connection_id: &str, schema: &str, table: &str) -> String {
        self.inner.cache_url(connection_id, schema, table)
    }

    fn cache_prefix(&self, connection_id: &str) -> String {
        self.inner.cache_prefix(connection_id)
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        self.inner.read(url).await
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        self.inner.write(url, data).await
    }

    async fn delete(&self, url: &str) -> Result<()> {
        self.inner.delete(url).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        self.inner.exists(url).await
    }

    async fn get_local_path(&self, url: &str) -> Result<(std::path::PathBuf, bool)> {
        self.inner.get_local_path(url).await
    }

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        self.inner.register_with_datafusion(ctx)
    }

    fn prepare_cache_write(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
    ) -> CacheWriteHandle {
        let mut handle = self.inner.prepare_cache_write(connection_id, schema, table);

        // If configured to fail, replace the local path with an unwritable one
        // Using /dev/null/invalid causes directory creation to fail on Unix
        if self.config.fail_prepare_path.load(Ordering::SeqCst) {
            handle.local_path = std::path::PathBuf::from("/dev/null/impossible/path/data.parquet");
        }

        handle
    }

    async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String> {
        if self.config.fail_finalize.load(Ordering::SeqCst) {
            anyhow::bail!("Injected storage failure at finalize")
        }
        self.inner.finalize_cache_write(handle).await
    }

    fn upload_url(&self, upload_id: &str) -> String {
        self.inner.upload_url(upload_id)
    }

    fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf {
        self.inner.prepare_upload_write(upload_id)
    }

    async fn finalize_upload_write(&self, upload_id: &str) -> Result<String> {
        self.inner.finalize_upload_write(upload_id).await
    }

    fn dataset_url(&self, dataset_id: &str, version: &str) -> String {
        self.inner.dataset_url(dataset_id, version)
    }

    fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle {
        self.inner.prepare_dataset_write(dataset_id)
    }

    async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String> {
        self.inner.finalize_dataset_write(handle).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_returns_result_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should have result_id (not null when persistence succeeds)
    assert!(json["result_id"].is_string());
    let result_id = json["result_id"].as_str().unwrap();
    assert!(!result_id.is_empty());
    // Should not have warning
    assert!(json.get("warning").is_none());

    // Should have expected data
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_result_by_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create a result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 'hello' as greeting"}).to_string(),
                ))?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Fetch by ID
    let get_uri = PATH_RESULT.replace("{id}", result_id);
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    assert_eq!(get_json["result_id"], result_id);
    assert_eq!(get_json["status"], "ready");
    assert_eq!(get_json["rows"][0][0], "hello");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_nonexistent_result_returns_404() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let get_uri = PATH_RESULT.replace("{id}", "nonexistent-id");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_queries_get_unique_result_ids() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Execute first query
    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;
    let result_id1 = json1["result_id"].as_str().unwrap();

    // Execute second query
    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 2 as y"}).to_string()))?,
        )
        .await?;

    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;
    let result_id2 = json2["result_id"].as_str().unwrap();

    // Result IDs should be different
    assert_ne!(result_id1, result_id2);

    // Wait for both results to be ready
    let status1 = wait_for_result_ready(&app, result_id1, 5000).await?;
    assert_eq!(status1, "ready");
    let status2 = wait_for_result_ready(&app, result_id2, 5000).await?;
    assert_eq!(status2, "ready");

    // Both should be retrievable with data
    let get_uri1 = PATH_RESULT.replace("{id}", result_id1);
    let get_response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri1)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_response1.status(), StatusCode::OK);

    let get_uri2 = PATH_RESULT.replace("{id}", result_id2);
    let get_response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri2)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_response2.status(), StatusCode::OK);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_result_returns_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Query that returns empty result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 1 as x WHERE false"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should still have result_id
    assert!(json["result_id"].is_string());
    let result_id = json["result_id"].as_str().unwrap();
    assert!(!result_id.is_empty());

    // But empty rows
    assert_eq!(json["row_count"], 0);

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Verify we can retrieve the empty result by ID
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    // Should have matching result_id
    assert_eq!(get_json["result_id"].as_str().unwrap(), result_id);
    assert_eq!(get_json["status"], "ready");
    // Should have the column name from the query
    assert_eq!(get_json["columns"].as_array().unwrap().len(), 1);
    assert_eq!(get_json["columns"][0], "x");
    // Should have empty rows
    assert_eq!(get_json["row_count"], 0);

    Ok(())
}

/// Test that storage failures result in status="failed" via GET /results/{id}.
/// With async persistence, query always returns result_id immediately.
#[tokio::test(flavor = "multi_thread")]
async fn test_persistence_failure_returns_failed_status() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create a storage backend that we can make fail on demand
    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query returns immediately with result_id (async persistence)
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Query always returns result_id with async persistence
    assert!(
        json["result_id"].is_string(),
        "result_id should always be present with async persistence"
    );
    let result_id = json["result_id"].as_str().unwrap();

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    // Wait for async persistence to complete (will fail)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(
        status, "failed",
        "Status should be 'failed' after storage failure"
    );

    Ok(())
}

/// Test that storage can recover after transient failures.
#[tokio::test(flavor = "multi_thread")]
async fn test_storage_recovery_after_failure() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create a storage backend that starts in failing mode
    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage.clone())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // First query - storage is failing (but query returns result_id with async persistence)
    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    assert_eq!(response1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;

    let result_id1 = json1["result_id"].as_str().unwrap();
    let status1 = wait_for_result_ready(&app, result_id1, 5000).await?;
    assert_eq!(
        status1, "failed",
        "First query should have 'failed' status due to storage failure"
    );

    // Now fix the storage
    failing_storage.set_fail_finalize(false);

    // Second query - storage should work now
    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 2 as y"}).to_string()))?,
        )
        .await?;

    assert_eq!(response2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;

    let result_id2 = json2["result_id"].as_str().unwrap();
    let status2 = wait_for_result_ready(&app, result_id2, 5000).await?;
    assert_eq!(
        status2, "ready",
        "Second query should have 'ready' status after storage recovery"
    );

    // Verify the result can be retrieved with data
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id2))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;
    assert_eq!(get_json["status"], "ready");

    Ok(())
}

/// Test that multiple queries with storage failures all get status="failed".
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_queries_with_storage_failure() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Execute multiple queries, all should fail persistence but return results
    for i in 1..=3 {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_QUERY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"sql": format!("SELECT {} as num", i)}).to_string(),
                    ))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        // Query returns result_id immediately with async persistence
        let result_id = json["result_id"]
            .as_str()
            .unwrap_or_else(|| panic!("Query {} should have result_id", i));

        assert_eq!(
            json["rows"][0][0], i,
            "Query {} should return correct data",
            i
        );

        // Wait for async persistence to complete (will fail)
        let status = wait_for_result_ready(&app, result_id, 5000).await?;
        assert_eq!(
            status, "failed",
            "Query {} should have 'failed' status due to storage failure",
            i
        );
    }

    Ok(())
}

/// Test that parquet writer init failure (bad path) results in status="failed".
/// This tests the failure path at writer.init() stage.
#[tokio::test(flavor = "multi_thread")]
async fn test_writer_init_failure_returns_failed_status() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create a storage backend that returns an unwritable path
    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_prepare_path(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query returns immediately with result_id (async persistence)
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Query always returns result_id with async persistence
    assert!(
        json["result_id"].is_string(),
        "result_id should always be present with async persistence"
    );
    let result_id = json["result_id"].as_str().unwrap();

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    // Wait for async persistence to complete (will fail due to writer init)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(
        status, "failed",
        "Status should be 'failed' after writer init failure"
    );

    Ok(())
}

/// Test that different failure stages all produce consistent status="failed".
/// This tests both init failure and finalize failure in the same test.
#[tokio::test(flavor = "multi_thread")]
async fn test_different_failure_stages_produce_consistent_status() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage.clone())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Test 1: Init failure (bad path)
    failing_storage.set_fail_prepare_path(true);
    failing_storage.set_fail_finalize(false);

    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 'init_fail' as stage"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(response1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;

    let result_id1 = json1["result_id"].as_str().unwrap();
    assert_eq!(
        json1["rows"][0][0], "init_fail",
        "Should return correct data despite init failure"
    );

    let status1 = wait_for_result_ready(&app, result_id1, 5000).await?;
    assert_eq!(
        status1, "failed",
        "Init failure should result in 'failed' status"
    );

    // Test 2: Finalize failure
    failing_storage.set_fail_prepare_path(false);
    failing_storage.set_fail_finalize(true);

    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 'finalize_fail' as stage"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(response2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;

    let result_id2 = json2["result_id"].as_str().unwrap();
    assert_eq!(
        json2["rows"][0][0], "finalize_fail",
        "Should return correct data despite finalize failure"
    );

    let status2 = wait_for_result_ready(&app, result_id2, 5000).await?;
    assert_eq!(
        status2, "failed",
        "Finalize failure should result in 'failed' status"
    );

    // Test 3: No failure - should succeed
    failing_storage.set_fail_prepare_path(false);
    failing_storage.set_fail_finalize(false);

    let response3 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 'success' as stage"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(response3.status(), StatusCode::OK);
    let body3 = axum::body::to_bytes(response3.into_body(), usize::MAX).await?;
    let json3: serde_json::Value = serde_json::from_slice(&body3)?;

    let result_id3 = json3["result_id"].as_str().unwrap();
    assert_eq!(
        json3["rows"][0][0], "success",
        "Should return correct data on success"
    );

    let status3 = wait_for_result_ready(&app, result_id3, 5000).await?;
    assert_eq!(status3, "ready", "Success should result in 'ready' status");

    // Verify we can retrieve the successful result with data
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id3))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;
    assert_eq!(get_json["status"], "ready");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_results_empty() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // List results when there are none
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_RESULTS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["count"], 0);
    assert_eq!(json["offset"], 0);
    assert_eq!(json["limit"], 100); // Default limit
    assert_eq!(json["has_more"], false);
    assert!(json["results"].as_array().unwrap().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_results_returns_created_results() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create two results
    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;
    assert_eq!(response1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;
    let result_id1 = json1["result_id"].as_str().unwrap().to_string();

    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 2 as y"}).to_string()))?,
        )
        .await?;
    assert_eq!(response2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;
    let result_id2 = json2["result_id"].as_str().unwrap().to_string();

    // Wait for both results to be ready
    wait_for_result_ready(&app, &result_id1, 5000).await?;
    wait_for_result_ready(&app, &result_id2, 5000).await?;

    // List results
    let list_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_RESULTS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX).await?;
    let list_json: serde_json::Value = serde_json::from_slice(&list_body)?;

    assert_eq!(list_json["count"], 2);
    assert_eq!(list_json["has_more"], false);

    let results = list_json["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // Results should be ordered by created_at descending (newest first)
    // so result_id2 should be first
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(result_ids.contains(&result_id1.as_str()));
    assert!(result_ids.contains(&result_id2.as_str()));

    // Each result should have id, status, and created_at
    for result in results {
        assert!(result["id"].is_string());
        assert!(result["status"].is_string());
        assert!(result["created_at"].is_string());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_results_pagination() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create 5 results
    let mut result_ids = Vec::new();
    for i in 1..=5 {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_QUERY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"sql": format!("SELECT {} as num", i)}).to_string(),
                    ))?,
            )
            .await?;
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        result_ids.push(json["result_id"].as_str().unwrap().to_string());
    }

    // Get first page with limit=2
    let page1_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=2", PATH_RESULTS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(page1_response.status(), StatusCode::OK);
    let page1_body = axum::body::to_bytes(page1_response.into_body(), usize::MAX).await?;
    let page1_json: serde_json::Value = serde_json::from_slice(&page1_body)?;

    assert_eq!(page1_json["count"], 2);
    assert_eq!(page1_json["limit"], 2);
    assert_eq!(page1_json["offset"], 0);
    assert_eq!(page1_json["has_more"], true);

    // Get second page with limit=2, offset=2
    let page2_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=2&offset=2", PATH_RESULTS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(page2_response.status(), StatusCode::OK);
    let page2_body = axum::body::to_bytes(page2_response.into_body(), usize::MAX).await?;
    let page2_json: serde_json::Value = serde_json::from_slice(&page2_body)?;

    assert_eq!(page2_json["count"], 2);
    assert_eq!(page2_json["limit"], 2);
    assert_eq!(page2_json["offset"], 2);
    assert_eq!(page2_json["has_more"], true);

    // Get third page - should have only 1 result
    let page3_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=2&offset=4", PATH_RESULTS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(page3_response.status(), StatusCode::OK);
    let page3_body = axum::body::to_bytes(page3_response.into_body(), usize::MAX).await?;
    let page3_json: serde_json::Value = serde_json::from_slice(&page3_body)?;

    assert_eq!(page3_json["count"], 1);
    assert_eq!(page3_json["limit"], 2);
    assert_eq!(page3_json["offset"], 4);
    assert_eq!(page3_json["has_more"], false);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_results_limit_capped_at_max() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Request with limit > max (1000) should be capped
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=5000", PATH_RESULTS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Limit should be capped at 1000
    assert_eq!(json["limit"], 1000);

    Ok(())
}

// =============================================================================
// SQL Query Tests - Query results via runtimedb.results schema
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_query_result_via_sql() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // First, create a result by running a query
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 42 as answer, 'hello' as greeting"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Now query the result via SQL using runtimedb.results schema
    let sql_query = format!("SELECT * FROM runtimedb.results.\"{}\"", result_id);
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    assert_eq!(query_response.status(), StatusCode::OK);
    let query_body = axum::body::to_bytes(query_response.into_body(), usize::MAX).await?;
    let query_json: serde_json::Value = serde_json::from_slice(&query_body)?;

    // Verify we got the original data back
    assert_eq!(query_json["row_count"], 1);
    assert_eq!(query_json["rows"][0][0], 42);
    assert_eq!(query_json["rows"][0][1], "hello");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_result_via_sql_multiple_rows() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create a result with multiple rows
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(num, letter)"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Query via SQL
    let sql_query = format!("SELECT * FROM runtimedb.results.\"{}\"", result_id);
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    assert_eq!(query_response.status(), StatusCode::OK);
    let query_body = axum::body::to_bytes(query_response.into_body(), usize::MAX).await?;
    let query_json: serde_json::Value = serde_json::from_slice(&query_body)?;

    // Verify all 3 rows are returned
    assert_eq!(query_json["row_count"], 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_result_via_sql_with_filter() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create a result with multiple rows
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(num, letter)"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Query with a filter
    let sql_query = format!(
        "SELECT * FROM runtimedb.results.\"{}\" WHERE num > 1",
        result_id
    );
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    assert_eq!(query_response.status(), StatusCode::OK);
    let query_body = axum::body::to_bytes(query_response.into_body(), usize::MAX).await?;
    let query_json: serde_json::Value = serde_json::from_slice(&query_body)?;

    // Should only return 2 rows (num=2 and num=3)
    assert_eq!(query_json["row_count"], 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_nonexistent_result_via_sql() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Try to query a result that doesn't exist
    let sql_query = "SELECT * FROM runtimedb.results.\"nonexistent_id\"";
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    // Should return an error (table not found) - DataFusion returns 500 for missing tables
    assert!(query_response.status().is_client_error() || query_response.status().is_server_error());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_result_with_aggregation() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create a result with numeric data
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM (VALUES (10), (20), (30)) AS t(value)"})
                        .to_string(),
                ))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for result to be ready (async persistence)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    // Query with aggregation
    let sql_query = format!(
        "SELECT SUM(value) as total, AVG(value) as avg FROM runtimedb.results.\"{}\"",
        result_id
    );
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    assert_eq!(query_response.status(), StatusCode::OK);
    let query_body = axum::body::to_bytes(query_response.into_body(), usize::MAX).await?;
    let query_json: serde_json::Value = serde_json::from_slice(&query_body)?;

    // Verify aggregation results
    assert_eq!(query_json["row_count"], 1);
    assert_eq!(query_json["rows"][0][0], 60); // SUM = 10+20+30
                                              // AVG returns as float, compare loosely
    let avg_value = query_json["rows"][0][1].as_f64().unwrap();
    assert!((avg_value - 20.0).abs() < 0.001); // AVG = 60/3

    Ok(())
}

/// Test that catalog finalize_result failures mark the result as failed.
/// This tests the scenario where parquet is successfully written but catalog update fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_finalize_result_failure_marks_result_failed() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let catalog_path = temp_dir.path().join("catalog.db");

    // Create a catalog that we can make fail on finalize_result
    let failing_catalog = Arc::new(FailingCatalog::new(catalog_path.to_str().unwrap()).await?);
    failing_catalog.set_fail_finalize_result(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .catalog(failing_catalog.clone())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query returns immediately with result_id (async persistence)
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Query always returns result_id with async persistence
    assert!(
        json["result_id"].is_string(),
        "result_id should always be present with async persistence"
    );
    let result_id = json["result_id"].as_str().unwrap();

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    // Wait for async persistence to complete (will fail at catalog.finalize_result)
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(
        status, "failed",
        "Status should be 'failed' after catalog finalize_result failure"
    );

    // Verify the error message contains useful info
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id))
                .body(Body::empty())?,
        )
        .await?;

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;
    assert_eq!(get_json["status"], "failed");
    assert!(
        get_json["error_message"].is_string(),
        "Failed result should have error_message"
    );
    let error_msg = get_json["error_message"].as_str().unwrap();
    assert!(
        error_msg.contains("catalog update failed"),
        "Error message should mention catalog update: {}",
        error_msg
    );

    Ok(())
}

/// Test that catalog store_result_pending failure returns results with warning (not hard failure).
/// This ensures query results are still returned even when persistence is unavailable.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_store_pending_failure_returns_results_with_warning() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let catalog_path = temp_dir.path().join("catalog.db");

    // Create a catalog that we can make fail on store_result_pending
    let failing_catalog = Arc::new(FailingCatalog::new(catalog_path.to_str().unwrap()).await?);
    failing_catalog.set_fail_store_result_pending(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .catalog(failing_catalog.clone())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query should return results even when catalog registration fails
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 42 as answer"}).to_string(),
                ))?,
        )
        .await?;

    // Should still succeed (not 500 error)
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should have query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 42);

    // result_id should be null (not present or null) when persistence failed
    assert!(
        json.get("result_id").is_none() || json["result_id"].is_null(),
        "result_id should be null when persistence failed, got: {:?}",
        json["result_id"]
    );

    // Should have a warning message
    assert!(
        json["warning"].is_string(),
        "Should have warning when persistence failed"
    );
    let warning = json["warning"].as_str().unwrap();
    assert!(
        warning.contains("persistence unavailable"),
        "Warning should mention persistence unavailable: {}",
        warning
    );

    Ok(())
}

/// Test that GET /results/{id} returns "failed" status (not 500) when parquet file is missing.
/// This can happen after concurrent cleanup or partial failure.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_result_missing_parquet_returns_failed_status() -> Result<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let base_dir = temp_dir.path().to_path_buf();

    let engine = RuntimeEngine::builder()
        .base_dir(&base_dir)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Execute a query to create a result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Wait for persistence to complete
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready", "Result should be ready initially");

    // Now delete the parquet file to simulate cleanup/partial failure
    let cache_dir = base_dir.join("cache");
    // Find and delete directories containing parquet files (deletes entire result dir)
    fn delete_parquet_dirs(dir: &std::path::Path) -> std::io::Result<usize> {
        let mut deleted = 0;
        if dir.is_dir() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // Check if this dir contains a parquet file
                    let has_parquet = std::fs::read_dir(&path)?.filter_map(|e| e.ok()).any(|e| {
                        e.path()
                            .extension()
                            .map(|ext| ext == "parquet")
                            .unwrap_or(false)
                    });
                    if has_parquet {
                        std::fs::remove_dir_all(&path)?;
                        deleted += 1;
                    } else {
                        deleted += delete_parquet_dirs(&path)?;
                    }
                }
            }
        }
        Ok(deleted)
    }
    let deleted = delete_parquet_dirs(&cache_dir)?;
    assert!(
        deleted > 0,
        "Should have deleted at least one parquet directory"
    );

    // Now GET /results/{id} should return "failed" status, not 500
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}/{}", PATH_RESULTS, result_id))
                .body(Body::empty())?,
        )
        .await?;

    // Should NOT be a 500 error - should be 200 with failed status
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Should return 200 with failed status, not 500"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(
        json["status"], "failed",
        "Status should be 'failed' when parquet file is missing"
    );
    assert!(
        json["error_message"].is_string(),
        "Should have error_message explaining the failure"
    );

    Ok(())
}
