use anyhow::Result;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use datafusion::prelude::SessionContext;
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_QUERY, PATH_RESULT, PATH_RESULTS};
use runtimedb::storage::{CacheWriteHandle, FilesystemStorage, StorageManager};
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
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
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        self.inner.cache_url(connection_id, schema, table)
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
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

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        self.inner.register_with_datafusion(ctx)
    }

    fn prepare_cache_write(
        &self,
        connection_id: i32,
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

    // Both should be retrievable
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
    // Should have the column name from the query
    assert_eq!(get_json["columns"].as_array().unwrap().len(), 1);
    assert_eq!(get_json["columns"][0], "x");
    // Should have empty rows
    assert_eq!(get_json["row_count"], 0);

    Ok(())
}

/// Test that storage failures result in null result_id with warning using injected failing storage.
#[tokio::test(flavor = "multi_thread")]
async fn test_persistence_failure_returns_null_result_id_with_warning() -> Result<()> {
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

    // Query should still succeed, but with warning due to injected storage failure
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

    // result_id should be null (not missing)
    assert!(
        json.get("result_id").is_some(),
        "result_id field should be present"
    );
    assert!(
        json["result_id"].is_null(),
        "result_id should be null on persistence failure"
    );

    // Should have warning explaining the failure
    assert!(
        json.get("warning").is_some(),
        "warning field should be present"
    );
    assert!(
        json["warning"].as_str().unwrap().contains("not persisted"),
        "warning should explain persistence failure"
    );

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

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

    // First query - storage is failing
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

    assert!(
        json1["result_id"].is_null(),
        "First query should have null result_id"
    );
    assert!(
        json1.get("warning").is_some(),
        "First query should have warning"
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

    assert!(
        json2["result_id"].is_string(),
        "Second query should have valid result_id after storage recovery"
    );
    assert!(
        json2.get("warning").is_none(),
        "Second query should not have warning after storage recovery"
    );

    // Verify the result can be retrieved
    let result_id = json2["result_id"].as_str().unwrap();
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

    Ok(())
}

/// Test that multiple queries with storage failures all get proper warnings.
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

        assert!(
            json["result_id"].is_null(),
            "Query {} should have null result_id",
            i
        );
        assert!(
            json.get("warning").is_some(),
            "Query {} should have warning",
            i
        );
        assert_eq!(
            json["rows"][0][0], i,
            "Query {} should return correct data",
            i
        );
    }

    Ok(())
}

/// Test that parquet writer init failure (bad path) results in null result_id with warning.
/// This tests the failure path at writer.init() stage.
#[tokio::test(flavor = "multi_thread")]
async fn test_writer_init_failure_returns_null_result_id_with_warning() -> Result<()> {
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

    // Query should still succeed, but with warning due to writer init failure
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

    // result_id should be null due to writer init failure
    assert!(
        json.get("result_id").is_some(),
        "result_id field should be present"
    );
    assert!(
        json["result_id"].is_null(),
        "result_id should be null on writer init failure"
    );

    // Should have warning explaining the failure
    assert!(
        json.get("warning").is_some(),
        "warning field should be present"
    );
    let warning = json["warning"].as_str().unwrap();
    assert!(
        warning.contains("not persisted"),
        "warning should explain persistence failure: {}",
        warning
    );

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

/// Test that different failure stages all produce consistent error handling.
/// This tests both init failure and finalize failure in the same test.
#[tokio::test(flavor = "multi_thread")]
async fn test_different_failure_stages_produce_consistent_warnings() -> Result<()> {
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

    assert!(
        json1["result_id"].is_null(),
        "Init failure should have null result_id"
    );
    assert!(
        json1.get("warning").is_some(),
        "Init failure should have warning"
    );
    assert_eq!(
        json1["rows"][0][0], "init_fail",
        "Should return correct data despite init failure"
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

    assert!(
        json2["result_id"].is_null(),
        "Finalize failure should have null result_id"
    );
    assert!(
        json2.get("warning").is_some(),
        "Finalize failure should have warning"
    );
    assert_eq!(
        json2["rows"][0][0], "finalize_fail",
        "Should return correct data despite finalize failure"
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

    assert!(
        json3["result_id"].is_string(),
        "Success should have valid result_id"
    );
    assert!(
        json3.get("warning").is_none(),
        "Success should not have warning"
    );
    assert_eq!(
        json3["rows"][0][0], "success",
        "Should return correct data on success"
    );

    // Verify we can retrieve the successful result
    let result_id = json3["result_id"].as_str().unwrap();
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
    let result_id1 = json1["result_id"].as_str().unwrap();

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
    assert!(result_ids.contains(&result_id1));
    assert!(result_ids.contains(&result_id2));

    // Each result should have id and created_at
    for result in results {
        assert!(result["id"].is_string());
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
