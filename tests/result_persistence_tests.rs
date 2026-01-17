use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_QUERY, PATH_RESULT};
use runtimedb::RuntimeEngine;
use serde_json::json;
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
    assert!(!json["result_id"].as_str().unwrap().is_empty());
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

#[tokio::test(flavor = "multi_thread")]
async fn test_persistence_failure_returns_null_result_id_with_warning() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Make the cache directory read-only to force persistence failure
    let cache_dir = temp_dir.path().join("cache");
    std::fs::create_dir_all(&cache_dir)?;
    let mut perms = std::fs::metadata(&cache_dir)?.permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&cache_dir, perms)?;

    // Query should still succeed, but with warning
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

    // Restore permissions for cleanup
    let mut perms = std::fs::metadata(&cache_dir)?.permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    std::fs::set_permissions(&cache_dir, perms)?;

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
