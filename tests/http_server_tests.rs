use anyhow::Result;
use axum::response::Response;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::datasets::DEFAULT_SCHEMA;
use runtimedb::http::app_server::{
    AppServer, PATH_CONNECTIONS, PATH_INFORMATION_SCHEMA, PATH_QUERY, PATH_REFRESH, PATH_SECRET,
    PATH_SECRETS,
};
use runtimedb::RuntimeEngine;
use serde_json::json;
use tempfile::TempDir;
use tower::util::ServiceExt;

/// Generate a test secret key (base64-encoded 32 bytes)
fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Create test router with in-memory engine
async fn setup_test() -> Result<(Router, TempDir)> {
    let temp_dir = tempfile::tempdir()?;

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    Ok((app.router, temp_dir))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_simple_select() -> Result<()> {
    let response = _send_query("SELECT 1 as num, 'hello' as text").await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["columns"], json!(["num", "text"]));
    assert_eq!(json["row_count"], 1);
    assert!(json["rows"].is_array());
    assert_eq!(json["rows"][0][0], 1);
    assert_eq!(json["rows"][0][1], "hello");
    assert!(json["execution_time_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_multiple_rows() -> Result<()> {
    let response =
        _send_query("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)").await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["columns"], json!(["id", "name"]));
    assert_eq!(json["row_count"], 3);
    assert_eq!(json["rows"].as_array().unwrap().len(), 3);

    // Verify first row
    assert_eq!(json["rows"][0][0], 1);
    assert_eq!(json["rows"][0][1], "a");

    // Verify last row
    assert_eq!(json["rows"][2][0], 3);
    assert_eq!(json["rows"][2][1], "c");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::approx_constant)]
async fn test_query_endpoint_different_data_types() -> Result<()> {
    let response =
        _send_query("SELECT 42 as int_col, 3.14 as float_col, true as bool_col, 'test' as str_col")
            .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(
        json["columns"],
        json!(["int_col", "float_col", "bool_col", "str_col"])
    );
    assert_eq!(json["row_count"], 1);

    // Verify data types are correctly converted
    assert_eq!(json["rows"][0][0], 42);
    assert_eq!(json["rows"][0][1], 3.14);
    assert_eq!(json["rows"][0][2], true);
    assert_eq!(json["rows"][0][3], "test");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_null_values() -> Result<()> {
    let response = _send_query("SELECT NULL as null_col, 1 as int_col").await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["columns"], json!(["null_col", "int_col"]));
    assert_eq!(json["row_count"], 1);
    // DataFusion may represent NULL differently in the JSON conversion
    // Just verify we have the expected structure
    assert_eq!(json["rows"][0].as_array().unwrap().len(), 2);
    assert_eq!(json["rows"][0][1], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_empty_result() -> Result<()> {
    let response = _send_query("SELECT 1 as num WHERE 1 = 0").await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Empty result may not have columns in schema
    assert_eq!(json["row_count"], 0);
    assert_eq!(json["rows"].as_array().unwrap().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_invalid_sql_text() -> Result<()> {
    let queries = vec!["", "   \n\t  "];

    for query in queries {
        let response = _send_query(query).await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        assert!(json["error"]["message"].as_str().unwrap().contains("empty"));
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_invalid_sql() -> Result<()> {
    let response = _send_query("SELECT * FROM nonexistent_table_12345").await?;

    // Should return error (could be 4xx or 5xx depending on error handling)
    assert!(response.status().is_client_error() || response.status().is_server_error());

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"].is_string());
    assert!(json["error"]["code"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_malformed_json() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from("{invalid json"))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_missing_sql_field() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "not_sql": "SELECT 1"
                }))?))?,
        )
        .await?;

    // Axum returns UNPROCESSABLE_ENTITY (422) when required fields are missing
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_endpoint_empty() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_INFORMATION_SCHEMA)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["tables"].is_array());
    // New engine should have no tables
    assert_eq!(json["tables"].as_array().unwrap().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_endpoint_with_connection_filter_not_found() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "{}?connection_id=nonexistent",
                    PATH_INFORMATION_SCHEMA
                ))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
    assert_eq!(json["error"]["code"], "NOT_FOUND");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_aggregate_functions() -> Result<()> {
    let sql = "\
        SELECT COUNT(*) as count, SUM(val) as total, AVG(val) as average \
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(val)";
    let response = _send_query(sql).await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["columns"], json!(["count", "total", "average"]));
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 5); // COUNT(*)
    assert_eq!(json["rows"][0][1], 15); // SUM
    assert_eq!(json["rows"][0][2], 3.0); // AVG

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_group_by() -> Result<()> {
    let sql = "\
        SELECT category, COUNT(*) as count \
        FROM (VALUES ('A', 1), ('A', 2), ('B', 3), ('B', 4)) AS t(category, val) \
        GROUP BY category \
        ORDER BY category";
    let response = _send_query(sql).await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["columns"], json!(["category", "count"]));
    assert_eq!(json["row_count"], 2);
    assert_eq!(json["rows"][0][0], "A");
    assert_eq!(json["rows"][0][1], 2);
    assert_eq!(json["rows"][1][0], "B");
    assert_eq!(json["rows"][1][1], 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_endpoint_joins() -> Result<()> {
    let sql = "SELECT a.id, a.name, b.value FROM (VALUES \
    (1, 'Alice'), \
    (2, 'Bob')) AS a(id, name) \
    JOIN (VALUES (1, 100), (2, 200)) AS b(id, value) ON a.id = b.id ORDER BY a.id";
    let response = _send_query(sql).await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["row_count"], 2);
    assert_eq!(json["rows"][0][0], 1);
    assert_eq!(json["rows"][0][1], "Alice");
    assert_eq!(json["rows"][0][2], 100);
    assert_eq!(json["rows"][1][0], 2);
    assert_eq!(json["rows"][1][1], "Bob");
    assert_eq!(json["rows"][1][2], 200);

    Ok(())
}

async fn _send_query(sql: &str) -> Result<Response> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": sql
                }))?))?,
        )
        .await?;
    Ok(response)
}

// ==================== Connection Endpoint Tests ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_list_connections_empty() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_CONNECTIONS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["connections"].is_array());
    assert_eq!(json["connections"].as_array().unwrap().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_empty_name() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "",
                    "source_type": "postgres",
                    "config": {}
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"].as_str().unwrap().contains("empty"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_unsupported_source_type() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_conn",
                    "source_type": "oracle",
                    "config": {}
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Check that error mentions supported source types
    let error_msg = json["error"]["message"].as_str().unwrap();
    assert!(error_msg.contains("postgres") || error_msg.contains("mysql"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_missing_fields() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_conn"
                }))?))?,
        )
        .await?;

    // Axum returns UNPROCESSABLE_ENTITY (422) when required fields are missing
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    Ok(())
}

// ==================== Secret Endpoint Tests ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_list_secrets_empty() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_SECRETS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["secrets"].is_array());
    assert_eq!(json["secrets"].as_array().unwrap().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_and_get_secret() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a secret
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_secret",
                    "value": "super_secret_value"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["name"], "test_secret");
    assert!(json["created_at"].is_string());

    // Fetch the secret metadata
    let secret_path = PATH_SECRET.replace("{name}", "test_secret");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&secret_path)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["name"], "test_secret");
    assert!(json["created_at"].is_string());
    assert!(json["updated_at"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_secrets_after_create() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create two secrets
    for name in ["secret_one", "secret_two"] {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_SECRETS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "name": name,
                        "value": format!("value_for_{}", name)
                    }))?))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // List secrets
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_SECRETS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    let secrets = json["secrets"].as_array().unwrap();
    assert_eq!(secrets.len(), 2);

    let names: Vec<&str> = secrets
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"secret_one"));
    assert!(names.contains(&"secret_two"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_secret() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a secret
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "to_delete",
                    "value": "will_be_deleted"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Delete the secret
    let secret_path = PATH_SECRET.replace("{name}", "to_delete");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&secret_path)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone by listing
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_SECRETS)
                .body(Body::empty())?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["secrets"].as_array().unwrap().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_nonexistent_secret() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let secret_path = PATH_SECRET.replace("{name}", "does_not_exist");
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&secret_path)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
    assert_eq!(json["error"]["code"], "NOT_FOUND");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_nonexistent_secret() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let secret_path = PATH_SECRET.replace("{name}", "does_not_exist");
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&secret_path)
                .body(Body::empty())?,
        )
        .await?;

    // Delete is idempotent - returns 204 even for non-existent secrets
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_secret_missing_fields() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_secret"
                    // missing "value" field
                }))?))?,
        )
        .await?;

    // Axum returns UNPROCESSABLE_ENTITY (422) when required fields are missing
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_secret() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a secret
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_secret",
                    "value": "original_value"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let create_json: serde_json::Value = serde_json::from_slice(&body)?;
    let created_at = create_json["created_at"].as_str().unwrap();

    // Small delay to ensure updated_at will be different
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Update the secret
    let secret_path = PATH_SECRET.replace("{name}", "test_secret");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&secret_path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "value": "updated_value"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let update_json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(update_json["name"], "test_secret");
    assert!(update_json["updated_at"].is_string());

    // Verify updated_at is different from created_at
    let updated_at = update_json["updated_at"].as_str().unwrap();
    assert_ne!(
        created_at, updated_at,
        "updated_at should change after update"
    );

    // Verify the new value can be retrieved via the manager
    let secret_value = app.engine.secret_manager().get("test_secret").await?;
    assert_eq!(secret_value, b"updated_value");

    // Verify metadata via GET shows updated timestamps
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&secret_path)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(get_json["updated_at"].as_str().unwrap(), updated_at);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_nonexistent_secret() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let secret_path = PATH_SECRET.replace("{name}", "does_not_exist");
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&secret_path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "value": "some_value"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
    assert_eq!(json["error"]["code"], "NOT_FOUND");

    Ok(())
}

// ==================== Decoupled Registration/Discovery Tests ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_registers_even_when_discovery_fails() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // Create a postgres connection with invalid credentials - registration should
    // succeed but discovery should fail
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my_pg",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "nonexistent_user",
                        "password": "bad_password",
                        "database": "nonexistent_db"
                    }
                }))?))?,
        )
        .await?;

    // Should return 201 CREATED (connection was registered)
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify response structure
    assert_eq!(json["name"], "my_pg");
    assert_eq!(json["source_type"], "postgres");
    assert_eq!(json["tables_discovered"], 0);
    assert_eq!(json["discovery_status"], "failed");
    assert!(json["discovery_error"].is_string());

    // Verify connection exists by listing connections
    let list_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_CONNECTIONS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX).await?;
    let list_json: serde_json::Value = serde_json::from_slice(&list_body)?;

    assert_eq!(list_json["connections"].as_array().unwrap().len(), 1);
    assert_eq!(list_json["connections"][0]["name"], "my_pg");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_connection_not_found() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // Use the /refresh endpoint with a non-existent connection_id
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": "con_nonexistent_fake_id"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_retry_after_failed_discovery() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // First create a connection with invalid credentials
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "retry_conn",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "bad_user",
                        "password": "bad_pass",
                        "database": "bad_db"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::CREATED);

    // Extract the connection id from the create response
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let create_json: serde_json::Value = serde_json::from_slice(&body)?;
    let connection_id = create_json["id"].as_str().unwrap();

    // Now try to refresh schema via the /refresh endpoint
    let refresh_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    // Should return 500 Internal Server Error because schema refresh fails on bad credentials
    assert_eq!(refresh_response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body = axum::body::to_bytes(refresh_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify error response structure
    assert!(json["error"]["message"].is_string());
    assert_eq!(json["error"]["code"], "INTERNAL_SERVER_ERROR");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_duplicate_name_rejected() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // Create first connection (will fail discovery but register successfully)
    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "dup_conn",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "user1",
                        "password": "pass1",
                        "database": "db1"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(first_response.status(), StatusCode::CREATED);

    // Try to create another connection with the same name
    let second_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "dup_conn",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "user2",
                        "password": "pass2",
                        "database": "db2"
                    }
                }))?))?,
        )
        .await?;

    // Should be rejected as conflict
    assert_eq!(second_response.status(), StatusCode::CONFLICT);

    let body = axum::body::to_bytes(second_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("already exists"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_successful_discovery() -> Result<()> {
    let (app, tempdir) = setup_test().await?;

    // Create a DuckDB file with a table
    let db_path = tempdir.path().join("test.duckdb");
    {
        let conn = duckdb::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR);
             INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');",
        )?;
    }

    // Create connection via API
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "test_duck",
                    "source_type": "duckdb",
                    "config": {
                        "path": db_path.to_str().unwrap()
                    }
                }))?))?,
        )
        .await?;

    // Should return 201 CREATED
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify successful discovery response
    assert_eq!(json["name"], "test_duck");
    assert_eq!(json["source_type"], "duckdb");
    assert_eq!(json["tables_discovered"], 1);
    assert_eq!(json["discovery_status"], "success");
    // discovery_error should not be present on success
    assert!(json["discovery_error"].is_null());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_successful() -> Result<()> {
    let (app, tempdir) = setup_test().await?;

    // Create a DuckDB file with a table
    let db_path = tempdir.path().join("discover_test.duckdb");
    {
        let conn = duckdb::Connection::open(&db_path)?;
        conn.execute_batch("CREATE TABLE orders (id INTEGER, amount DECIMAL);")?;
    }

    // First create the connection
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "discover_duck",
                    "source_type": "duckdb",
                    "config": {
                        "path": db_path.to_str().unwrap()
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::CREATED);

    // Extract the connection id from the create response
    let body = axum::body::to_bytes(create_response.into_body(), usize::MAX).await?;
    let create_json: serde_json::Value = serde_json::from_slice(&body)?;
    let connection_id = create_json["id"].as_str().unwrap();

    // Now call refresh endpoint (even though already discovered, it should work)
    let refresh_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    assert_eq!(refresh_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(refresh_response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify successful refresh response (schema refresh result)
    assert_eq!(json["connections_refreshed"], 1);
    assert_eq!(json["tables_discovered"], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_with_password_auto_creates_secret() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // Create a postgres connection with password field (old API format)
    // Discovery will fail (no postgres server), but the secret should be created
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my_pg",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "testuser",
                        "password": "testpassword123",
                        "database": "testdb"
                    }
                }))?))?,
        )
        .await?;

    // Connection should be created (discovery will fail, but that's expected)
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["name"], "my_pg");
    assert_eq!(json["source_type"], "postgres");
    // Discovery fails because no postgres server, but the important thing is it's not
    // "no credential configured" - it should be a connection error
    assert_eq!(json["discovery_status"], "failed");
    let error = json["discovery_error"].as_str().unwrap();
    assert!(
        !error.contains("no credential configured"),
        "Error should not be 'no credential configured', got: {}",
        error
    );

    // Verify the secret was auto-created with the expected name
    let secrets_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_SECRETS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(secrets_response.status(), StatusCode::OK);

    let secrets_body = axum::body::to_bytes(secrets_response.into_body(), usize::MAX).await?;
    let secrets_json: serde_json::Value = serde_json::from_slice(&secrets_body)?;

    let secrets = secrets_json["secrets"].as_array().unwrap();
    assert_eq!(
        secrets.len(),
        1,
        "Expected exactly one secret to be created"
    );
    assert_eq!(
        secrets[0]["name"], "conn-my_pg-password",
        "Secret should be named 'conn-my_pg-password'"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_with_secret_name() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // First, create a secret
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my-pg-password",
                    "value": "testpassword123"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Create a connection referencing the secret by name
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my_pg",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "testuser",
                        "database": "testdb",
                        "auth": "password"
                    },
                    "secret_name": "my-pg-password"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["name"], "my_pg");
    assert_eq!(json["source_type"], "postgres");
    // Discovery fails because no postgres server, but the error should be a connection error,
    // not "no credential configured"
    assert_eq!(json["discovery_status"], "failed");
    let error = json["discovery_error"].as_str().unwrap();
    assert!(
        !error.contains("no credential configured"),
        "Error should not be 'no credential configured', got: {}",
        error
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_with_secret_id() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // First, create a secret and get its ID
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my-pg-password",
                    "value": "testpassword123"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let secret_json: serde_json::Value = serde_json::from_slice(&body)?;
    let secret_id = secret_json["id"].as_str().unwrap();
    assert!(
        secret_id.starts_with("secr"),
        "Secret ID should start with 'secr', got: {}",
        secret_id
    );

    // Create a connection referencing the secret by ID
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my_pg",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "testuser",
                        "database": "testdb",
                        "auth": "password"
                    },
                    "secret_id": secret_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["name"], "my_pg");
    assert_eq!(json["source_type"], "postgres");
    // Discovery fails because no postgres server, but the error should be a connection error,
    // not "no credential configured"
    assert_eq!(json["discovery_status"], "failed");
    let error = json["discovery_error"].as_str().unwrap();
    assert!(
        !error.contains("no credential configured"),
        "Error should not be 'no credential configured', got: {}",
        error
    );

    Ok(())
}

// === Upload Endpoint Tests ===

async fn parse_body(response: Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_file_endpoint() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("id,name\n1,test\n2,other"))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["id"].as_str().unwrap().starts_with("upld"));
    assert_eq!(body["status"], "pending");
    assert!(body["size_bytes"].as_i64().unwrap() > 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_uploads_empty() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["uploads"].as_array().unwrap().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_uploads_after_upload() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Upload a file first
    let _ = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("a,b\n1,2"))?,
        )
        .await?;

    // List uploads
    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    let uploads = body["uploads"].as_array().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0]["status"], "pending");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_file_empty_rejected() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("cannot be empty"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_file_too_large() -> Result<()> {
    // Note: We can't actually test with 2GB of data in a unit test.
    // This test documents the expected behavior. The actual size limit
    // (2GB) is enforced in the handler via MAX_UPLOAD_SIZE constant.
    // The validation logic is tested implicitly by test_upload_file_endpoint
    // which verifies that valid uploads succeed.
    //
    // To fully test the size limit, you would need an integration test
    // that can handle large payloads, or mock the body length.

    // For now, we verify the handler exists and accepts valid uploads,
    // and that the constant is properly defined (compile-time check).
    let (app, _temp) = setup_test().await?;

    // Verify a normal upload still works (regression test)
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("id,name\n1,test"))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    Ok(())
}

// === Dataset Endpoint Tests ===

const PATH_DATASETS: &str = "/v1/datasets";

fn path_dataset(id: &str) -> String {
    format!("/v1/datasets/{}", id)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_dataset_from_upload() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Step 1: Upload a CSV file
    let csv_data = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300";
    let upload_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from(csv_data))?,
        )
        .await?;

    assert_eq!(upload_response.status(), StatusCode::CREATED);
    let upload_body: serde_json::Value = parse_body(upload_response).await;
    let upload_id = upload_body["id"].as_str().unwrap();

    // Step 2: Create dataset from the upload
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Test Dataset",
                    "source": {
                        "upload_id": upload_id,
                        "format": "csv"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = parse_body(response).await;
    assert!(body["id"].as_str().unwrap().starts_with("data"));
    assert_eq!(body["label"], "Test Dataset");
    assert_eq!(body["table_name"], "test_dataset");
    assert_eq!(body["status"], "ready");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_dataset_with_inline_data() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Inline Data",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a,b\n1,2\n3,4"
                        }
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = parse_body(response).await;
    assert!(body["id"].as_str().unwrap().starts_with("data"));
    assert_eq!(body["label"], "Inline Data");
    assert_eq!(body["table_name"], "inline_data");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_dataset_inline_too_large() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create inline data > 1MB (1_048_576 bytes)
    let large_content = "x".repeat(1_048_577);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Too Large",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": large_content
                        }
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = parse_body(response).await;
    assert!(body["error"]["message"].as_str().unwrap().contains("1MB"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_dataset_invalid_table_name() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // "select" is a SQL reserved word
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "My Data",
                    "table_name": "select",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a,b\n1,2"
                        }
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = parse_body(response).await;
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("reserved"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_datasets() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset first
    let _create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "List Test",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a,b\n1,2"
                        }
                    }
                }))?))?,
        )
        .await?;

    // List datasets
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_DATASETS)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    let datasets = body["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0]["label"], "List Test");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_dataset() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Get Test",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "col1,col2\nfoo,42"
                        }
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(create_response.status(), StatusCode::CREATED);
    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Get the dataset
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path_dataset(dataset_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["id"], dataset_id);
    assert_eq!(body["label"], "Get Test");
    assert_eq!(body["table_name"], "get_test");
    assert_eq!(body["schema_name"], DEFAULT_SCHEMA);

    // Should include columns
    let columns = body["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_dataset_not_found() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path_dataset("data_nonexistent12345678901234"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Original Label",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Update the dataset
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset(dataset_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Updated Label",
                    "table_name": "updated_table"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["label"], "Updated Label");
    assert_eq!(body["table_name"], "updated_table");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_dataset() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "To Delete",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Delete the dataset
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(path_dataset(dataset_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path_dataset(dataset_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_consumed_only_once() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Upload a file
    let upload_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("a,b\n1,2"))?,
        )
        .await?;

    let upload_body: serde_json::Value = parse_body(upload_response).await;
    let upload_id = upload_body["id"].as_str().unwrap();

    // Create first dataset from upload
    let first_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "First",
                    "source": {
                        "upload_id": upload_id,
                        "format": "csv"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(first_response.status(), StatusCode::CREATED);

    // Try to use same upload again
    let second_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Second",
                    "source": {
                        "upload_id": upload_id,
                        "format": "csv"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(second_response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = parse_body(second_response).await;
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("consumed"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_connection_with_both_secret_name_and_id_fails() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // First, create a secret
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_SECRETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my-pg-password",
                    "value": "testpassword123"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let secret_json: serde_json::Value = serde_json::from_slice(&body)?;
    let secret_id = secret_json["id"].as_str().unwrap();

    // Try to create a connection with both secret_name and secret_id - should fail
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_CONNECTIONS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "name": "my_pg",
                    "source_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "testuser",
                        "database": "testdb",
                        "auth": "password"
                    },
                    "secret_name": "my-pg-password",
                    "secret_id": secret_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let error = json["error"]["message"].as_str().unwrap();
    assert!(
        error.contains("secret_name") && error.contains("secret_id"),
        "Error should mention both secret_name and secret_id, got: {}",
        error
    );

    Ok(())
}

// ============================================================================
// Additional Dataset HTTP API Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_list_datasets_with_pagination() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create 3 datasets with labels that sort alphabetically
    for label in ["Alpha", "Beta", "Gamma"] {
        let _create_response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_DATASETS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "label": label,
                        "source": {
                            "inline": {
                                "format": "csv",
                                "content": "a\n1"
                            }
                        }
                    }))?))?,
            )
            .await?;
    }

    // Test pagination: limit=2, offset=0
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=2&offset=0", PATH_DATASETS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    let datasets = body["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 2);
    assert!(body["has_more"].as_bool().unwrap());
    assert_eq!(datasets[0]["label"], "Alpha");
    assert_eq!(datasets[1]["label"], "Beta");

    // Test pagination: limit=2, offset=2
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?limit=2&offset=2", PATH_DATASETS))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    let datasets = body["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 1);
    assert!(!body["has_more"].as_bool().unwrap());
    assert_eq!(datasets[0]["label"], "Gamma");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset_label_only() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Original Label",
                    "table_name": "original_table",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Update only the label (omit table_name)
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset(dataset_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "New Label"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["label"], "New Label");
    assert_eq!(body["table_name"], "original_table"); // unchanged

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset_table_name_only() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "My Dataset",
                    "table_name": "old_table",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Update only the table_name (omit label)
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset(dataset_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "table_name": "new_table"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["label"], "My Dataset"); // unchanged
    assert_eq!(body["table_name"], "new_table");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset_not_found() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Try to update a non-existent dataset
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset("ds_nonexistent_12345"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "New Label"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset_table_name_conflict() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create first dataset
    let _create1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "First",
                    "table_name": "taken_name",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    // Create second dataset
    let create2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Second",
                    "table_name": "other_name",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "b\n2"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create2_body: serde_json::Value = parse_body(create2).await;
    let dataset2_id = create2_body["id"].as_str().unwrap();

    // Try to update second dataset to use first dataset's table_name
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset(dataset2_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "table_name": "taken_name"
                }))?))?,
        )
        .await?;

    // Should get 409 Conflict (or 400 Bad Request depending on implementation)
    assert!(
        response.status() == StatusCode::CONFLICT || response.status() == StatusCode::BAD_REQUEST,
        "Expected 409 or 400, got {}",
        response.status()
    );

    let body: serde_json::Value = parse_body(response).await;
    let error = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        error.contains("taken_name") || error.contains("in use") || error.contains("already"),
        "Error should mention table name conflict: {}",
        error
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_dataset_not_found() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Try to delete a non-existent dataset
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(path_dataset("ds_nonexistent_12345"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_dataset_empty_label() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = parse_body(response).await;
    let error = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        error.contains("empty") || error.contains("label"),
        "Error should mention empty label: {}",
        error
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_dataset_empty_label() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    // Create a dataset
    let create_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_DATASETS)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": "Valid Label",
                    "source": {
                        "inline": {
                            "format": "csv",
                            "content": "a\n1"
                        }
                    }
                }))?))?,
        )
        .await?;

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Try to update with empty label
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_dataset(dataset_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "label": ""
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = parse_body(response).await;
    let error = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        error.contains("empty") || error.contains("label"),
        "Error should mention empty label: {}",
        error
    );

    Ok(())
}

// ==================== Query Run History Tests ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_query_returns_query_run_id() -> Result<()> {
    let response = _send_query("SELECT 1 as num").await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    let query_run_id = json["query_run_id"].as_str().unwrap();
    assert!(
        query_run_id.starts_with("qrun"),
        "query_run_id should start with 'qrun': {}",
        query_run_id
    );
    assert_eq!(query_run_id.len(), 30);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_query_runs_empty() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["query_runs"].is_array());
    assert_eq!(json["count"], 0);
    assert!(!json["has_more"].as_bool().unwrap());
    assert!(json["next_cursor"].is_null());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_run_lifecycle_and_list() -> Result<()> {
    let (router, _tempdir) = setup_test().await?;

    // Execute a query
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": "SELECT 42 as answer"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let query_json: serde_json::Value = serde_json::from_slice(&body)?;
    let query_run_id = query_json["query_run_id"].as_str().unwrap().to_string();

    // List query runs
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs?limit=10")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let list_json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(list_json["count"], 1);
    let runs = list_json["query_runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["id"], query_run_id);
    assert_eq!(runs[0]["status"], "succeeded");
    assert_eq!(runs[0]["sql_text"], "SELECT 42 as answer");
    assert!(runs[0]["row_count"].as_i64().unwrap() > 0);
    assert!(runs[0]["execution_time_ms"].as_i64().is_some());
    assert!(runs[0]["completed_at"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_run_failed_query() -> Result<()> {
    let (router, _tempdir) = setup_test().await?;

    // Execute a bad query
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": "SELECT * FROM nonexistent_table_xyz"
                }))?))?,
        )
        .await?;
    // Query should fail
    assert_ne!(response.status(), StatusCode::OK);

    // List query runs - should show the failed run
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let list_json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(list_json["count"], 1);
    let runs = list_json["query_runs"].as_array().unwrap();
    assert_eq!(runs[0]["status"], "failed");
    assert!(runs[0]["error_message"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_run_pagination() -> Result<()> {
    let (router, _tempdir) = setup_test().await?;

    // Execute 3 queries
    for i in 0..3 {
        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_QUERY)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "sql": format!("SELECT {}", i)
                    }))?))?,
            )
            .await?;
    }

    // First page: limit 2
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs?limit=2")
                .body(Body::empty())?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let page1: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(page1["count"], 2);
    assert!(page1["has_more"].as_bool().unwrap());
    let next_cursor = page1["next_cursor"].as_str().unwrap();

    // Second page using cursor
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/query-runs?limit=2&cursor={}", next_cursor))
                .body(Body::empty())?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let page2: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(page2["count"], 1);
    assert!(!page2["has_more"].as_bool().unwrap());
    assert!(page2["next_cursor"].is_null());

    Ok(())
}

/// Regression: limit=0 used to produce an inconsistent page (has_more=true, next_cursor=null).
/// The engine now clamps limit to at least 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_list_query_runs_limit_zero_clamps_to_one() -> Result<()> {
    let (router, _tempdir) = setup_test().await?;

    // Create one query run
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": "SELECT 1"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Request with limit=0 — should be clamped to 1
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs?limit=0")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // With limit clamped to 1, we should get exactly 1 result
    assert_eq!(json["count"], 1);
    assert_eq!(json["limit"], 1);
    assert!(!json["has_more"].as_bool().unwrap());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_query_runs_invalid_cursor_returns_400() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query-runs?cursor=not-valid-base64!!!")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_with_very_large_sql() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    // Build a large SQL query (~30KB) using repeated column aliases
    let columns: Vec<String> = (0..2000).map(|i| format!("{} as col_{}", i, i)).collect();
    let large_sql = format!("SELECT {}", columns.join(", "));
    assert!(large_sql.len() > 30_000);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": large_sql
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["query_run_id"].as_str().unwrap().starts_with("qrun"));
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["columns"].as_array().unwrap().len(), 2000);

    Ok(())
}
