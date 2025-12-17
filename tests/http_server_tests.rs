use anyhow::Result;
use axum::response::Response;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use rivetdb::http::app_server::{
    AppServer, PATH_CONNECTIONS, PATH_QUERY, PATH_SECRET, PATH_SECRETS, PATH_TABLES,
};
use rivetdb::RivetEngine;
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

    let engine = RivetEngine::builder()
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
async fn test_tables_endpoint_empty() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(PATH_TABLES)
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
async fn test_tables_endpoint_with_connection_filter_not_found() -> Result<()> {
    let (app, _tempdir) = setup_test().await?;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{}?connection=nonexistent", PATH_TABLES))
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
                    "source_type": "mysql",
                    "config": {}
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("postgres"));

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
    let engine = RivetEngine::builder()
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
    let engine = RivetEngine::builder()
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
    let engine = RivetEngine::builder()
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

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

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
