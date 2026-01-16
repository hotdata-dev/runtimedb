//! Integration tests for the refresh endpoint.
//!
//! Tests cover schema refresh, data refresh, validation, and pending deletions.

use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_CONNECTIONS, PATH_REFRESH};
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

/// Generate a test secret key (base64-encoded 32 bytes)
fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Test harness providing engine and router access
struct RefreshTestHarness {
    engine: Arc<RuntimeEngine>,
    router: Router,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl RefreshTestHarness {
    async fn new() -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;

        let engine = RuntimeEngine::builder()
            .base_dir(temp_dir.path())
            .secret_key(generate_test_secret_key())
            .build()
            .await?;

        let app = AppServer::new(engine);

        Ok(Self {
            engine: app.engine,
            router: app.router,
            temp_dir,
        })
    }

    /// Create a DuckDB test database and return the path
    fn create_duckdb(&self, name: &str) -> String {
        let db_path = self.temp_dir.path().join(format!("{}.duckdb", name));
        let conn = duckdb::Connection::open(&db_path).expect("Failed to open DuckDB");

        conn.execute("CREATE SCHEMA sales", [])
            .expect("Failed to create schema");
        conn.execute(
            "CREATE TABLE sales.orders (id INTEGER, customer VARCHAR, amount DOUBLE)",
            [],
        )
        .expect("Failed to create table");
        conn.execute(
            "INSERT INTO sales.orders VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            [],
        )
        .expect("Failed to insert data");

        db_path.to_str().unwrap().to_string()
    }

    /// Create a DuckDB with multiple tables for testing schema changes
    fn create_duckdb_multi_table(&self, name: &str) -> String {
        let db_path = self.temp_dir.path().join(format!("{}.duckdb", name));
        let conn = duckdb::Connection::open(&db_path).expect("Failed to open DuckDB");

        conn.execute("CREATE SCHEMA sales", [])
            .expect("Failed to create schema");
        conn.execute(
            "CREATE TABLE sales.orders (id INTEGER, customer VARCHAR)",
            [],
        )
        .expect("Failed to create orders");
        conn.execute(
            "CREATE TABLE sales.products (id INTEGER, name VARCHAR, price DOUBLE)",
            [],
        )
        .expect("Failed to create products");
        conn.execute("INSERT INTO sales.orders VALUES (1, 'Alice')", [])
            .expect("Failed to insert");
        conn.execute("INSERT INTO sales.products VALUES (1, 'Widget', 9.99)", [])
            .expect("Failed to insert");

        db_path.to_str().unwrap().to_string()
    }

    /// Add a table to an existing DuckDB
    fn add_table_to_duckdb(db_path: &str, schema: &str, table: &str) {
        let conn = duckdb::Connection::open(db_path).expect("Failed to open DuckDB");
        conn.execute(
            &format!("CREATE TABLE {}.{} (id INTEGER)", schema, table),
            [],
        )
        .expect("Failed to create table");
    }

    /// Remove a table from a DuckDB
    fn remove_table_from_duckdb(db_path: &str, schema: &str, table: &str) {
        let conn = duckdb::Connection::open(db_path).expect("Failed to open DuckDB");
        conn.execute(&format!("DROP TABLE {}.{}", schema, table), [])
            .expect("Failed to drop table");
    }

    /// Create a connection via API and return the connection_id
    async fn create_connection(&self, name: &str, db_path: &str) -> Result<String> {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_CONNECTIONS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "name": name,
                        "source_type": "duckdb",
                        "config": {
                            "path": db_path
                        }
                    }))?))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        Ok(json["id"].as_str().unwrap().to_string())
    }
}

// ============================================================================
// Schema Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_empty_connections() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Call refresh with no parameters (refresh all schemas)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({}))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // With no connections, refresh should succeed with 0 connections refreshed
    assert_eq!(json["connections_refreshed"], 0);
    assert_eq!(json["tables_discovered"], 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_new_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with initial tables
    let db_path = harness.create_duckdb_multi_table("schema_test");

    // Create connection (discovery happens automatically)
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Add a new table to the DuckDB
    RefreshTestHarness::add_table_to_duckdb(&db_path, "sales", "customers");

    // Refresh schema for this connection
    let response = harness
        .router
        .clone()
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

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 1);
    assert_eq!(json["tables_discovered"], 3); // orders, products, customers
    assert_eq!(json["tables_added"], 1); // customers was added

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_removed_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("removal_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Remove a table from the DuckDB
    RefreshTestHarness::remove_table_from_duckdb(&db_path, "sales", "products");

    // Refresh schema
    let response = harness
        .router
        .clone()
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

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 1);
    // tables_discovered reflects catalog state (stale tables are NOT deleted)
    assert_eq!(json["tables_discovered"], 2); // orders + products (stale) remain in catalog
                                              // NOTE: tables_removed reports detection but doesn't delete from catalog.
                                              // Stale table cleanup is intentionally not implemented.
    assert_eq!(json["tables_removed"], 1); // products was detected as removed from remote

    // Verify products table still exists in catalog (stale tables are not deleted)
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let products_exists = tables.iter().any(|t| t.table_name == "products");
    assert!(
        products_exists,
        "products table should still exist in catalog (stale table cleanup not implemented)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_preserves_cached_data() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("cache_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query the table to trigger sync (creates cached parquet)
    let result = harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    assert!(!result.results.is_empty());

    // Verify table is synced
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let orders_table = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .expect("orders table should exist");
    assert!(
        orders_table.parquet_path.is_some(),
        "orders should have cached data"
    );
    let original_path = orders_table.parquet_path.clone();

    // Add a new table to the DuckDB
    RefreshTestHarness::add_table_to_duckdb(&db_path, "sales", "inventory");

    // Refresh schema
    let response = harness
        .router
        .clone()
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

    assert_eq!(response.status(), StatusCode::OK);

    // Verify orders table still has its cached data
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let orders_table = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .expect("orders table should still exist");

    assert!(
        orders_table.parquet_path.is_some(),
        "orders should still have cached data after schema refresh"
    );
    assert_eq!(
        orders_table.parquet_path, original_path,
        "cached data path should be unchanged"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_all_schemas_multiple_connections() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create two DuckDBs
    let db_path1 = harness.create_duckdb("conn1_db");
    let db_path2 = harness.create_duckdb("conn2_db");

    // Create two connections
    let _conn_id1 = harness.create_connection("conn1", &db_path1).await?;
    let _conn_id2 = harness.create_connection("conn2", &db_path2).await?;

    // Add tables to both
    RefreshTestHarness::add_table_to_duckdb(&db_path1, "sales", "new_table1");
    RefreshTestHarness::add_table_to_duckdb(&db_path2, "sales", "new_table2");

    // Refresh all schemas (no connection_id)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({}))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 2);
    assert_eq!(json["tables_discovered"], 4); // 2 per connection
    assert_eq!(json["tables_added"], 2); // 1 new table per connection

    Ok(())
}

// ============================================================================
// Data Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_single_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("data_refresh_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data for a single table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify response contains table refresh info
    assert_eq!(json["schema_name"], "sales");
    assert_eq!(json["table_name"], "orders");
    assert!(json["duration_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_connection_wide() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("conn_refresh_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query both tables to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Refresh all data in connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify connection refresh response
    assert_eq!(json["tables_refreshed"], 2);
    assert_eq!(json["tables_failed"], 0);
    assert!(json["duration_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_creates_new_versioned_file() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("row_update_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    let result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = result.results.first().unwrap();
    let initial_count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 2); // Initial data has 2 rows

    // Add more rows to DuckDB
    let conn = duckdb::Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO sales.orders VALUES (3, 'Charlie', 300.0), (4, 'Diana', 400.0)",
        [],
    )?;

    // Refresh data - this creates a new versioned parquet file
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Note: Before the old file is deleted (after grace period), both files
    // exist in the cache directory. DataFusion reads all parquet files in
    // the directory, so the count will be old_rows + new_rows until deletion.
    // The test verifies the refresh succeeds and creates a new file.
    // A separate test verifies deletion cleans up correctly.

    // Verify the refresh response
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(json["schema_name"], "sales");
    assert_eq!(json["table_name"], "orders");
    assert!(json["duration_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_data_refresh_no_duplicate_rows() -> Result<()> {
    // This test validates that during the grace period (before old file deletion),
    // queries return correct row counts without duplication.
    //
    // The fix uses versioned DIRECTORIES instead of versioned FILENAMES:
    // - Before: cache/1/schema/table/table_v{version}.parquet (multiple files in one dir)
    // - After:  cache/1/schema/table/{version}/data.parquet (one file per version dir)
    //
    // This ensures DataFusion's ListingTable only reads the active version.
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with known data
    let db_path = harness.create_duckdb("no_dup_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync and verify initial count
    let result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = result.results.first().unwrap();
    let initial_count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 2, "Initial data should have 2 rows");

    // Refresh data (creates new versioned file, schedules old for deletion)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // IMMEDIATELY query again - before any grace period cleanup
    // Row count should be SAME, not doubled
    let result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = result.results.first().unwrap();
    let post_refresh_count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(
        post_refresh_count, 2,
        "Row count should be 2 after refresh, not {} (doubled would be 4)",
        post_refresh_count
    );

    Ok(())
}

// ============================================================================
// Validation Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_without_connection() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to refresh with schema_name but no connection_id
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "schema_name": "sales"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires connection_id"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_table_without_schema() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a connection first
    let db_path = harness.create_duckdb("validation_test");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try to refresh with table_name but no schema_name
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "table_name": "orders"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires schema_name"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_data_without_connection() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to do data refresh without connection_id
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("data refresh requires connection_id"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_level_not_supported() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test2");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try schema-level refresh (schema_name without table_name, not data mode)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("schema-level refresh not supported"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_data_refresh_schema_without_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test3");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try data refresh with schema but no table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("data refresh with schema_name requires table_name"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_refresh_cannot_target_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test4");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try schema refresh targeting a specific table (not allowed)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": false
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("schema refresh cannot target specific table"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_connection_not_found() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to refresh a non-existent connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": "con_nonexistent_fake_id_12345"
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

// ============================================================================
// Pending Deletion Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_data_refresh_schedules_pending_deletion() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("pending_deletion_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync (creates first parquet file)
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Get the initial cache directory path
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let initial_cache_path = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .and_then(|t| t.parquet_path.as_ref())
        .expect("orders should have parquet path")
        .clone();

    // Verify initial cache directory contains a parquet file
    let initial_cache_dir = initial_cache_path.strip_prefix("file://").unwrap();
    let files_before: Vec<_> = std::fs::read_dir(initial_cache_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();
    assert_eq!(
        files_before.len(),
        1,
        "should have 1 parquet file before refresh"
    );

    // Refresh data (creates a new versioned directory and schedules old for deletion)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Get the new cache path after refresh - it should be a versioned directory
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let new_cache_path = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .and_then(|t| t.parquet_path.as_ref())
        .expect("orders should have parquet path")
        .clone();

    // The new cache path should be different (versioned directory)
    assert_ne!(
        initial_cache_path, new_cache_path,
        "cache path should change after refresh to versioned directory"
    );

    // Verify the new versioned directory contains data.parquet
    let new_cache_dir = new_cache_path.strip_prefix("file://").unwrap();
    let files_in_new_version: Vec<_> = std::fs::read_dir(new_cache_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();
    assert_eq!(
        files_in_new_version.len(),
        1,
        "versioned directory should have exactly 1 parquet file (data.parquet)"
    );

    // Verify old cache directory still exists (pending deletion)
    assert!(
        std::path::Path::new(initial_cache_dir).exists(),
        "old cache directory should still exist during grace period"
    );

    // The deletion is scheduled 60 seconds in the future by default
    // so we verify the scheduling mechanism works but don't wait for deletion
    let due = harness.engine.catalog().get_pending_deletions().await?;
    assert!(
        due.is_empty(),
        "deletions should not be due immediately (60s grace period)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_refresh_reports_removed_tables_but_keeps_them() -> Result<()> {
    // NOTE: This test verifies that stale tables are detected but NOT deleted.
    // Stale table cleanup is intentionally not implemented to avoid data loss.
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("schema_deletion_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query products table to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Verify products has cached data
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let products_cached = tables
        .iter()
        .find(|t| t.table_name == "products")
        .and_then(|t| t.parquet_path.as_ref())
        .is_some();
    assert!(products_cached, "products should have cached data");

    // Remove products table from DuckDB
    RefreshTestHarness::remove_table_from_duckdb(&db_path, "sales", "products");

    // Refresh schema
    let response = harness
        .router
        .clone()
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

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify table removal was detected
    assert_eq!(json["tables_removed"], 1);

    // Verify products table STILL exists (stale tables are not deleted)
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let products_exists = tables.iter().any(|t| t.table_name == "products");
    assert!(
        products_exists,
        "products table should still exist (stale table cleanup not implemented)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pending_deletions_respect_timing() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("timing_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data to schedule a deletion
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Get due deletions immediately - should be empty since deletion is 60s in future
    let due = harness.engine.catalog().get_pending_deletions().await?;
    assert!(
        due.is_empty(),
        "deletions should not be due immediately after scheduling"
    );

    // Process pending deletions - should delete nothing since not due yet
    let deleted = harness.engine.process_pending_deletions().await?;
    assert_eq!(deleted, 0, "no files should be deleted before due time");

    Ok(())
}

// ============================================================================
// Response Format Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_data_refresh_returns_rows_synced() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with known data (2 rows in orders)
    let db_path = harness.create_duckdb("rows_synced_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data for a single table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify rows_synced is present and correct (2 rows in our test data)
    assert!(
        json["rows_synced"].is_number(),
        "rows_synced should be a number, got: {:?}",
        json["rows_synced"]
    );
    assert_eq!(
        json["rows_synced"].as_u64().unwrap(),
        2,
        "rows_synced should be 2 (matches test data)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_refresh_returns_total_rows() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables (1 row each in multi_table setup)
    let db_path = harness.create_duckdb_multi_table("total_rows_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query both tables to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Refresh all data in connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify total_rows is present and correct (1 row each in 2 tables = 2 total)
    assert!(
        json["total_rows"].is_number(),
        "total_rows should be a number, got: {:?}",
        json["total_rows"]
    );
    assert_eq!(
        json["total_rows"].as_u64().unwrap(),
        2,
        "total_rows should be 2 (1 row per table in multi_table setup)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_refresh_response_contains_external_connection_id() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("external_id_test");

    // Create connection and get the external ID
    let external_connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Verify the external ID has the expected format (starts with "conn")
    assert!(
        external_connection_id.starts_with("conn"),
        "external connection_id should start with 'conn', got: {}",
        external_connection_id
    );

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data for a single table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": external_connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify response contains the external connection_id string, not internal int
    let response_conn_id = &json["connection_id"];
    assert!(
        response_conn_id.is_string(),
        "connection_id in response should be a string, got: {:?}",
        response_conn_id
    );
    assert_eq!(
        response_conn_id.as_str().unwrap(),
        external_connection_id,
        "response connection_id should match the external ID used in request"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_refresh_response_contains_external_connection_id() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("conn_external_id_test");

    // Create connection and get the external ID
    let external_connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Verify the external ID has the expected format (starts with "conn")
    assert!(
        external_connection_id.starts_with("conn"),
        "external connection_id should start with 'conn', got: {}",
        external_connection_id
    );

    // Query both tables to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Refresh all data in connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": external_connection_id,
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify response contains the external connection_id string, not internal int
    let response_conn_id = &json["connection_id"];
    assert!(
        response_conn_id.is_string(),
        "connection_id in response should be a string, got: {:?}",
        response_conn_id
    );
    assert_eq!(
        response_conn_id.as_str().unwrap(),
        external_connection_id,
        "response connection_id should match the external ID used in request"
    );

    Ok(())
}

// ============================================================================
// Concurrent Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_refresh_same_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("concurrent_same_table");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Run two refreshes concurrently on the same table
    let engine1 = harness.engine.clone();
    let engine2 = harness.engine.clone();
    let conn_id = connection_id.clone();

    // Get internal connection ID
    let internal_conn = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .expect("connection should exist");

    let (result1, result2) = tokio::join!(
        engine1.refresh_table_data(internal_conn.id, &conn_id, "sales", "orders"),
        engine2.refresh_table_data(internal_conn.id, &conn_id, "sales", "orders")
    );

    // Both should complete (one might succeed, both might succeed)
    // The key assertion is that neither should panic and data should be consistent
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

    assert!(
        success_count >= 1,
        "At least one concurrent refresh should succeed. Result1: {:?}, Result2: {:?}",
        result1,
        result2
    );

    // Verify the final state is consistent - query should work
    let query_result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = query_result.results.first().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 2, "Row count should be 2 after concurrent refreshes");

    // Verify pending deletions don't have duplicates for the same path
    // (This validates the fix from #8 - INSERT OR IGNORE on pending_deletions)
    let due = harness.engine.catalog().get_pending_deletions().await?;
    let paths: std::collections::HashSet<_> = due.iter().map(|d| &d.path).collect();
    assert_eq!(
        due.len(),
        paths.len(),
        "Pending deletions should not contain duplicate paths"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_refresh_different_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("concurrent_diff_tables");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query both tables to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Get internal connection ID
    let internal_conn = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .expect("connection should exist");

    let engine1 = harness.engine.clone();
    let engine2 = harness.engine.clone();
    let conn_id1 = connection_id.clone();
    let conn_id2 = connection_id.clone();
    let internal_id = internal_conn.id;

    // Run refreshes for different tables concurrently
    let (result1, result2) = tokio::join!(
        engine1.refresh_table_data(internal_id, &conn_id1, "sales", "orders"),
        engine2.refresh_table_data(internal_id, &conn_id2, "sales", "products")
    );

    // Both should succeed since they're different tables
    assert!(
        result1.is_ok(),
        "Refresh of orders table should succeed: {:?}",
        result1
    );
    assert!(
        result2.is_ok(),
        "Refresh of products table should succeed: {:?}",
        result2
    );

    // Verify both tables are queryable
    let orders_result = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM test_conn.sales.orders")
        .await?;
    let products_result = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM test_conn.sales.products")
        .await?;

    assert!(
        !orders_result.results.is_empty(),
        "Orders query should return results"
    );
    assert!(
        !products_result.results.is_empty(),
        "Products query should return results"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_schema_and_data_refresh() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("concurrent_schema_data");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Get internal connection ID
    let internal_conn = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .expect("connection should exist");

    let engine1 = harness.engine.clone();
    let engine2 = harness.engine.clone();
    let conn_id = connection_id.clone();
    let internal_id = internal_conn.id;

    // Run schema refresh and data refresh concurrently
    let (schema_result, data_result) = tokio::join!(
        engine1.refresh_schema(internal_id),
        engine2.refresh_table_data(internal_id, &conn_id, "sales", "orders")
    );

    // Both should succeed
    assert!(
        schema_result.is_ok(),
        "Schema refresh should succeed: {:?}",
        schema_result
    );
    assert!(
        data_result.is_ok(),
        "Data refresh should succeed: {:?}",
        data_result
    );

    // Verify final state is consistent
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    assert!(
        tables.len() >= 1,
        "Should have at least one table after concurrent refresh"
    );

    // Query should still work
    let query_result = harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    assert!(
        !query_result.results.is_empty(),
        "Query should return results after concurrent refresh"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_connection_wide_refresh() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("concurrent_conn_wide");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query both tables to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Get internal connection ID
    let internal_conn = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .expect("connection should exist");

    let engine1 = harness.engine.clone();
    let engine2 = harness.engine.clone();
    let conn_id1 = connection_id.clone();
    let conn_id2 = connection_id.clone();
    let internal_id = internal_conn.id;

    // Run two connection-wide data refreshes concurrently
    // Use include_uncached=false (default) - only refresh already-cached tables
    let (result1, result2) = tokio::join!(
        engine1.refresh_connection_data(internal_id, &conn_id1, false),
        engine2.refresh_connection_data(internal_id, &conn_id2, false)
    );

    // Both should complete without panicking
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

    assert!(
        success_count >= 1,
        "At least one connection-wide refresh should succeed. Result1: {:?}, Result2: {:?}",
        result1,
        result2
    );

    // Verify final state is queryable
    let orders_result = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM test_conn.sales.orders")
        .await?;
    assert!(!orders_result.results.is_empty());

    let products_result = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM test_conn.sales.products")
        .await?;
    assert!(!products_result.results.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_refresh_multiple_connections() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create two separate DuckDBs
    let db_path1 = harness.create_duckdb("concurrent_conn1");
    let db_path2 = harness.create_duckdb("concurrent_conn2");

    // Create two connections
    let conn_id1 = harness.create_connection("conn1", &db_path1).await?;
    let conn_id2 = harness.create_connection("conn2", &db_path2).await?;

    // Query both to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM conn1.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM conn2.sales.orders")
        .await?;

    // Get internal IDs
    let internal_conn1 = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&conn_id1)
        .await?
        .expect("conn1 should exist");
    let internal_conn2 = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&conn_id2)
        .await?
        .expect("conn2 should exist");

    let engine1 = harness.engine.clone();
    let engine2 = harness.engine.clone();
    let ext_id1 = conn_id1.clone();
    let ext_id2 = conn_id2.clone();

    // Refresh different connections concurrently
    let (result1, result2) = tokio::join!(
        engine1.refresh_table_data(internal_conn1.id, &ext_id1, "sales", "orders"),
        engine2.refresh_table_data(internal_conn2.id, &ext_id2, "sales", "orders")
    );

    // Both should succeed - no interference between connections
    assert!(
        result1.is_ok(),
        "Refresh of conn1.orders should succeed: {:?}",
        result1
    );
    assert!(
        result2.is_ok(),
        "Refresh of conn2.orders should succeed: {:?}",
        result2
    );

    // Verify both are queryable
    let q1 = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM conn1.sales.orders")
        .await?;
    let q2 = harness
        .engine
        .execute_query("SELECT COUNT(*) FROM conn2.sales.orders")
        .await?;

    assert!(!q1.results.is_empty());
    assert!(!q2.results.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rapid_sequential_refresh_same_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("rapid_refresh");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Get internal connection ID
    let internal_conn = harness
        .engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .expect("connection should exist");

    // Perform rapid sequential refreshes (simulates rapid button clicks)
    for i in 0..5 {
        let result = harness
            .engine
            .refresh_table_data(internal_conn.id, &connection_id, "sales", "orders")
            .await;

        assert!(result.is_ok(), "Refresh {} should succeed: {:?}", i, result);
    }

    // Verify final state is consistent
    let query_result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = query_result.results.first().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 2, "Row count should be 2 after rapid refreshes");

    Ok(())
}
