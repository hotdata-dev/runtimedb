//! Golden path integration tests for RuntimeDB.
//!
//! Tests verify the complete workflow: create connection, discover tables, query data.
//! Uses a unified test harness that runs the same assertions against:
//! - Multiple data sources (DuckDB, PostgreSQL)
//! - Multiple access methods (Engine API, REST API)

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use runtimedb::http::app_server::{
    AppServer, PATH_CONNECTIONS, PATH_INFORMATION_SCHEMA, PATH_QUERY, PATH_RESULT, PATH_RESULTS,
};
use runtimedb::source::Source;
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

// ============================================================================
// Unified Test Abstractions
// ============================================================================

/// Normalized query result that works for both engine and API responses.
struct QueryResult {
    row_count: usize,
    columns: Vec<String>,
    /// Nullable flags for each column (parallel to columns vec)
    nullable_flags: Vec<bool>,
    result_id: Option<String>,
}

impl QueryResult {
    fn from_engine(response: &runtimedb::QueryResponse) -> Self {
        // Use the schema field which is always present, even for empty results
        let columns: Vec<String> = response
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let nullable_flags: Vec<bool> = response
            .schema
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect();

        let row_count: usize = response.results.iter().map(|b| b.num_rows()).sum();

        Self {
            row_count,
            columns,
            nullable_flags,
            result_id: None, // Engine doesn't return result_id directly
        }
    }

    fn from_api(json: &serde_json::Value) -> Self {
        let columns = json["columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let nullable_flags = json["nullable"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_bool()).collect())
            .unwrap_or_default();

        let row_count = json["row_count"].as_u64().unwrap_or(0) as usize;
        let result_id = json["result_id"].as_str().map(String::from);

        Self {
            row_count,
            columns,
            nullable_flags,
            result_id,
        }
    }

    fn assert_row_count(&self, expected: usize) {
        assert_eq!(
            self.row_count, expected,
            "Expected {} rows, got {}",
            expected, self.row_count
        );
    }

    fn assert_columns(&self, expected: &[&str]) {
        let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(self.columns, expected, "Column mismatch");
    }

    /// Assert nullable flags match expected values.
    /// Takes a slice of (column_name, expected_nullable) tuples.
    fn assert_nullable_flags(&self, expected: &[(&str, bool)]) {
        for (col_name, expected_nullable) in expected {
            let idx = self
                .columns
                .iter()
                .position(|c| c == *col_name)
                .unwrap_or_else(|| panic!("Column '{}' not found in result", col_name));

            let actual_nullable = self.nullable_flags.get(idx).copied().unwrap_or(true);
            assert_eq!(
                actual_nullable, *expected_nullable,
                "Column '{}' nullable mismatch: expected {}, got {}",
                col_name, expected_nullable, actual_nullable
            );
        }
    }

    fn assert_has_result_id(&self) {
        assert!(self.result_id.is_some(), "Expected result_id to be present");
    }

    fn get_result_id(&self) -> Option<&str> {
        self.result_id.as_deref()
    }
}

/// Connection info from list operations.
struct ConnectionResult {
    count: usize,
    names: Vec<String>,
    ids: Vec<String>,
}

impl ConnectionResult {
    fn from_engine(connections: &[runtimedb::catalog::ConnectionInfo]) -> Self {
        Self {
            count: connections.len(),
            names: connections.iter().map(|c| c.name.clone()).collect(),
            ids: connections.iter().map(|c| c.external_id.clone()).collect(),
        }
    }

    fn from_api(json: &serde_json::Value) -> Self {
        let arr = json["connections"].as_array();
        Self {
            count: arr.map(|a| a.len()).unwrap_or(0),
            names: arr
                .map(|a| {
                    a.iter()
                        .filter_map(|c| c["name"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            ids: arr
                .map(|a| {
                    a.iter()
                        .filter_map(|c| c["id"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    fn assert_has_connection(&self, name: &str) {
        assert!(
            self.names.contains(&name.to_string()),
            "Connection '{}' not found in {:?}",
            name,
            self.names
        );
    }

    fn assert_count(&self, expected: usize) {
        assert_eq!(self.count, expected, "Expected {} connections", expected);
    }

    fn get_id_by_name(&self, name: &str) -> Option<String> {
        self.names
            .iter()
            .position(|n| n == name)
            .and_then(|idx| self.ids.get(idx).cloned())
    }
}

/// Table info from list operations.
struct TablesResult {
    tables: Vec<(String, String, bool)>, // (schema, table, synced)
}

impl TablesResult {
    fn from_engine(tables: &[runtimedb::catalog::TableInfo]) -> Self {
        Self {
            tables: tables
                .iter()
                .map(|t| {
                    (
                        t.schema_name.clone(),
                        t.table_name.clone(),
                        t.parquet_path.is_some(),
                    )
                })
                .collect(),
        }
    }

    fn from_api(json: &serde_json::Value) -> Self {
        let tables = json["tables"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let schema = t["schema"].as_str()?;
                        let table = t["table"].as_str()?;
                        let synced = t["synced"].as_bool().unwrap_or(false);
                        Some((schema.to_string(), table.to_string(), synced))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self { tables }
    }

    fn assert_has_table(&self, schema: &str, table: &str) {
        assert!(
            self.tables
                .iter()
                .any(|(s, t, _)| s == schema && t == table),
            "Table '{}.{}' not found. Available: {:?}",
            schema,
            table,
            self.tables
        );
    }

    fn assert_not_empty(&self) {
        assert!(!self.tables.is_empty(), "Expected tables but found none");
    }

    fn synced_count(&self) -> usize {
        self.tables.iter().filter(|(_, _, synced)| *synced).count()
    }

    fn assert_none_synced(&self) {
        assert_eq!(self.synced_count(), 0, "Expected no tables to be synced");
    }

    fn is_table_synced(&self, schema: &str, table: &str) -> bool {
        self.tables
            .iter()
            .find(|(s, t, _)| s == schema && t == table)
            .map(|(_, _, synced)| *synced)
            .unwrap_or(false)
    }
}

/// Result list from GET /results
struct ResultsListResult {
    count: usize,
    #[allow(dead_code)]
    has_more: bool,
    result_ids: Vec<String>,
}

impl ResultsListResult {
    fn from_api(json: &serde_json::Value) -> Self {
        let results = json["results"].as_array();
        Self {
            count: json["count"].as_u64().unwrap_or(0) as usize,
            has_more: json["has_more"].as_bool().unwrap_or(false),
            result_ids: results
                .map(|arr| {
                    arr.iter()
                        .filter_map(|r| r["id"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    fn assert_count(&self, expected: usize) {
        assert_eq!(self.count, expected, "Expected {} results", expected);
    }

    fn assert_contains(&self, result_id: &str) {
        assert!(
            self.result_ids.contains(&result_id.to_string()),
            "Result '{}' not found in list",
            result_id
        );
    }
}

/// Single connection details from get operations.
#[allow(dead_code)]
struct ConnectionDetails {
    id: String,
    name: String,
    source_type: String,
    table_count: usize,
    synced_table_count: usize,
}

impl ConnectionDetails {
    fn from_engine(
        conn: &runtimedb::catalog::ConnectionInfo,
        tables: &[runtimedb::catalog::TableInfo],
    ) -> Self {
        Self {
            id: conn.external_id.clone(),
            name: conn.name.clone(),
            source_type: conn.source_type.clone(),
            table_count: tables.len(),
            synced_table_count: tables.iter().filter(|t| t.parquet_path.is_some()).count(),
        }
    }

    fn from_api(json: &serde_json::Value) -> Self {
        Self {
            id: json["id"].as_str().unwrap_or("").to_string(),
            name: json["name"].as_str().unwrap_or("").to_string(),
            source_type: json["source_type"].as_str().unwrap_or("").to_string(),
            table_count: json["table_count"].as_u64().unwrap_or(0) as usize,
            synced_table_count: json["synced_table_count"].as_u64().unwrap_or(0) as usize,
        }
    }
}

/// Trait for test execution - allows same test logic for engine vs API.
#[async_trait::async_trait]
trait TestExecutor: Send + Sync {
    /// Connect and return an identifier (name for engine, connection_id for API)
    async fn connect(&self, name: &str, source: &Source, secret_name: Option<&str>) -> String;
    async fn list_connections(&self) -> ConnectionResult;
    async fn list_tables(&self, connection_name: &str) -> TablesResult;
    async fn query(&self, sql: &str) -> QueryResult;

    // CRUD operations - identifier is name for engine, connection_id for API
    async fn get_connection(&self, identifier: &str) -> Option<ConnectionDetails>;
    async fn delete_connection(&self, identifier: &str) -> bool;
    async fn purge_connection_cache(&self, identifier: &str) -> bool;
    async fn purge_table_cache(&self, identifier: &str, schema: &str, table: &str) -> bool;

    // Result persistence operations (API-only for now)
    async fn get_result(&self, result_id: &str) -> Option<QueryResult>;
    async fn list_results(&self) -> ResultsListResult;
}

/// Engine-based test executor.
struct EngineExecutor {
    engine: Arc<RuntimeEngine>,
}

#[async_trait::async_trait]
impl TestExecutor for EngineExecutor {
    async fn connect(&self, name: &str, source: &Source, secret_name: Option<&str>) -> String {
        // Resolve secret name to ID if provided (matching HTTP handler behavior)
        let secret_id = if let Some(sn) = secret_name {
            let metadata = self
                .engine
                .secret_manager()
                .get_metadata(sn)
                .await
                .expect("Failed to get secret metadata");
            Some(metadata.id)
        } else {
            None
        };

        self.engine
            .connect(name, source.clone(), secret_id.as_deref())
            .await
            .expect("Engine connect failed");
        name.to_string() // Engine uses name as identifier
    }

    async fn list_connections(&self) -> ConnectionResult {
        ConnectionResult::from_engine(&self.engine.list_connections().await.unwrap())
    }

    async fn list_tables(&self, connection_name: &str) -> TablesResult {
        TablesResult::from_engine(
            &self
                .engine
                .list_tables(Some(connection_name))
                .await
                .unwrap(),
        )
    }

    async fn query(&self, sql: &str) -> QueryResult {
        QueryResult::from_engine(&self.engine.execute_query(sql).await.unwrap())
    }

    async fn get_connection(&self, identifier: &str) -> Option<ConnectionDetails> {
        // For engine, identifier is the name
        let conn = self
            .engine
            .catalog()
            .get_connection(identifier)
            .await
            .ok()??;
        let tables = self.engine.list_tables(Some(identifier)).await.ok()?;
        Some(ConnectionDetails::from_engine(&conn, &tables))
    }

    async fn delete_connection(&self, identifier: &str) -> bool {
        // For engine, identifier is the name
        self.engine.remove_connection(identifier).await.is_ok()
    }

    async fn purge_connection_cache(&self, identifier: &str) -> bool {
        // For engine, identifier is the name
        self.engine.purge_connection(identifier).await.is_ok()
    }

    async fn purge_table_cache(&self, identifier: &str, schema: &str, table: &str) -> bool {
        // For engine, identifier is the name
        self.engine
            .purge_table(identifier, schema, table)
            .await
            .is_ok()
    }

    async fn get_result(&self, _result_id: &str) -> Option<QueryResult> {
        // Engine doesn't expose result persistence directly in the same way as API
        // For integration tests, we only test via API
        None
    }

    async fn list_results(&self) -> ResultsListResult {
        // Engine doesn't expose result listing directly
        ResultsListResult {
            count: 0,
            has_more: false,
            result_ids: vec![],
        }
    }
}

/// REST API-based test executor.
struct ApiExecutor {
    router: Router,
}

#[async_trait::async_trait]
impl TestExecutor for ApiExecutor {
    async fn connect(&self, name: &str, source: &Source, secret_name: Option<&str>) -> String {
        let (source_type, config) = match source {
            Source::Duckdb { path } => ("duckdb", json!({ "path": path })),
            Source::Postgres {
                host,
                port,
                user,
                database,
                auth,
            } => (
                "postgres",
                json!({ "host": host, "port": port, "user": user, "database": database, "auth": auth }),
            ),
            Source::Mysql {
                host,
                port,
                user,
                database,
                auth,
            } => (
                "mysql",
                json!({ "host": host, "port": port, "user": user, "database": database, "auth": auth }),
            ),
            _ => panic!("Unsupported source type"),
        };

        let mut body_json = json!({
            "name": name,
            "source_type": source_type,
            "config": config,
        });
        if let Some(sn) = secret_name {
            body_json["secret_name"] = json!(sn);
        }

        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_CONNECTIONS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body_json).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED, "API connect failed");

        // Extract and return connection_id from response
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        json["id"].as_str().unwrap_or("").to_string()
    }

    async fn list_connections(&self) -> ConnectionResult {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(PATH_CONNECTIONS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        ConnectionResult::from_api(&serde_json::from_slice(&body).unwrap())
    }

    async fn list_tables(&self, connection_name: &str) -> TablesResult {
        // First get connection_id from connection name
        let connections = self.list_connections().await;
        let connection_id = connections
            .get_id_by_name(connection_name)
            .expect("Connection not found for list_tables");

        let uri = format!(
            "{}?connection_id={}",
            PATH_INFORMATION_SCHEMA, connection_id
        );
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        TablesResult::from_api(&serde_json::from_slice(&body).unwrap())
    }

    async fn query(&self, sql: &str) -> QueryResult {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_QUERY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&json!({ "sql": sql })).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "Query failed: {}", sql);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        QueryResult::from_api(&serde_json::from_slice(&body).unwrap())
    }

    async fn get_connection(&self, identifier: &str) -> Option<ConnectionDetails> {
        // For API, identifier is connection_id
        let uri = format!("/connections/{}", identifier);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        if response.status() != StatusCode::OK {
            return None;
        }

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Some(ConnectionDetails::from_api(
            &serde_json::from_slice(&body).unwrap(),
        ))
    }

    async fn delete_connection(&self, identifier: &str) -> bool {
        // For API, identifier is connection_id
        let uri = format!("/connections/{}", identifier);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        response.status() == StatusCode::NO_CONTENT
    }

    async fn purge_connection_cache(&self, identifier: &str) -> bool {
        // For API, identifier is connection_id
        let uri = format!("/connections/{}/cache", identifier);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        response.status() == StatusCode::NO_CONTENT
    }

    async fn purge_table_cache(&self, identifier: &str, schema: &str, table: &str) -> bool {
        // For API, identifier is connection_id
        let uri = format!(
            "/connections/{}/tables/{}/{}/cache",
            identifier, schema, table
        );
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        response.status() == StatusCode::NO_CONTENT
    }

    async fn get_result(&self, result_id: &str) -> Option<QueryResult> {
        let uri = PATH_RESULT.replace("{id}", result_id);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        if response.status() != StatusCode::OK {
            return None;
        }

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Some(QueryResult::from_api(
            &serde_json::from_slice(&body).unwrap(),
        ))
    }

    async fn list_results(&self) -> ResultsListResult {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(PATH_RESULTS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        ResultsListResult::from_api(&serde_json::from_slice(&body).unwrap())
    }
}

/// Generate a random base64-encoded 32-byte key for test secret manager.
fn generate_test_secret_key() -> String {
    use base64::Engine;
    let key_bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(key_bytes)
}

/// Test context providing both executors.
struct TestHarness {
    engine_executor: EngineExecutor,
    api_executor: ApiExecutor,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestHarness {
    async fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();

        // Generate a test secret key to enable the secret manager
        let secret_key = generate_test_secret_key();

        let engine = RuntimeEngine::builder()
            .base_dir(temp_dir.path())
            .secret_key(secret_key)
            .build()
            .await
            .unwrap();

        let app = AppServer::new(engine);

        Self {
            engine_executor: EngineExecutor { engine: app.engine },
            api_executor: ApiExecutor { router: app.router },
            temp_dir,
        }
    }

    fn engine(&self) -> &dyn TestExecutor {
        &self.engine_executor
    }

    fn api(&self) -> &dyn TestExecutor {
        &self.api_executor
    }

    /// Store a secret for use in connection credentials.
    async fn store_secret(&self, name: &str, value: &str) {
        let secret_manager = self.engine_executor.engine.secret_manager();
        secret_manager
            .create(name, value.as_bytes())
            .await
            .expect("Failed to store test secret");
    }
}

// ============================================================================
// Shared Test Scenarios
// ============================================================================

/// SQL queries used across all tests.
mod queries {
    pub fn select_orders(conn: &str) -> String {
        format!(
            "SELECT order_id, customer_name, amount FROM {}.sales.orders ORDER BY order_id",
            conn
        )
    }

    pub fn count_paid_orders(conn: &str) -> String {
        format!("SELECT COUNT(*) as cnt, SUM(amount) as total FROM {}.sales.orders WHERE is_paid = true", conn)
    }

    pub fn select_all(conn: &str, schema: &str, table: &str) -> String {
        format!("SELECT * FROM {}.{}.{}", conn, schema, table)
    }

    pub fn select_nullable_test(conn: &str, schema: &str) -> String {
        format!(
            "SELECT c_custkey, c_name, c_acctbal, c_comment FROM {}.{}.customer ORDER BY c_custkey",
            conn, schema
        )
    }

    pub fn select_empty_nullable_test(conn: &str, schema: &str) -> String {
        format!(
            "SELECT c_custkey, c_name, c_acctbal, c_comment FROM {}.{}.empty_customer ORDER BY c_custkey",
            conn, schema
        )
    }
}

/// Run the golden path test scenario against any executor and source.
async fn run_golden_path_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    secret_name: Option<&str>,
) {
    // Connect and get identifier
    let _conn_id = executor.connect(conn_name, source, secret_name).await;

    // Verify connection exists
    let connections = executor.list_connections().await;
    connections.assert_count(1);
    connections.assert_has_connection(conn_name);

    // Verify tables discovered (uses connection name for filter)
    let tables = executor.list_tables(conn_name).await;
    tables.assert_not_empty();
    tables.assert_has_table("sales", "orders");

    // Query and verify (uses connection name in SQL)
    let result = executor.query(&queries::select_orders(conn_name)).await;
    result.assert_row_count(4);
    result.assert_columns(&["order_id", "customer_name", "amount"]);

    // Aggregation
    let agg = executor.query(&queries::count_paid_orders(conn_name)).await;
    agg.assert_row_count(1);
}

/// Run multi-schema test scenario.
async fn run_multi_schema_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    secret_name: Option<&str>,
) {
    let _conn_id = executor.connect(conn_name, source, secret_name).await;

    let tables = executor.list_tables(conn_name).await;
    tables.assert_has_table("sales", "orders");
    tables.assert_has_table("inventory", "products");

    let orders = executor
        .query(&queries::select_all(conn_name, "sales", "orders"))
        .await;
    orders.assert_row_count(1);

    let products = executor
        .query(&queries::select_all(conn_name, "inventory", "products"))
        .await;
    products.assert_row_count(1);
}

/// Run delete connection test scenario.
async fn run_delete_connection_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    secret_name: Option<&str>,
) {
    // Create connection and get identifier
    let conn_id = executor.connect(conn_name, source, secret_name).await;

    // Verify it exists using identifier
    let conn = executor.get_connection(&conn_id).await;
    assert!(conn.is_some(), "Connection should exist after creation");

    // Delete using identifier
    let deleted = executor.delete_connection(&conn_id).await;
    assert!(deleted, "Delete should succeed");

    // Verify it's gone
    let conn = executor.get_connection(&conn_id).await;
    assert!(conn.is_none(), "Connection should not exist after deletion");

    // Verify delete of non-existent returns false
    let deleted_again = executor.delete_connection(&conn_id).await;
    assert!(!deleted_again, "Delete of non-existent should fail");
}

/// Run purge connection cache test scenario.
async fn run_purge_connection_cache_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    secret_name: Option<&str>,
) {
    // Create connection and get identifier
    let conn_id = executor.connect(conn_name, source, secret_name).await;

    // Query to trigger sync (uses connection name in SQL)
    let _ = executor.query(&queries::select_orders(conn_name)).await;

    // Verify tables are synced (uses connection name for filter)
    let tables = executor.list_tables(conn_name).await;
    assert!(
        tables.synced_count() > 0,
        "Should have synced tables after query"
    );

    // Purge cache using identifier
    let purged = executor.purge_connection_cache(&conn_id).await;
    assert!(purged, "Purge should succeed");

    // Verify tables still exist but not synced
    let tables = executor.list_tables(conn_name).await;
    tables.assert_not_empty();
    tables.assert_none_synced();

    // Connection should still exist
    let conn = executor.get_connection(&conn_id).await;
    assert!(
        conn.is_some(),
        "Connection should still exist after cache purge"
    );
}

/// Run purge table cache test scenario.
async fn run_purge_table_cache_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    secret_name: Option<&str>,
) {
    // Create connection and get identifier
    let conn_id = executor.connect(conn_name, source, secret_name).await;

    // Query to trigger sync of orders table (uses connection name in SQL)
    let _ = executor.query(&queries::select_orders(conn_name)).await;

    // Verify orders table is synced (uses connection name for filter)
    let tables = executor.list_tables(conn_name).await;
    assert!(
        tables.is_table_synced("sales", "orders"),
        "orders should be synced"
    );

    // Purge just the orders table cache using identifier
    let purged = executor
        .purge_table_cache(&conn_id, "sales", "orders")
        .await;
    assert!(purged, "Purge table should succeed");

    // Verify orders is no longer synced
    let tables = executor.list_tables(conn_name).await;
    assert!(
        !tables.is_table_synced("sales", "orders"),
        "orders should not be synced after purge"
    );

    // Table should still be listed
    tables.assert_has_table("sales", "orders");
}

/// Run nullable flags preservation test scenario.
/// Verifies fix for issue #59: columns declared as NOT NULL should have nullable=false
/// in the resulting Arrow schema after sync.
async fn run_nullable_flags_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    schema_name: &str,
    secret_name: Option<&str>,
) {
    // Connect
    let _conn_id = executor.connect(conn_name, source, secret_name).await;

    // Query to trigger sync - this will create the parquet cache with schema
    let result = executor
        .query(&queries::select_nullable_test(conn_name, schema_name))
        .await;

    // Verify we got all 3 rows (including rows with NULLs in nullable columns)
    result.assert_row_count(3);
    result.assert_columns(&["c_custkey", "c_name", "c_acctbal", "c_comment"]);

    // Verify nullable flags are correctly preserved (issue #59)
    // c_custkey and c_name are NOT NULL, c_acctbal and c_comment are nullable
    result.assert_nullable_flags(&[
        ("c_custkey", false), // NOT NULL
        ("c_name", false),    // NOT NULL
        ("c_acctbal", true),  // nullable
        ("c_comment", true),  // nullable
    ]);
}

/// Run empty table nullable flags preservation test scenario.
/// Verifies that nullable flags are correctly preserved even when table has no data rows.
/// This is critical because the fix queries information_schema rather than inferring from data.
async fn run_empty_nullable_flags_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
    schema_name: &str,
    secret_name: Option<&str>,
) {
    // Connect
    let _conn_id = executor.connect(conn_name, source, secret_name).await;

    // Query to trigger sync - this will create the parquet cache with schema
    // The table is empty, so we expect 0 rows but correct column schema
    let result = executor
        .query(&queries::select_empty_nullable_test(conn_name, schema_name))
        .await;

    // Verify we got 0 rows but correct columns
    result.assert_row_count(0);
    result.assert_columns(&["c_custkey", "c_name", "c_acctbal", "c_comment"]);

    // Verify nullable flags are correctly preserved even for empty tables (issue #59)
    // This is the critical case - nullable must come from information_schema, not data
    result.assert_nullable_flags(&[
        ("c_custkey", false), // NOT NULL
        ("c_name", false),    // NOT NULL
        ("c_acctbal", true),  // nullable
        ("c_comment", true),  // nullable
    ]);
}

// ============================================================================
// Data Source Fixtures
// ============================================================================

mod fixtures {
    use super::*;

    /// Creates a DuckDB with standard test data (sales.orders with 4 rows).
    pub fn duckdb_standard() -> (TempDir, Source) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("source.duckdb");
        let conn = duckdb::Connection::open(&db_path).unwrap();

        conn.execute("CREATE SCHEMA sales", []).unwrap();
        conn.execute(
            "CREATE TABLE sales.orders (order_id INTEGER, customer_name VARCHAR, amount DOUBLE, is_paid BOOLEAN)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sales.orders VALUES (1, 'Alice', 100.50, true), (2, 'Bob', 250.75, false), (3, 'Charlie', 75.25, true), (4, 'David', 500.00, true)",
            [],
        ).unwrap();

        (
            temp_dir,
            Source::Duckdb {
                path: db_path.to_str().unwrap().to_string(),
            },
        )
    }

    /// Creates a DuckDB with multiple schemas.
    pub fn duckdb_multi_schema() -> (TempDir, Source) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("source.duckdb");
        let conn = duckdb::Connection::open(&db_path).unwrap();

        conn.execute("CREATE SCHEMA sales", []).unwrap();
        conn.execute("CREATE SCHEMA inventory", []).unwrap();
        conn.execute("CREATE TABLE sales.orders (id INTEGER, total DOUBLE)", [])
            .unwrap();
        conn.execute(
            "CREATE TABLE inventory.products (id INTEGER, name VARCHAR)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO sales.orders VALUES (1, 99.99)", [])
            .unwrap();
        conn.execute("INSERT INTO inventory.products VALUES (1, 'Widget')", [])
            .unwrap();

        (
            temp_dir,
            Source::Duckdb {
                path: db_path.to_str().unwrap().to_string(),
            },
        )
    }

    /// Creates a DuckDB with nullable test data.
    /// Table has mix of nullable and non-nullable columns to verify issue #59 fix.
    pub fn duckdb_nullable_test() -> (TempDir, Source) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("source.duckdb");
        let conn = duckdb::Connection::open(&db_path).unwrap();

        conn.execute("CREATE SCHEMA testschema", []).unwrap();
        // c_custkey and c_name are NOT NULL, c_acctbal and c_comment are nullable
        conn.execute(
            "CREATE TABLE testschema.customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR NOT NULL,
                c_acctbal DOUBLE,
                c_comment VARCHAR
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO testschema.customer VALUES
                (1, 'Customer One', 1234.56, 'Regular customer'),
                (2, 'Customer Two', NULL, NULL),
                (3, 'Customer Three', 9999.99, 'VIP customer')",
            [],
        )
        .unwrap();

        (
            temp_dir,
            Source::Duckdb {
                path: db_path.to_str().unwrap().to_string(),
            },
        )
    }

    /// Creates a DuckDB with empty nullable test table.
    /// Verifies nullable flags are preserved even when table has no rows.
    pub fn duckdb_empty_nullable_test() -> (TempDir, Source) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("source.duckdb");
        let conn = duckdb::Connection::open(&db_path).unwrap();

        conn.execute("CREATE SCHEMA testschema", []).unwrap();
        // Same schema as nullable_test but with no data
        conn.execute(
            "CREATE TABLE testschema.empty_customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR NOT NULL,
                c_acctbal DOUBLE,
                c_comment VARCHAR
            )",
            [],
        )
        .unwrap();
        // No INSERT - table is intentionally empty

        (
            temp_dir,
            Source::Duckdb {
                path: db_path.to_str().unwrap().to_string(),
            },
        )
    }
}

mod postgres_fixtures {
    use super::*;
    use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    /// The password used for test Postgres containers.
    pub const TEST_PASSWORD: &str = "postgres";

    pub struct PostgresFixture {
        #[allow(dead_code)]
        pub container: ContainerAsync<Postgres>,
        pub source: Source,
    }

    async fn start_container() -> (ContainerAsync<Postgres>, String) {
        let container = Postgres::default()
            .with_tag("15-alpine")
            .start()
            .await
            .expect("Failed to start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let conn_str = format!(
            "postgres://postgres:{}@localhost:{}/postgres",
            TEST_PASSWORD, port
        );
        (container, conn_str)
    }

    pub async fn standard() -> PostgresFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::PgPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE SCHEMA sales")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE sales.orders (order_id INTEGER, customer_name VARCHAR(100), amount DOUBLE PRECISION, is_paid BOOLEAN)"
        ).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO sales.orders VALUES (1, 'Alice', 100.50, true), (2, 'Bob', 250.75, false), (3, 'Charlie', 75.25, true), (4, 'David', 500.00, true)"
        ).execute(&pool).await.unwrap();
        pool.close().await;

        let port = container.get_host_port_ipv4(5432).await.unwrap();
        PostgresFixture {
            container,
            source: Source::Postgres {
                host: "localhost".into(),
                port,
                user: "postgres".into(),
                database: "postgres".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }

    pub async fn multi_schema() -> PostgresFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::PgPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE SCHEMA sales")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE SCHEMA inventory")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE sales.orders (id INTEGER, total DOUBLE PRECISION)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE inventory.products (id INTEGER, name VARCHAR(100))")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO sales.orders VALUES (1, 99.99)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO inventory.products VALUES (1, 'Widget')")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let port = container.get_host_port_ipv4(5432).await.unwrap();
        PostgresFixture {
            container,
            source: Source::Postgres {
                host: "localhost".into(),
                port,
                user: "postgres".into(),
                database: "postgres".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }

    /// Creates a PostgreSQL with nullable test data.
    /// Table has mix of nullable and non-nullable columns to verify issue #59 fix.
    pub async fn nullable_test() -> PostgresFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::PgPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE SCHEMA testschema")
            .execute(&pool)
            .await
            .unwrap();
        // c_custkey and c_name are NOT NULL, c_acctbal and c_comment are nullable
        sqlx::query(
            "CREATE TABLE testschema.customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR(100) NOT NULL,
                c_acctbal DOUBLE PRECISION,
                c_comment VARCHAR(255)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO testschema.customer VALUES
                (1, 'Customer One', 1234.56, 'Regular customer'),
                (2, 'Customer Two', NULL, NULL),
                (3, 'Customer Three', 9999.99, 'VIP customer')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let port = container.get_host_port_ipv4(5432).await.unwrap();
        PostgresFixture {
            container,
            source: Source::Postgres {
                host: "localhost".into(),
                port,
                user: "postgres".into(),
                database: "postgres".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }

    /// Creates a PostgreSQL with empty nullable test table.
    /// Verifies nullable flags are preserved even when table has no rows.
    pub async fn empty_nullable_test() -> PostgresFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::PgPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE SCHEMA testschema")
            .execute(&pool)
            .await
            .unwrap();
        // Same schema as nullable_test but with no data
        sqlx::query(
            "CREATE TABLE testschema.empty_customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR(100) NOT NULL,
                c_acctbal DOUBLE PRECISION,
                c_comment VARCHAR(255)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        // No INSERT - table is intentionally empty
        pool.close().await;

        let port = container.get_host_port_ipv4(5432).await.unwrap();
        PostgresFixture {
            container,
            source: Source::Postgres {
                host: "localhost".into(),
                port,
                user: "postgres".into(),
                database: "postgres".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }
}

mod mysql_fixtures {
    use super::*;
    use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
    use testcontainers_modules::mysql::Mysql;

    /// The password used for test MySQL containers.
    pub const TEST_PASSWORD: &str = "root";

    pub struct MysqlFixture {
        #[allow(dead_code)]
        pub container: ContainerAsync<Mysql>,
        pub source: Source,
    }

    async fn start_container() -> (ContainerAsync<Mysql>, String) {
        let container = Mysql::default()
            .with_tag("8.0")
            .with_env_var("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
            .start()
            .await
            .expect("Failed to start mysql");
        let port = container.get_host_port_ipv4(3306).await.unwrap();
        let conn_str = format!("mysql://root:{}@localhost:{}/mysql", TEST_PASSWORD, port);
        (container, conn_str)
    }

    pub async fn standard() -> MysqlFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        // Create database and schema (MySQL uses database = schema)
        sqlx::query("CREATE DATABASE IF NOT EXISTS sales")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE sales.orders (order_id INTEGER, customer_name VARCHAR(100), amount DOUBLE, is_paid BOOLEAN)"
        ).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO sales.orders VALUES (1, 'Alice', 100.50, true), (2, 'Bob', 250.75, false), (3, 'Charlie', 75.25, true), (4, 'David', 500.00, true)"
        ).execute(&pool).await.unwrap();
        pool.close().await;

        let port = container.get_host_port_ipv4(3306).await.unwrap();
        MysqlFixture {
            container,
            source: Source::Mysql {
                host: "localhost".into(),
                port,
                user: "root".into(),
                database: "sales".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }

    /// Creates a MySQL with nullable test data.
    /// Table has mix of nullable and non-nullable columns to verify issue #59 fix.
    pub async fn nullable_test() -> MysqlFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        // Create database (MySQL uses database = schema)
        sqlx::query("CREATE DATABASE IF NOT EXISTS testschema")
            .execute(&pool)
            .await
            .unwrap();
        // c_custkey and c_name are NOT NULL, c_acctbal and c_comment are nullable
        sqlx::query(
            "CREATE TABLE testschema.customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR(100) NOT NULL,
                c_acctbal DOUBLE,
                c_comment VARCHAR(255)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO testschema.customer VALUES
                (1, 'Customer One', 1234.56, 'Regular customer'),
                (2, 'Customer Two', NULL, NULL),
                (3, 'Customer Three', 9999.99, 'VIP customer')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let port = container.get_host_port_ipv4(3306).await.unwrap();
        MysqlFixture {
            container,
            source: Source::Mysql {
                host: "localhost".into(),
                port,
                user: "root".into(),
                database: "testschema".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }

    /// Creates a MySQL with empty nullable test table.
    /// Verifies nullable flags are preserved even when table has no rows.
    pub async fn empty_nullable_test() -> MysqlFixture {
        let (container, conn_str) = start_container().await;
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        // Create database (MySQL uses database = schema)
        sqlx::query("CREATE DATABASE IF NOT EXISTS testschema")
            .execute(&pool)
            .await
            .unwrap();
        // Same schema as nullable_test but with no data
        sqlx::query(
            "CREATE TABLE testschema.empty_customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR(100) NOT NULL,
                c_acctbal DOUBLE,
                c_comment VARCHAR(255)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        // No INSERT - table is intentionally empty
        pool.close().await;

        let port = container.get_host_port_ipv4(3306).await.unwrap();
        MysqlFixture {
            container,
            source: Source::Mysql {
                host: "localhost".into(),
                port,
                user: "root".into(),
                database: "testschema".into(),
                auth: runtimedb::source::AuthType::Password,
            },
        }
    }
}

// ============================================================================
// Tests - DuckDB
// ============================================================================

mod duckdb_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_golden_path() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_golden_path_test(harness.engine(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_golden_path_test(harness.api(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new().await;
        run_multi_schema_test(harness.engine(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new().await;
        run_multi_schema_test(harness.api(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_delete_connection() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_delete_connection_test(harness.engine(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_delete_connection() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_delete_connection_test(harness.api(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_purge_connection_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_connection_cache_test(harness.engine(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_purge_connection_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_connection_cache_test(harness.api(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_purge_table_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_table_cache_test(harness.engine(), &source, "duckdb_conn", None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_purge_table_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_table_cache_test(harness.api(), &source, "duckdb_conn", None).await;
    }

    /// Test that nullable flags are preserved through sync (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_nullable_flags() {
        let (_dir, source) = fixtures::duckdb_nullable_test();
        let harness = TestHarness::new().await;
        run_nullable_flags_test(harness.engine(), &source, "duckdb_conn", "testschema", None).await;
    }

    /// Test that nullable flags are preserved through sync via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_nullable_flags() {
        let (_dir, source) = fixtures::duckdb_nullable_test();
        let harness = TestHarness::new().await;
        run_nullable_flags_test(harness.api(), &source, "duckdb_conn", "testschema", None).await;
    }

    /// Test that nullable flags are preserved for empty tables (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_empty_table_nullable_flags() {
        let (_dir, source) = fixtures::duckdb_empty_nullable_test();
        let harness = TestHarness::new().await;
        run_empty_nullable_flags_test(harness.engine(), &source, "duckdb_conn", "testschema", None)
            .await;
    }

    /// Test that nullable flags are preserved for empty tables via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_empty_table_nullable_flags() {
        let (_dir, source) = fixtures::duckdb_empty_nullable_test();
        let harness = TestHarness::new().await;
        run_empty_nullable_flags_test(harness.api(), &source, "duckdb_conn", "testschema", None)
            .await;
    }
}

// ============================================================================
// Tests - PostgreSQL
// ============================================================================

mod postgres_tests {
    use super::*;

    const PG_SECRET_NAME: &str = "pg-test-password";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_golden_path() {
        let harness = TestHarness::new().await;
        // Store the password as a secret before creating the fixture
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::standard().await;
        run_golden_path_test(
            harness.engine(),
            &fixture.source,
            "pg_conn",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::standard().await;
        run_golden_path_test(
            harness.api(),
            &fixture.source,
            "pg_conn",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_multi_schema() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::multi_schema().await;
        run_multi_schema_test(
            harness.engine(),
            &fixture.source,
            "pg_conn",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multi_schema() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::multi_schema().await;
        run_multi_schema_test(
            harness.api(),
            &fixture.source,
            "pg_conn",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved through sync (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::nullable_test().await;
        run_nullable_flags_test(
            harness.engine(),
            &fixture.source,
            "pg_conn",
            "testschema",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved through sync via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::nullable_test().await;
        run_nullable_flags_test(
            harness.api(),
            &fixture.source,
            "pg_conn",
            "testschema",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved for empty tables (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_empty_table_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::empty_nullable_test().await;
        run_empty_nullable_flags_test(
            harness.engine(),
            &fixture.source,
            "pg_conn",
            "testschema",
            Some(PG_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved for empty tables via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_empty_table_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::empty_nullable_test().await;
        run_empty_nullable_flags_test(
            harness.api(),
            &fixture.source,
            "pg_conn",
            "testschema",
            Some(PG_SECRET_NAME),
        )
        .await;
    }
}

// ============================================================================
// Tests - MySQL
// ============================================================================

mod mysql_tests {
    use super::*;

    const MYSQL_SECRET_NAME: &str = "mysql-test-password";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_golden_path() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::standard().await;
        run_golden_path_test(
            harness.engine(),
            &fixture.source,
            "mysql_conn",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::standard().await;
        run_golden_path_test(
            harness.api(),
            &fixture.source,
            "mysql_conn",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved through sync (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::nullable_test().await;
        run_nullable_flags_test(
            harness.engine(),
            &fixture.source,
            "mysql_conn",
            "testschema",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved through sync via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::nullable_test().await;
        run_nullable_flags_test(
            harness.api(),
            &fixture.source,
            "mysql_conn",
            "testschema",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved for empty tables (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_empty_table_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::empty_nullable_test().await;
        run_empty_nullable_flags_test(
            harness.engine(),
            &fixture.source,
            "mysql_conn",
            "testschema",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }

    /// Test that nullable flags are preserved for empty tables via API (issue #59).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_empty_table_nullable_flags() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::empty_nullable_test().await;
        run_empty_nullable_flags_test(
            harness.api(),
            &fixture.source,
            "mysql_conn",
            "testschema",
            Some(MYSQL_SECRET_NAME),
        )
        .await;
    }
}

// ============================================================================
// Tests - Error Cases
// ============================================================================

mod error_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_nonexistent_connection() {
        let harness = TestHarness::new().await;

        // Engine
        let conn = harness.engine().get_connection("nonexistent").await;
        assert!(conn.is_none());

        // API
        let conn = harness.api().get_connection("nonexistent").await;
        assert!(conn.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_nonexistent_connection() {
        let harness = TestHarness::new().await;

        let deleted = harness.api().delete_connection("nonexistent").await;
        assert!(!deleted, "Delete of nonexistent should return false/404");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_purge_nonexistent_connection_cache() {
        let harness = TestHarness::new().await;

        let purged = harness.api().purge_connection_cache("nonexistent").await;
        assert!(!purged, "Purge of nonexistent should return false/404");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_purge_nonexistent_table_cache() {
        let harness = TestHarness::new().await;

        let purged = harness
            .api()
            .purge_table_cache("nonexistent", "schema", "table")
            .await;
        assert!(
            !purged,
            "Purge of nonexistent table should return false/404"
        );
    }
}

// ============================================================================
// Tests - Result Persistence
// ============================================================================

mod result_persistence_tests {
    use super::*;

    /// Test the full flow: query -> get result_id -> retrieve result -> list results
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_result_persistence_flow() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        let api = harness.api();

        // Connect
        let _conn_id = api.connect("test_conn", &source, None).await;

        // Execute query and verify we get a result_id
        let query_result = api.query(&queries::select_orders("test_conn")).await;
        query_result.assert_row_count(4);
        query_result.assert_has_result_id();

        let result_id = query_result.get_result_id().unwrap();

        // Retrieve the result by ID
        let retrieved = api.get_result(result_id).await;
        assert!(
            retrieved.is_some(),
            "Should be able to retrieve result by ID"
        );

        let retrieved = retrieved.unwrap();
        retrieved.assert_row_count(4);
        retrieved.assert_columns(&["order_id", "customer_name", "amount"]);

        // Verify the result appears in the list
        let results_list = api.list_results().await;
        results_list.assert_count(1);
        results_list.assert_contains(result_id);
    }

    /// Test that multiple queries create separate results
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multiple_queries_create_separate_results() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        let api = harness.api();

        // Connect
        let _conn_id = api.connect("test_conn", &source, None).await;

        // Execute first query
        let result1 = api.query(&queries::select_orders("test_conn")).await;
        result1.assert_has_result_id();
        let id1 = result1.get_result_id().unwrap().to_string();

        // Execute second query (aggregation)
        let result2 = api.query(&queries::count_paid_orders("test_conn")).await;
        result2.assert_has_result_id();
        let id2 = result2.get_result_id().unwrap().to_string();

        // IDs should be different
        assert_ne!(id1, id2, "Each query should get a unique result_id");

        // Both should be in the list
        let results_list = api.list_results().await;
        results_list.assert_count(2);
        results_list.assert_contains(&id1);
        results_list.assert_contains(&id2);

        // Both should be retrievable
        let retrieved1 = api.get_result(&id1).await;
        assert!(retrieved1.is_some());
        retrieved1.unwrap().assert_row_count(4);

        let retrieved2 = api.get_result(&id2).await;
        assert!(retrieved2.is_some());
        retrieved2.unwrap().assert_row_count(1);
    }

    /// Test that non-existent result returns None
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_get_nonexistent_result() {
        let harness = TestHarness::new().await;

        let result = harness.api().get_result("rslt_nonexistent_id_12345").await;
        assert!(result.is_none(), "Non-existent result should return None");
    }
}
