//! Golden path integration tests for RivetDB.
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
use rivetdb::http::app_server::{AppServer, PATH_CONNECTIONS, PATH_QUERY, PATH_TABLES};
use rivetdb::source::Source;
use rivetdb::RivetEngine;
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
}

impl QueryResult {
    fn from_engine(response: &rivetdb::QueryResponse) -> Self {
        let batch = response.results.first();

        let (columns, row_count) = match batch {
            Some(b) => {
                let cols: Vec<String> = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                (cols, b.num_rows())
            }
            None => (vec![], 0),
        };

        Self { row_count, columns }
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

        let row_count = json["row_count"].as_u64().unwrap_or(0) as usize;

        Self { row_count, columns }
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
}

/// Connection info from list operations.
struct ConnectionResult {
    count: usize,
    names: Vec<String>,
}

impl ConnectionResult {
    fn from_engine(connections: &[rivetdb::catalog::ConnectionInfo]) -> Self {
        Self {
            count: connections.len(),
            names: connections.iter().map(|c| c.name.clone()).collect(),
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
}

/// Table info from list operations.
struct TablesResult {
    tables: Vec<(String, String, bool)>, // (schema, table, synced)
}

impl TablesResult {
    fn from_engine(tables: &[rivetdb::catalog::TableInfo]) -> Self {
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

/// Single connection details from get operations.
#[allow(dead_code)]
struct ConnectionDetails {
    id: i32,
    name: String,
    source_type: String,
    table_count: usize,
    synced_table_count: usize,
}

impl ConnectionDetails {
    fn from_engine(
        conn: &rivetdb::catalog::ConnectionInfo,
        tables: &[rivetdb::catalog::TableInfo],
    ) -> Self {
        Self {
            id: conn.id,
            name: conn.name.clone(),
            source_type: conn.source_type.clone(),
            table_count: tables.len(),
            synced_table_count: tables.iter().filter(|t| t.parquet_path.is_some()).count(),
        }
    }

    fn from_api(json: &serde_json::Value) -> Self {
        Self {
            id: json["id"].as_i64().unwrap_or(0) as i32,
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
    async fn connect(&self, name: &str, source: &Source);
    async fn list_connections(&self) -> ConnectionResult;
    async fn list_tables(&self, connection: &str) -> TablesResult;
    async fn query(&self, sql: &str) -> QueryResult;

    // New methods for CRUD operations
    async fn get_connection(&self, name: &str) -> Option<ConnectionDetails>;
    async fn delete_connection(&self, name: &str) -> bool;
    async fn purge_connection_cache(&self, name: &str) -> bool;
    async fn purge_table_cache(&self, conn: &str, schema: &str, table: &str) -> bool;
}

/// Engine-based test executor.
struct EngineExecutor {
    engine: Arc<RivetEngine>,
}

#[async_trait::async_trait]
impl TestExecutor for EngineExecutor {
    async fn connect(&self, name: &str, source: &Source) {
        self.engine
            .connect(name, source.clone())
            .await
            .expect("Engine connect failed");
    }

    async fn list_connections(&self) -> ConnectionResult {
        ConnectionResult::from_engine(&self.engine.list_connections().await.unwrap())
    }

    async fn list_tables(&self, connection: &str) -> TablesResult {
        TablesResult::from_engine(&self.engine.list_tables(Some(connection)).await.unwrap())
    }

    async fn query(&self, sql: &str) -> QueryResult {
        QueryResult::from_engine(&self.engine.execute_query(sql).await.unwrap())
    }

    async fn get_connection(&self, name: &str) -> Option<ConnectionDetails> {
        let conn = self.engine.catalog().get_connection(name).await.ok()??;
        let tables = self.engine.list_tables(Some(name)).await.ok()?;
        Some(ConnectionDetails::from_engine(&conn, &tables))
    }

    async fn delete_connection(&self, name: &str) -> bool {
        self.engine.remove_connection(name).await.is_ok()
    }

    async fn purge_connection_cache(&self, name: &str) -> bool {
        self.engine.purge_connection(name).await.is_ok()
    }

    async fn purge_table_cache(&self, conn: &str, schema: &str, table: &str) -> bool {
        self.engine.purge_table(conn, schema, table).await.is_ok()
    }
}

/// REST API-based test executor.
struct ApiExecutor {
    router: Router,
}

#[async_trait::async_trait]
impl TestExecutor for ApiExecutor {
    async fn connect(&self, name: &str, source: &Source) {
        let (source_type, config) = match source {
            Source::Duckdb { path } => ("duckdb", json!({ "path": path })),
            Source::Postgres {
                host,
                port,
                user,
                database,
                credential,
            } => {
                let cred_json = match credential {
                    rivetdb::source::Credential::None => json!({"type": "none"}),
                    rivetdb::source::Credential::SecretRef { name } => {
                        json!({"type": "secret_ref", "name": name})
                    }
                };
                (
                    "postgres",
                    json!({ "host": host, "port": port, "user": user, "database": database, "credential": cred_json }),
                )
            }
            Source::Mysql {
                host,
                port,
                user,
                database,
                credential,
            } => {
                let cred_json = match credential {
                    rivetdb::source::Credential::None => json!({"type": "none"}),
                    rivetdb::source::Credential::SecretRef { name } => {
                        json!({"type": "secret_ref", "name": name})
                    }
                };
                (
                    "mysql",
                    json!({ "host": host, "port": port, "user": user, "database": database, "credential": cred_json }),
                )
            }
            _ => panic!("Unsupported source type"),
        };

        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_CONNECTIONS)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&json!({
                            "name": name,
                            "source_type": source_type,
                            "config": config,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED, "API connect failed");
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

    async fn list_tables(&self, connection: &str) -> TablesResult {
        let uri = format!("{}?connection={}", PATH_TABLES, connection);
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

    async fn get_connection(&self, name: &str) -> Option<ConnectionDetails> {
        let uri = format!("/connections/{}", name);
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

    async fn delete_connection(&self, name: &str) -> bool {
        let uri = format!("/connections/{}", name);
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

    async fn purge_connection_cache(&self, name: &str) -> bool {
        let uri = format!("/connections/{}/cache", name);
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

    async fn purge_table_cache(&self, conn: &str, schema: &str, table: &str) -> bool {
        let uri = format!("/connections/{}/tables/{}/{}/cache", conn, schema, table);
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

        let engine = RivetEngine::builder()
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
}

/// Run the golden path test scenario against any executor and source.
async fn run_golden_path_test(executor: &dyn TestExecutor, source: &Source, conn_name: &str) {
    // Connect
    executor.connect(conn_name, source).await;

    // Verify connection exists
    let connections = executor.list_connections().await;
    connections.assert_count(1);
    connections.assert_has_connection(conn_name);

    // Verify tables discovered
    let tables = executor.list_tables(conn_name).await;
    tables.assert_not_empty();
    tables.assert_has_table("sales", "orders");

    // Query and verify
    let result = executor.query(&queries::select_orders(conn_name)).await;
    result.assert_row_count(4);
    result.assert_columns(&["order_id", "customer_name", "amount"]);

    // Aggregation
    let agg = executor.query(&queries::count_paid_orders(conn_name)).await;
    agg.assert_row_count(1);
}

/// Run multi-schema test scenario.
async fn run_multi_schema_test(executor: &dyn TestExecutor, source: &Source, conn_name: &str) {
    executor.connect(conn_name, source).await;

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
async fn run_delete_connection_test(executor: &dyn TestExecutor, source: &Source, conn_name: &str) {
    // Create connection
    executor.connect(conn_name, source).await;

    // Verify it exists
    let conn = executor.get_connection(conn_name).await;
    assert!(conn.is_some(), "Connection should exist after creation");

    // Delete it
    let deleted = executor.delete_connection(conn_name).await;
    assert!(deleted, "Delete should succeed");

    // Verify it's gone
    let conn = executor.get_connection(conn_name).await;
    assert!(conn.is_none(), "Connection should not exist after deletion");

    // Verify delete of non-existent returns false
    let deleted_again = executor.delete_connection(conn_name).await;
    assert!(!deleted_again, "Delete of non-existent should fail");
}

/// Run purge connection cache test scenario.
async fn run_purge_connection_cache_test(
    executor: &dyn TestExecutor,
    source: &Source,
    conn_name: &str,
) {
    // Create connection
    executor.connect(conn_name, source).await;

    // Query to trigger sync
    let _ = executor.query(&queries::select_orders(conn_name)).await;

    // Verify tables are synced
    let tables = executor.list_tables(conn_name).await;
    assert!(
        tables.synced_count() > 0,
        "Should have synced tables after query"
    );

    // Purge cache
    let purged = executor.purge_connection_cache(conn_name).await;
    assert!(purged, "Purge should succeed");

    // Verify tables still exist but not synced
    let tables = executor.list_tables(conn_name).await;
    tables.assert_not_empty();
    tables.assert_none_synced();

    // Connection should still exist
    let conn = executor.get_connection(conn_name).await;
    assert!(
        conn.is_some(),
        "Connection should still exist after cache purge"
    );
}

/// Run purge table cache test scenario.
async fn run_purge_table_cache_test(executor: &dyn TestExecutor, source: &Source, conn_name: &str) {
    // Create connection
    executor.connect(conn_name, source).await;

    // Query to trigger sync of orders table
    let _ = executor.query(&queries::select_orders(conn_name)).await;

    // Verify orders table is synced
    let tables = executor.list_tables(conn_name).await;
    assert!(
        tables.is_table_synced("sales", "orders"),
        "orders should be synced"
    );

    // Purge just the orders table cache
    let purged = executor
        .purge_table_cache(conn_name, "sales", "orders")
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

    pub async fn standard(secret_name: &str) -> PostgresFixture {
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
                credential: rivetdb::source::Credential::SecretRef {
                    name: secret_name.to_string(),
                },
            },
        }
    }

    pub async fn multi_schema(secret_name: &str) -> PostgresFixture {
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
                credential: rivetdb::source::Credential::SecretRef {
                    name: secret_name.to_string(),
                },
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

    pub async fn standard(secret_name: &str) -> MysqlFixture {
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
                credential: rivetdb::source::Credential::SecretRef {
                    name: secret_name.to_string(),
                },
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
        run_golden_path_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_golden_path_test(harness.api(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new().await;
        run_multi_schema_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new().await;
        run_multi_schema_test(harness.api(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_delete_connection() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_delete_connection_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_delete_connection() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_delete_connection_test(harness.api(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_purge_connection_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_connection_cache_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_purge_connection_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_connection_cache_test(harness.api(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_purge_table_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_table_cache_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_purge_table_cache() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new().await;
        run_purge_table_cache_test(harness.api(), &source, "duckdb_conn").await;
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
        let fixture = postgres_fixtures::standard(PG_SECRET_NAME).await;
        run_golden_path_test(harness.engine(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::standard(PG_SECRET_NAME).await;
        run_golden_path_test(harness.api(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_multi_schema() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::multi_schema(PG_SECRET_NAME).await;
        run_multi_schema_test(harness.engine(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multi_schema() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(PG_SECRET_NAME, postgres_fixtures::TEST_PASSWORD)
            .await;
        let fixture = postgres_fixtures::multi_schema(PG_SECRET_NAME).await;
        run_multi_schema_test(harness.api(), &fixture.source, "pg_conn").await;
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
        let fixture = mysql_fixtures::standard(MYSQL_SECRET_NAME).await;
        run_golden_path_test(harness.engine(), &fixture.source, "mysql_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let harness = TestHarness::new().await;
        harness
            .store_secret(MYSQL_SECRET_NAME, mysql_fixtures::TEST_PASSWORD)
            .await;
        let fixture = mysql_fixtures::standard(MYSQL_SECRET_NAME).await;
        run_golden_path_test(harness.api(), &fixture.source, "mysql_conn").await;
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
