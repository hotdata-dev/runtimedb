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
use rivetdb::datafusion::HotDataEngine;
use rivetdb::http::app_server::{AppServer, PATH_CONNECTIONS, PATH_QUERY, PATH_TABLES};
use rivetdb::source::Source;
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
    fn from_engine(response: &rivetdb::datafusion::QueryResponse) -> Self {
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
    tables: Vec<(String, String)>, // (schema, table)
}

impl TablesResult {
    fn from_engine(tables: &[rivetdb::catalog::TableInfo]) -> Self {
        Self {
            tables: tables
                .iter()
                .map(|t| (t.schema_name.clone(), t.table_name.clone()))
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
                        Some((schema.to_string(), table.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self { tables }
    }

    fn assert_has_table(&self, schema: &str, table: &str) {
        assert!(
            self.tables.iter().any(|(s, t)| s == schema && t == table),
            "Table '{}.{}' not found. Available: {:?}",
            schema,
            table,
            self.tables
        );
    }

    fn assert_not_empty(&self) {
        assert!(!self.tables.is_empty(), "Expected tables but found none");
    }
}

/// Trait for test execution - allows same test logic for engine vs API.
#[async_trait::async_trait]
trait TestExecutor: Send + Sync {
    async fn connect(&self, name: &str, source: &Source);
    async fn list_connections(&self) -> ConnectionResult;
    async fn list_tables(&self, connection: &str) -> TablesResult;
    async fn query(&self, sql: &str) -> QueryResult;
}

/// Engine-based test executor.
struct EngineExecutor {
    engine: Arc<HotDataEngine>,
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
        ConnectionResult::from_engine(&self.engine.list_connections().unwrap())
    }

    async fn list_tables(&self, connection: &str) -> TablesResult {
        TablesResult::from_engine(&self.engine.list_tables(Some(connection)).unwrap())
    }

    async fn query(&self, sql: &str) -> QueryResult {
        QueryResult::from_engine(&self.engine.execute_query(sql).await.unwrap())
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
                password,
                database,
            } => (
                "postgres",
                json!({ "host": host, "port": port, "user": user, "password": password, "database": database }),
            ),
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
}

/// Test context providing both executors.
struct TestHarness {
    engine_executor: EngineExecutor,
    api_executor: ApiExecutor,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestHarness {
    fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("catalog.db");
        let cache_dir = temp_dir.path().join("cache");
        let state_dir = temp_dir.path().join("state");

        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&state_dir).unwrap();

        let engine = HotDataEngine::new_with_paths(
            catalog_path.to_str().unwrap(),
            cache_dir.to_str().unwrap(),
            state_dir.to_str().unwrap(),
            false,
        )
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
        let conn_str = format!("postgres://postgres:postgres@localhost:{}/postgres", port);
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
                password: "postgres".into(),
                database: "postgres".into(),
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
                password: "postgres".into(),
                database: "postgres".into(),
            },
        }
    }
}

// ============================================================================
// Tests - DuckDB
// ============================================================================

mod duckdb_tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_golden_path() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new();
        run_golden_path_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test]
    async fn test_api_golden_path() {
        let (_dir, source) = fixtures::duckdb_standard();
        let harness = TestHarness::new();
        run_golden_path_test(harness.api(), &source, "duckdb_conn").await;
    }

    #[tokio::test]
    async fn test_engine_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new();
        run_multi_schema_test(harness.engine(), &source, "duckdb_conn").await;
    }

    #[tokio::test]
    async fn test_api_multi_schema() {
        let (_dir, source) = fixtures::duckdb_multi_schema();
        let harness = TestHarness::new();
        run_multi_schema_test(harness.api(), &source, "duckdb_conn").await;
    }
}

// ============================================================================
// Tests - PostgreSQL
// ============================================================================

mod postgres_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_golden_path() {
        let fixture = postgres_fixtures::standard().await;
        let harness = TestHarness::new();
        run_golden_path_test(harness.engine(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let fixture = postgres_fixtures::standard().await;
        let harness = TestHarness::new();
        run_golden_path_test(harness.api(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_multi_schema() {
        let fixture = postgres_fixtures::multi_schema().await;
        let harness = TestHarness::new();
        run_multi_schema_test(harness.engine(), &fixture.source, "pg_conn").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_multi_schema() {
        let fixture = postgres_fixtures::multi_schema().await;
        let harness = TestHarness::new();
        run_multi_schema_test(harness.api(), &fixture.source, "pg_conn").await;
    }
}
