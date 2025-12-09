//! Golden path integration tests for RivetDB.
//!
//! These tests verify the complete workflow: create connection, discover tables, query data.
//! Tests cover both DuckDB (local) and PostgreSQL (via testcontainers).
//! Each source type is tested via both the engine API and the REST API.

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
// Shared Test Infrastructure
// ============================================================================

/// Test context that provides both engine and API access.
struct TestContext {
    engine: Arc<HotDataEngine>,
    router: Router,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestContext {
    /// Create a new test context with a fresh engine.
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
        .expect("Failed to create test engine");

        let app = AppServer::new(engine);

        Self {
            engine: app.engine,
            router: app.router,
            temp_dir,
        }
    }

    /// Connect to a source via the engine directly.
    async fn connect_engine(&self, name: &str, source: Source) {
        self.engine
            .connect(name, source)
            .await
            .expect("Failed to connect via engine");
    }

    /// Connect to a source via the REST API.
    async fn connect_api(&self, name: &str, source: &Source) -> serde_json::Value {
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
                json!({
                    "host": host,
                    "port": port,
                    "user": user,
                    "password": password,
                    "database": database,
                }),
            ),
            _ => panic!("Unsupported source type for API test"),
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

        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "Connection creation should succeed"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// Query via the engine directly.
    async fn query_engine(&self, sql: &str) -> rivetdb::datafusion::QueryResponse {
        self.engine
            .execute_query(sql)
            .await
            .expect("Query should succeed")
    }

    /// Query via the REST API.
    async fn query_api(&self, sql: &str) -> serde_json::Value {
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

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Query should succeed: {}",
            sql
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// List connections via the REST API.
    async fn list_connections_api(&self) -> serde_json::Value {
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

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// List tables via the REST API.
    async fn list_tables_api(&self, connection: Option<&str>) -> serde_json::Value {
        let uri = match connection {
            Some(conn) => format!("{}?connection={}", PATH_TABLES, conn),
            None => PATH_TABLES.to_string(),
        };

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

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }
}

// ============================================================================
// DuckDB Tests
// ============================================================================

mod duckdb_tests {
    use super::*;

    /// Create a DuckDB source with test data.
    fn create_duckdb_source() -> (TempDir, Source) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("source.duckdb");

        let conn = duckdb::Connection::open(&db_path).unwrap();

        conn.execute("CREATE SCHEMA sales", []).unwrap();
        conn.execute(
            "CREATE TABLE sales.orders (
                order_id INTEGER,
                customer_name VARCHAR,
                amount DOUBLE,
                is_paid BOOLEAN
            )",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO sales.orders VALUES
                (1, 'Alice', 100.50, true),
                (2, 'Bob', 250.75, false),
                (3, 'Charlie', 75.25, true),
                (4, 'David', 500.00, true)",
            [],
        )
        .unwrap();

        let source = Source::Duckdb {
            path: db_path.to_str().unwrap().to_string(),
        };

        (temp_dir, source)
    }

    #[tokio::test]
    async fn test_engine_golden_path() {
        let (_source_dir, source) = create_duckdb_source();
        let ctx = TestContext::new();

        // Connect via engine
        ctx.connect_engine("local_duckdb", source).await;

        // Verify connection
        let connections = ctx.engine.list_connections().unwrap();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].name, "local_duckdb");

        // Verify tables were discovered
        let tables = ctx.engine.list_tables(Some("local_duckdb")).unwrap();
        assert!(tables.iter().any(|t| t.table_name == "orders"));

        // Query and verify results
        let result = ctx
            .query_engine(
                "SELECT order_id, customer_name, amount FROM local_duckdb.sales.orders ORDER BY order_id",
            )
            .await;
        assert_eq!(result.results[0].num_rows(), 4);
        assert_eq!(result.results[0].num_columns(), 3);

        // Aggregation query
        let agg = ctx
            .query_engine(
                "SELECT COUNT(*) as cnt, SUM(amount) as total FROM local_duckdb.sales.orders WHERE is_paid = true",
            )
            .await;
        assert_eq!(agg.results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_api_golden_path() {
        let (_source_dir, source) = create_duckdb_source();
        let ctx = TestContext::new();

        // Connect via API
        let conn_response = ctx.connect_api("local_duckdb", &source).await;
        assert_eq!(conn_response["name"], "local_duckdb");
        assert_eq!(conn_response["source_type"], "duckdb");
        assert!(conn_response["tables_discovered"].as_i64().unwrap() > 0);

        // List connections via API
        let connections = ctx.list_connections_api().await;
        assert_eq!(connections["connections"].as_array().unwrap().len(), 1);
        assert_eq!(connections["connections"][0]["name"], "local_duckdb");

        // List tables via API
        let tables = ctx.list_tables_api(Some("local_duckdb")).await;
        let table_list = tables["tables"].as_array().unwrap();
        assert!(table_list.iter().any(|t| t["table"] == "orders"));

        // Query via API
        let result = ctx
            .query_api(
                "SELECT order_id, customer_name, amount FROM local_duckdb.sales.orders ORDER BY order_id",
            )
            .await;
        assert_eq!(result["row_count"], 4);
        assert_eq!(result["columns"], json!(["order_id", "customer_name", "amount"]));
        assert_eq!(result["rows"][0][0], 1);
        assert_eq!(result["rows"][0][1], "Alice");

        // Aggregation via API
        let agg = ctx
            .query_api(
                "SELECT COUNT(*) as cnt, SUM(amount) as total FROM local_duckdb.sales.orders WHERE is_paid = true",
            )
            .await;
        assert_eq!(agg["row_count"], 1);
        assert_eq!(agg["rows"][0][0], 3); // 3 paid orders
    }
}

// ============================================================================
// PostgreSQL Tests (using testcontainers)
// ============================================================================

mod postgres_tests {
    use super::*;
    use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    /// PostgreSQL test fixture with container and source.
    struct PostgresFixture {
        #[allow(dead_code)]
        container: ContainerAsync<Postgres>,
        source: Source,
    }

    impl PostgresFixture {
        /// Create a Postgres container with test data.
        async fn new() -> Self {
            let container = Postgres::default()
                .with_tag("15-alpine")
                .start()
                .await
                .expect("Failed to start postgres container");

            let host_port = container.get_host_port_ipv4(5432).await.unwrap();
            let connection_string = format!(
                "postgres://postgres:postgres@localhost:{}/postgres",
                host_port
            );

            // Create test schema and data
            let pool = sqlx::PgPool::connect(&connection_string)
                .await
                .expect("Failed to connect to postgres");

            sqlx::query("CREATE SCHEMA IF NOT EXISTS sales")
                .execute(&pool)
                .await
                .unwrap();

            sqlx::query(
                "CREATE TABLE IF NOT EXISTS sales.orders (
                    order_id INTEGER,
                    customer_name VARCHAR(100),
                    amount DOUBLE PRECISION,
                    is_paid BOOLEAN
                )",
            )
            .execute(&pool)
            .await
            .unwrap();

            sqlx::query(
                "INSERT INTO sales.orders (order_id, customer_name, amount, is_paid) VALUES
                    (1, 'Alice', 100.50, true),
                    (2, 'Bob', 250.75, false),
                    (3, 'Charlie', 75.25, true),
                    (4, 'David', 500.00, true)",
            )
            .execute(&pool)
            .await
            .unwrap();

            pool.close().await;

            let source = Source::Postgres {
                host: "localhost".to_string(),
                port: host_port,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                database: "postgres".to_string(),
            };

            Self { container, source }
        }

        /// Create a Postgres container with multiple schemas.
        async fn new_multi_schema() -> Self {
            let container = Postgres::default()
                .with_tag("15-alpine")
                .start()
                .await
                .expect("Failed to start postgres container");

            let host_port = container.get_host_port_ipv4(5432).await.unwrap();
            let connection_string = format!(
                "postgres://postgres:postgres@localhost:{}/postgres",
                host_port
            );

            let pool = sqlx::PgPool::connect(&connection_string)
                .await
                .expect("Failed to connect to postgres");

            sqlx::query("CREATE SCHEMA IF NOT EXISTS sales")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("CREATE SCHEMA IF NOT EXISTS inventory")
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

            let source = Source::Postgres {
                host: "localhost".to_string(),
                port: host_port,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                database: "postgres".to_string(),
            };

            Self { container, source }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_engine_golden_path() {
        let fixture = PostgresFixture::new().await;
        let ctx = TestContext::new();

        // Connect via engine
        ctx.connect_engine("pg_source", fixture.source.clone()).await;

        // Verify connection
        let connections = ctx.engine.list_connections().unwrap();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].name, "pg_source");

        // Verify tables were discovered
        let tables = ctx.engine.list_tables(Some("pg_source")).unwrap();
        assert!(
            tables.iter().any(|t| t.table_name == "orders"),
            "Should find orders table. Found: {:?}",
            tables.iter().map(|t| &t.table_name).collect::<Vec<_>>()
        );

        // Query and verify results
        let result = ctx
            .query_engine(
                "SELECT order_id, customer_name, amount FROM pg_source.sales.orders ORDER BY order_id",
            )
            .await;
        assert_eq!(result.results[0].num_rows(), 4);
        assert_eq!(result.results[0].num_columns(), 3);

        // Aggregation query
        let agg = ctx
            .query_engine(
                "SELECT COUNT(*) as cnt, SUM(amount) as total FROM pg_source.sales.orders WHERE is_paid = true",
            )
            .await;
        assert_eq!(agg.results[0].num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_golden_path() {
        let fixture = PostgresFixture::new().await;
        let ctx = TestContext::new();

        // Connect via API
        let conn_response = ctx.connect_api("pg_source", &fixture.source).await;
        assert_eq!(conn_response["name"], "pg_source");
        assert_eq!(conn_response["source_type"], "postgres");
        assert!(conn_response["tables_discovered"].as_i64().unwrap() > 0);

        // List connections via API
        let connections = ctx.list_connections_api().await;
        assert_eq!(connections["connections"].as_array().unwrap().len(), 1);
        assert_eq!(connections["connections"][0]["name"], "pg_source");

        // List tables via API
        let tables = ctx.list_tables_api(Some("pg_source")).await;
        let table_list = tables["tables"].as_array().unwrap();
        assert!(
            table_list.iter().any(|t| t["table"] == "orders"),
            "Should find orders table. Found: {:?}",
            table_list
        );

        // Query via API
        let result = ctx
            .query_api(
                "SELECT order_id, customer_name, amount FROM pg_source.sales.orders ORDER BY order_id",
            )
            .await;
        assert_eq!(result["row_count"], 4);
        assert_eq!(result["columns"], json!(["order_id", "customer_name", "amount"]));
        assert_eq!(result["rows"][0][0], 1);
        assert_eq!(result["rows"][0][1], "Alice");

        // Aggregation via API
        let agg = ctx
            .query_api(
                "SELECT COUNT(*) as cnt, SUM(amount) as total FROM pg_source.sales.orders WHERE is_paid = true",
            )
            .await;
        assert_eq!(agg["row_count"], 1);
        assert_eq!(agg["rows"][0][0], 3); // 3 paid orders
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_schemas() {
        let fixture = PostgresFixture::new_multi_schema().await;
        let ctx = TestContext::new();

        // Connect via engine
        ctx.connect_engine("pg", fixture.source.clone()).await;

        // Verify both schemas discovered (via engine)
        let tables = ctx.engine.list_tables(Some("pg")).unwrap();
        let table_names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"products"));

        // Verify via API
        let tables_api = ctx.list_tables_api(Some("pg")).await;
        let table_list = tables_api["tables"].as_array().unwrap();
        assert!(table_list.iter().any(|t| t["table"] == "orders"));
        assert!(table_list.iter().any(|t| t["table"] == "products"));

        // Query from both schemas via engine
        let orders = ctx.query_engine("SELECT * FROM pg.sales.orders").await;
        assert_eq!(orders.results[0].num_rows(), 1);

        let products = ctx
            .query_engine("SELECT * FROM pg.inventory.products")
            .await;
        assert_eq!(products.results[0].num_rows(), 1);

        // Query from both schemas via API
        let orders_api = ctx.query_api("SELECT * FROM pg.sales.orders").await;
        assert_eq!(orders_api["row_count"], 1);

        let products_api = ctx.query_api("SELECT * FROM pg.inventory.products").await;
        assert_eq!(products_api["row_count"], 1);
    }
}