use runtimedb::RuntimeEngine;
use tempfile::TempDir;

/// Generate a test secret key (base64-encoded 32 bytes)
fn test_secret_key() -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Create a test engine with a DuckDB connection for testing
async fn create_test_engine_with_data(temp_dir: &TempDir) -> RuntimeEngine {
    use runtimedb::source::Source;

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path().to_path_buf())
        .secret_key(test_secret_key())
        .build()
        .await
        .expect("Failed to create engine");

    // Create a DuckDB source with some test data
    let duckdb_path = temp_dir.path().join("test.duckdb");
    let db = duckdb::Connection::open(&duckdb_path).expect("Failed to create DuckDB");

    db.execute_batch(
        r#"
        CREATE SCHEMA sales;
        CREATE TABLE sales.orders (
            id INTEGER PRIMARY KEY,
            customer_name VARCHAR NOT NULL,
            amount DECIMAL(10, 2),
            created_at TIMESTAMP
        );
        INSERT INTO sales.orders VALUES (1, 'Alice', 100.50, '2024-01-01');

        CREATE TABLE sales.products (
            sku VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            price DECIMAL(10, 2)
        );
        "#,
    )
    .expect("Failed to create test tables");

    drop(db);

    // Register the DuckDB connection
    let source = Source::Duckdb {
        path: duckdb_path.to_str().unwrap().to_string(),
    };
    engine
        .connect("testdb", source, None)
        .await
        .expect("Failed to connect");

    engine
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_tables() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let engine = create_test_engine_with_data(&temp_dir).await;

    // Query information_schema.tables
    let result = engine
        .execute_query("SELECT * FROM runtimedb.information_schema.tables ORDER BY table_name")
        .await
        .expect("Query failed");

    assert!(!result.results.is_empty(), "Should have results");

    let batch = &result.results[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 tables");

    // Check column names
    let schema = batch.schema();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        column_names,
        vec!["table_catalog", "table_schema", "table_name", "table_type"]
    );

    // Check table names (should be orders and products)
    let table_names = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("table_name should be string");
    assert_eq!(table_names.value(0), "orders");
    assert_eq!(table_names.value(1), "products");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_columns() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let engine = create_test_engine_with_data(&temp_dir).await;

    // Query information_schema.columns for orders table
    let result = engine
        .execute_query(
            "SELECT column_name, data_type, is_nullable
             FROM runtimedb.information_schema.columns
             WHERE table_name = 'orders'
             ORDER BY ordinal_position",
        )
        .await
        .expect("Query failed");

    assert!(!result.results.is_empty(), "Should have results");

    let batch = &result.results[0];
    assert_eq!(batch.num_rows(), 4, "orders table should have 4 columns");

    // Check column names
    let column_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("column_name should be string");
    assert_eq!(column_names.value(0), "id");
    assert_eq!(column_names.value(1), "customer_name");
    assert_eq!(column_names.value(2), "amount");
    assert_eq!(column_names.value(3), "created_at");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_columns_all_tables() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let engine = create_test_engine_with_data(&temp_dir).await;

    // Query all columns across all tables
    let result = engine
        .execute_query("SELECT COUNT(*) as col_count FROM runtimedb.information_schema.columns")
        .await
        .expect("Query failed");

    assert!(!result.results.is_empty(), "Should have results");

    let batch = &result.results[0];
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count should be int64");

    // orders has 4 columns, products has 3 columns = 7 total
    assert_eq!(count.value(0), 7, "Should have 7 columns total");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema_empty_catalog() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Create engine without any connections
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path().to_path_buf())
        .secret_key(test_secret_key())
        .build()
        .await
        .expect("Failed to create engine");

    // Query should return empty results
    let result = engine
        .execute_query("SELECT * FROM runtimedb.information_schema.tables")
        .await
        .expect("Query failed");

    assert!(!result.results.is_empty(), "Should have result batch");
    assert_eq!(result.results[0].num_rows(), 0, "Should have 0 tables");
}
