//! Integration tests for datafetch module

use rivetdb::datafetch::{DataFetcher, NativeFetcher};
use rivetdb::source::Source;

#[tokio::test]
async fn test_duckdb_discovery_empty() {
    let fetcher = NativeFetcher::new();
    let source = Source::Duckdb {
        path: ":memory:".to_string(),
    };

    let result = fetcher.discover_tables(&source).await;
    assert!(
        result.is_ok(),
        "Discovery should succeed: {:?}",
        result.err()
    );

    let tables = result.unwrap();
    assert!(tables.is_empty(), "Empty DuckDB should have no tables");
}

#[tokio::test]
async fn test_duckdb_discovery_with_table() {
    // Create a temp file for DuckDB
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.duckdb");

    // Create table using duckdb crate directly
    {
        let conn = duckdb::Connection::open(&db_path).unwrap();
        conn.execute("CREATE SCHEMA test_schema", []).unwrap();
        conn.execute(
            "CREATE TABLE test_schema.users (id INTEGER, name VARCHAR)",
            [],
        )
        .unwrap();
    }

    let fetcher = NativeFetcher::new();
    let source = Source::Duckdb {
        path: db_path.to_str().unwrap().to_string(),
    };

    let result = fetcher.discover_tables(&source).await;
    assert!(
        result.is_ok(),
        "Discovery should succeed: {:?}",
        result.err()
    );

    let tables = result.unwrap();
    assert!(!tables.is_empty(), "Should find the test table");

    let users_table = tables.iter().find(|t| t.table_name == "users");
    assert!(users_table.is_some(), "Should find users table");

    let users = users_table.unwrap();
    assert_eq!(users.columns.len(), 2);
    assert_eq!(users.columns[0].name, "id");
    assert_eq!(users.columns[1].name, "name");
}

#[tokio::test]
async fn test_unsupported_driver() {
    let fetcher = NativeFetcher::new();
    // Use a Snowflake source which is not implemented
    let source = Source::Snowflake {
        account: "fake".to_string(),
        user: "fake".to_string(),
        password: "fake".to_string(),
        warehouse: "fake".to_string(),
        database: "fake".to_string(),
        role: None,
    };

    let result = fetcher.discover_tables(&source).await;
    assert!(result.is_err(), "Should fail for unsupported driver");
}

#[tokio::test]
async fn test_duckdb_fetch_table() {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rivetdb::datafetch::StreamingParquetWriter;
    use std::fs::File;

    // Create a temp DuckDB database with test data
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.duckdb");

    // Populate database with test data (multiple types)
    {
        let conn = duckdb::Connection::open(&db_path).unwrap();
        conn.execute("CREATE SCHEMA test_schema", []).unwrap();
        conn.execute(
            "CREATE TABLE test_schema.products (
                id INTEGER,
                name VARCHAR,
                price DOUBLE,
                in_stock BOOLEAN
            )",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO test_schema.products VALUES
                (1, 'Widget', 19.99, true),
                (2, 'Gadget', 29.99, false),
                (3, 'Doohickey', 39.99, true)",
            [],
        )
        .unwrap();
    }

    // Fetch to parquet using StreamingParquetWriter
    let fetcher = NativeFetcher::new();
    let source = Source::Duckdb {
        path: db_path.to_str().unwrap().to_string(),
    };

    let output_path = temp_dir.path().join("output.parquet");
    let mut writer = StreamingParquetWriter::new(output_path.clone());

    let result = fetcher
        .fetch_table(&source, None, "test_schema", "products", &mut writer)
        .await;
    assert!(result.is_ok(), "Fetch should succeed: {:?}", result.err());

    writer.close().unwrap();

    // Read back the parquet and verify data
    let file = File::open(&output_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut total_rows = 0;
    let mut batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
        batches.push(batch);
    }

    // Verify row count
    assert_eq!(total_rows, 3, "Should have 3 rows");

    // Verify schema has correct columns
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 4);
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    assert!(schema.field_with_name("price").is_ok());
    assert!(schema.field_with_name("in_stock").is_ok());
}
