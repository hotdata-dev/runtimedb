use tempfile::tempdir;
use rivetdb::catalog::CatalogManager;
use rivetdb::catalog::DuckdbCatalogManager;

#[test]
fn test_full_connection_workflow() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add connection
    let config = r#"{"type": "postgres", "host": "localhost", "port": 5432}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();

    // Add some tables
    catalog.add_table(conn_id, "public", "users", "").unwrap();
    catalog.add_table(conn_id, "public", "orders", "").unwrap();

    // List connections
    let connections = catalog.list_connections().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].name, "test_db");

    // List tables
    let tables = catalog.list_tables(Some(conn_id)).unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.table_name == "users"));
    assert!(tables.iter().any(|t| t.table_name == "orders"));
}

#[tokio::test]
async fn test_duckdb_sync_and_query() {
    use datafusion::prelude::*;
    use rivetdb::datafetch::{ConnectionConfig, DataFetcher, NativeFetcher, StreamingParquetWriter};
    use rivetdb::storage::{FilesystemStorage, StorageManager};

    // Create temp directories
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("source.duckdb");
    let cache_base = temp_dir.path().join("cache");
    let state_base = temp_dir.path().join("state");

    // Create DuckDB source with test data
    {
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
    }

    // Create storage
    let storage = FilesystemStorage::new(
        cache_base.to_str().unwrap(),
        state_base.to_str().unwrap(),
    );

    // Use NativeFetcher with StreamingParquetWriter
    let fetcher = NativeFetcher::new();
    let config = ConnectionConfig {
        source_type: "duckdb".to_string(),
        connection_string: db_path.to_str().unwrap().to_string(),
    };

    let conn_id = 1;
    let schema = "sales";
    let table = "orders";

    // Full flow: prepare -> fetch -> close -> finalize
    let write_path = storage.prepare_cache_write(conn_id, schema, table);
    let mut writer = StreamingParquetWriter::new(write_path.clone());

    fetcher
        .fetch_table(&config, None, schema, table, &mut writer)
        .await
        .unwrap();

    writer.close().unwrap();

    let _cache_url = storage
        .finalize_cache_write(&write_path, conn_id, schema, table)
        .await
        .unwrap();

    // Verify the parquet file exists
    assert!(write_path.exists());

    // Use DataFusion to query the cached parquet
    let ctx = SessionContext::new();

    // Register the parquet file (use the actual file path, not the directory URL)
    let file_url = format!("file://{}", write_path.to_str().unwrap());
    ctx.register_parquet("orders", &file_url, ParquetReadOptions::default())
        .await
        .unwrap();

    // Query the data
    let df = ctx.sql("SELECT order_id, customer_name, amount, is_paid FROM orders ORDER BY order_id").await.unwrap();
    let batches = df.collect().await.unwrap();

    // Verify results
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.num_columns(), 4);

    // Verify we can aggregate
    let df = ctx
        .sql("SELECT COUNT(*) as total, SUM(amount) as total_amount FROM orders WHERE is_paid = true")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // Verify schema
    let schema = batch.schema();
    assert!(schema.field_with_name("order_id").is_ok());
    assert!(schema.field_with_name("customer_name").is_ok());
    assert!(schema.field_with_name("amount").is_ok());
    assert!(schema.field_with_name("is_paid").is_ok());
}
