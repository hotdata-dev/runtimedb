//! Integration tests for datafetch module

use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
use runtimedb::datafetch::{DataFetcher, NativeFetcher};
use runtimedb::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use runtimedb::source::Source;
use std::sync::Arc;
use tempfile::TempDir;

/// Create a test SecretManager with temporary storage.
/// The secret manager is required by the API even for sources that don't use credentials.
async fn test_secret_manager(dir: &TempDir) -> SecretManager {
    let db_path = dir.path().join("test_catalog.db");
    let catalog = Arc::new(
        SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap(),
    );
    catalog.run_migrations().await.unwrap();

    let key = [0x42u8; 32];
    let backend = Arc::new(EncryptedCatalogBackend::new(key, catalog.clone()));

    SecretManager::new(backend, catalog, ENCRYPTED_PROVIDER_TYPE)
}

#[tokio::test]
async fn test_duckdb_discovery_empty() {
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    let fetcher = NativeFetcher::new();
    let source = Source::Duckdb {
        path: ":memory:".to_string(),
    };

    let result = fetcher.discover_tables(&source, &secrets).await;
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
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    // Create a temp file for DuckDB
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

    let result = fetcher.discover_tables(&source, &secrets).await;
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
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    let fetcher = NativeFetcher::new();
    // Use a Snowflake source with no credentials - should fail with connection error
    let source = Source::Snowflake {
        account: "fake".to_string(),
        user: "fake".to_string(),
        warehouse: "fake".to_string(),
        database: "fake".to_string(),
        schema: None,
        role: None,
        credential: runtimedb::source::Credential::None,
    };

    let result = fetcher.discover_tables(&source, &secrets).await;
    assert!(result.is_err(), "Should fail without valid credentials");
}

// MySQL tests using testcontainers
mod mysql_container_tests {
    use super::*;
    use testcontainers::{runners::AsyncRunner, ImageExt};
    use testcontainers_modules::mysql::Mysql;

    const TEST_PASSWORD: &str = "root";

    async fn create_test_secret_manager_with_password(
        dir: &TempDir,
        secret_name: &str,
        password: &str,
    ) -> SecretManager {
        let secrets = test_secret_manager(dir).await;
        secrets
            .create(secret_name, password.as_bytes())
            .await
            .unwrap();
        secrets
    }

    #[tokio::test]
    async fn test_mysql_discovery() {
        let temp_dir = TempDir::new().unwrap();
        let secrets =
            create_test_secret_manager_with_password(&temp_dir, "mysql-pass", TEST_PASSWORD).await;

        // Start MySQL container with explicit root password
        let container = Mysql::default()
            .with_tag("8.0")
            .with_env_var("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
            .start()
            .await
            .expect("Failed to start mysql");
        let port = container.get_host_port_ipv4(3306).await.unwrap();

        // Create test database and table
        let conn_str = format!("mysql://root:{}@localhost:{}/mysql", TEST_PASSWORD, port);
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE DATABASE IF NOT EXISTS testdb")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE testdb.users (id INT, name VARCHAR(100), active BOOLEAN)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        // Test discovery
        let fetcher = NativeFetcher::new();
        let source = Source::Mysql {
            host: "localhost".to_string(),
            port,
            user: "root".to_string(),
            database: "testdb".to_string(),
            credential: runtimedb::source::Credential::SecretRef {
                name: "mysql-pass".to_string(),
            },
        };

        let result = fetcher.discover_tables(&source, &secrets).await;
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
        assert_eq!(users.columns.len(), 3);
        assert_eq!(users.columns[0].name, "id");
        assert_eq!(users.columns[1].name, "name");
        assert_eq!(users.columns[2].name, "active");
    }

    #[tokio::test]
    async fn test_mysql_fetch_table() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use runtimedb::datafetch::StreamingParquetWriter;
        use std::fs::File;

        let temp_dir = TempDir::new().unwrap();
        let secrets =
            create_test_secret_manager_with_password(&temp_dir, "mysql-pass", TEST_PASSWORD).await;

        // Start MySQL container with explicit root password
        let container = Mysql::default()
            .with_tag("8.0")
            .with_env_var("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
            .start()
            .await
            .expect("Failed to start mysql");
        let port = container.get_host_port_ipv4(3306).await.unwrap();

        // Create test database with data
        let conn_str = format!("mysql://root:{}@localhost:{}/mysql", TEST_PASSWORD, port);
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE DATABASE IF NOT EXISTS testdb")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE testdb.products (
                id INT,
                name VARCHAR(100),
                price DOUBLE,
                in_stock BOOLEAN
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO testdb.products VALUES
                (1, 'Widget', 19.99, true),
                (2, 'Gadget', 29.99, false),
                (3, 'Doohickey', 39.99, true)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        // Fetch to parquet
        let fetcher = NativeFetcher::new();
        let source = Source::Mysql {
            host: "localhost".to_string(),
            port,
            user: "root".to_string(),
            database: "testdb".to_string(),
            credential: runtimedb::source::Credential::SecretRef {
                name: "mysql-pass".to_string(),
            },
        };

        let output_path = temp_dir.path().join("mysql_output.parquet");
        let mut writer = StreamingParquetWriter::new(output_path.clone());

        let result = fetcher
            .fetch_table(&source, &secrets, None, "testdb", "products", &mut writer)
            .await;
        assert!(result.is_ok(), "Fetch should succeed: {:?}", result.err());

        writer.close().unwrap();

        // Verify parquet file
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

        assert_eq!(total_rows, 3, "Should have 3 rows");

        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("price").is_ok());
        assert!(schema.field_with_name("in_stock").is_ok());
    }

    /// Test that nullable flags from the database are correctly preserved in Parquet schema.
    /// This verifies fix for issue #59: columns declared as NOT NULL should have nullable=false
    /// in the resulting Arrow/Parquet schema.
    #[tokio::test]
    async fn test_mysql_fetch_preserves_nullable_flags() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use runtimedb::datafetch::StreamingParquetWriter;
        use std::fs::File;

        let temp_dir = TempDir::new().unwrap();
        let secrets =
            create_test_secret_manager_with_password(&temp_dir, "mysql-pass", TEST_PASSWORD).await;

        // Start MySQL container
        let container = Mysql::default()
            .with_tag("8.0")
            .with_env_var("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
            .start()
            .await
            .expect("Failed to start mysql");
        let port = container.get_host_port_ipv4(3306).await.unwrap();

        // Create test database with explicit nullable/non-nullable columns
        let conn_str = format!("mysql://root:{}@localhost:{}/mysql", TEST_PASSWORD, port);
        let pool = sqlx::MySqlPool::connect(&conn_str).await.unwrap();

        sqlx::query("CREATE DATABASE IF NOT EXISTS testdb")
            .execute(&pool)
            .await
            .unwrap();

        // Create table with mix of nullable and non-nullable columns
        // c_custkey and c_name are NOT NULL (like TPC-H customer table)
        // c_acctbal and c_comment are nullable
        sqlx::query(
            "CREATE TABLE testdb.customer (
                c_custkey INT NOT NULL,
                c_name VARCHAR(100) NOT NULL,
                c_acctbal DECIMAL(15,2),
                c_comment VARCHAR(255)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert data including NULL values for nullable columns
        sqlx::query(
            "INSERT INTO testdb.customer VALUES
                (1, 'Customer One', 1234.56, 'Regular customer'),
                (2, 'Customer Two', NULL, NULL),
                (3, 'Customer Three', 9999.99, 'VIP customer')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        // Fetch to parquet
        let fetcher = NativeFetcher::new();
        let source = Source::Mysql {
            host: "localhost".to_string(),
            port,
            user: "root".to_string(),
            database: "testdb".to_string(),
            credential: runtimedb::source::Credential::SecretRef {
                name: "mysql-pass".to_string(),
            },
        };

        let output_path = temp_dir.path().join("customer_output.parquet");
        let mut writer = StreamingParquetWriter::new(output_path.clone());

        let result = fetcher
            .fetch_table(&source, &secrets, None, "testdb", "customer", &mut writer)
            .await;
        assert!(result.is_ok(), "Fetch should succeed: {:?}", result.err());

        writer.close().unwrap();

        // Verify parquet file schema has correct nullable flags
        let file = File::open(&output_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result.unwrap());
        }

        let schema = batches[0].schema();

        // Verify non-nullable columns
        let c_custkey = schema.field_with_name("c_custkey").unwrap();
        assert!(
            !c_custkey.is_nullable(),
            "c_custkey should be NOT NULL, but schema has nullable={}",
            c_custkey.is_nullable()
        );

        let c_name = schema.field_with_name("c_name").unwrap();
        assert!(
            !c_name.is_nullable(),
            "c_name should be NOT NULL, but schema has nullable={}",
            c_name.is_nullable()
        );

        // Verify nullable columns
        let c_acctbal = schema.field_with_name("c_acctbal").unwrap();
        assert!(
            c_acctbal.is_nullable(),
            "c_acctbal should be nullable, but schema has nullable={}",
            c_acctbal.is_nullable()
        );

        let c_comment = schema.field_with_name("c_comment").unwrap();
        assert!(
            c_comment.is_nullable(),
            "c_comment should be nullable, but schema has nullable={}",
            c_comment.is_nullable()
        );

        // Verify we have all 3 rows (including rows with NULLs)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should have 3 rows");
    }
}

#[tokio::test]
async fn test_duckdb_fetch_table() {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use runtimedb::datafetch::StreamingParquetWriter;
    use std::fs::File;

    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    // Create a temp DuckDB database with test data
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
        .fetch_table(
            &source,
            &secrets,
            None,
            "test_schema",
            "products",
            &mut writer,
        )
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
