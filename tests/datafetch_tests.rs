//! Integration tests for datafetch module

use rivetdb::datafetch::{ConnectionConfig, DataFetcher, NativeFetcher};

#[tokio::test]
async fn test_duckdb_discovery_empty() {
    let fetcher = NativeFetcher::new();
    let config = ConnectionConfig {
        source_type: "duckdb".to_string(),
        connection_string: ":memory:".to_string(),
    };

    let result = fetcher.discover_tables(&config).await;
    assert!(result.is_ok(), "Discovery should succeed: {:?}", result.err());

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
    let config = ConnectionConfig {
        source_type: "duckdb".to_string(),
        connection_string: db_path.to_str().unwrap().to_string(),
    };

    let result = fetcher.discover_tables(&config).await;
    assert!(result.is_ok(), "Discovery should succeed: {:?}", result.err());

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
    let config = ConnectionConfig {
        source_type: "unsupported_db".to_string(),
        connection_string: "fake://connection".to_string(),
    };

    let result = fetcher.discover_tables(&config).await;
    assert!(result.is_err(), "Should fail for unsupported driver");
}