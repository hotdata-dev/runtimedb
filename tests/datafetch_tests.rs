//! Integration tests for datafetch module
//!
//! These tests require ADBC drivers to be available.
//! Set RIVETDB_DRIVER_DUCKDB to the path of libduckdb.dylib/so

use rivetdb::datafetch::{AdbcFetcher, ConnectionConfig, DataFetcher};
use std::env;

fn has_duckdb_driver() -> bool {
    env::var("RIVETDB_DRIVER_DUCKDB").is_ok()
}

#[tokio::test]
async fn test_duckdb_discovery() {
    if !has_duckdb_driver() {
        eprintln!("Skipping test: RIVETDB_DRIVER_DUCKDB not set");
        return;
    }

    let fetcher = AdbcFetcher::new();
    let config = ConnectionConfig {
        source_type: "duckdb".to_string(),
        connection_string: ":memory:".to_string(),
    };

    // For in-memory DuckDB, we need to create a table first
    // This test just verifies the discovery mechanism works
    let result = fetcher.discover_tables(&config).await;

    // In-memory DuckDB with no tables should return empty list
    assert!(result.is_ok(), "Discovery should succeed: {:?}", result.err());
    let tables = result.unwrap();
    assert!(tables.is_empty(), "Empty DuckDB should have no tables");
}

#[tokio::test]
async fn test_unsupported_driver() {
    let fetcher = AdbcFetcher::new();
    let config = ConnectionConfig {
        source_type: "unsupported_db".to_string(),
        connection_string: "fake://connection".to_string(),
    };

    let result = fetcher.discover_tables(&config).await;
    assert!(result.is_err(), "Should fail for unsupported driver");
}