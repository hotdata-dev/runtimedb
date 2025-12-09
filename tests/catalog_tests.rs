use rivetdb::catalog::CatalogManager;
use rivetdb::catalog::DuckdbCatalogManager;
use tempfile::tempdir;

#[test]
fn test_catalog_initialization() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Should be able to list connections (empty initially)
    let connections = catalog.list_connections().unwrap();
    assert_eq!(connections.len(), 0);
}

#[test]
fn test_add_connection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();

    let connections = catalog.list_connections().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].name, "test_db");
}

#[test]
fn test_clear_connection_cache_metadata() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection and table
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();
    let table_id = catalog.add_table(conn_id, "public", "users", "").unwrap();

    // Update table to have paths
    catalog
        .update_table_sync(table_id, "/fake/path/test.parquet", "/fake/path/test.json")
        .unwrap();

    // Verify paths exist before clear
    let table_before = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    assert!(table_before.parquet_path.is_some());

    // Clear connection cache metadata
    catalog.clear_connection_cache_metadata("test_db").unwrap();

    // Verify connection still exists
    let conn = catalog.get_connection("test_db").unwrap();
    assert!(conn.is_some());

    // Verify table still exists but paths are NULL
    let table_after = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    assert!(table_after.parquet_path.is_none());
    assert!(table_after.state_path.is_none());
    assert!(table_after.last_sync.is_none());
}

#[test]
fn test_delete_connection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection and table
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();
    catalog.add_table(conn_id, "public", "users", "").unwrap();

    // Verify connection and table exist
    assert!(catalog.get_connection("test_db").unwrap().is_some());
    assert!(catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .is_some());

    // Delete connection (metadata only)
    catalog.delete_connection("test_db").unwrap();

    // Verify connection is deleted
    assert!(catalog.get_connection("test_db").unwrap().is_none());

    // Verify table is deleted
    assert!(catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .is_none());
}

#[test]
fn test_clear_nonexistent_connection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Try to clear cache metadata for a connection that doesn't exist
    let result = catalog.clear_connection_cache_metadata("nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_delete_nonexistent_connection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Try to delete a connection that doesn't exist
    let result = catalog.delete_connection("nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_close_catalog() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection before closing
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();

    // Close the catalog
    catalog.close().unwrap();

    // Operations after close should fail
    let result = catalog.list_connections();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("closed"));
}

#[test]
fn test_close_catalog_idempotent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Close should be idempotent (can be called multiple times)
    catalog.close().unwrap();
    catalog.close().unwrap();
    catalog.close().unwrap();
}

#[test]
fn test_close_releases_file_lock() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    {
        let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();
        let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
        catalog
            .add_connection("test_db", "postgres", config)
            .unwrap();

        // Explicitly close
        catalog.close().unwrap();
    }

    // Should be able to open the same database file again
    let catalog2 = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();
    let connections = catalog2.list_connections().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].name, "test_db");
}

#[test]
fn test_list_tables_multiple_connections() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add first connection with tables
    let config1 = r#"{"host": "localhost", "port": 5432, "database": "db1"}"#;
    let conn1_id = catalog
        .add_connection("neon_east", "postgres", config1)
        .unwrap();
    catalog.add_table(conn1_id, "public", "cities", "").unwrap();
    catalog
        .add_table(conn1_id, "public", "locations", "")
        .unwrap();
    catalog
        .add_table(conn1_id, "public", "table_1", "")
        .unwrap();

    // Add second connection with tables
    let config2 = r#"{"host": "localhost", "port": 5433, "database": "db2"}"#;
    let conn2_id = catalog
        .add_connection("connection2", "postgres", config2)
        .unwrap();
    catalog
        .add_table(conn2_id, "public", "table_1", "")
        .unwrap();

    // List all tables (no filter)
    let all_tables = catalog.list_tables(None).unwrap();
    assert_eq!(
        all_tables.len(),
        4,
        "Should have 4 tables total across both connections"
    );

    // List tables for first connection
    let conn1_tables = catalog.list_tables(Some(conn1_id)).unwrap();
    assert_eq!(
        conn1_tables.len(),
        3,
        "Connection 'neon_east' should have 3 tables"
    );
    assert!(conn1_tables.iter().all(|t| t.connection_id == conn1_id));
    assert!(conn1_tables.iter().any(|t| t.table_name == "cities"));
    assert!(conn1_tables.iter().any(|t| t.table_name == "locations"));
    assert!(conn1_tables.iter().any(|t| t.table_name == "table_1"));

    // Verify all tables have correct schema
    assert!(conn1_tables.iter().all(|t| t.schema_name == "public"));

    // List tables for second connection
    let conn2_tables = catalog.list_tables(Some(conn2_id)).unwrap();
    assert_eq!(
        conn2_tables.len(),
        1,
        "Connection 'connection2' should have 1 table"
    );
    assert!(conn2_tables.iter().all(|t| t.connection_id == conn2_id));
    assert_eq!(conn2_tables[0].table_name, "table_1");
    assert_eq!(conn2_tables[0].schema_name, "public");

    // Verify tables are initially not cached
    assert!(conn1_tables.iter().all(|t| t.parquet_path.is_none()));
    assert!(conn2_tables.iter().all(|t| t.parquet_path.is_none()));
}

#[test]
fn test_list_tables_with_cached_status() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add connection with tables
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();
    let table1_id = catalog
        .add_table(conn_id, "public", "cached_table", "")
        .unwrap();
    let _table2_id = catalog
        .add_table(conn_id, "public", "not_cached_table", "")
        .unwrap();

    // Mark one table as cached
    catalog
        .update_table_sync(table1_id, "/fake/path/test.parquet", "/fake/path/test.json")
        .unwrap();

    // List tables and verify cached status
    let tables = catalog.list_tables(Some(conn_id)).unwrap();
    assert_eq!(tables.len(), 2);

    let cached_table = tables
        .iter()
        .find(|t| t.table_name == "cached_table")
        .unwrap();
    let not_cached_table = tables
        .iter()
        .find(|t| t.table_name == "not_cached_table")
        .unwrap();

    assert!(
        cached_table.parquet_path.is_some(),
        "cached_table should have parquet_path"
    );
    assert!(
        cached_table.state_path.is_some(),
        "cached_table should have state_path"
    );
    assert!(
        cached_table.last_sync.is_some(),
        "cached_table should have last_sync"
    );

    assert!(
        not_cached_table.parquet_path.is_none(),
        "not_cached_table should not have parquet_path"
    );
    assert!(
        not_cached_table.state_path.is_none(),
        "not_cached_table should not have state_path"
    );
    assert!(
        not_cached_table.last_sync.is_none(),
        "not_cached_table should not have last_sync"
    );
}

#[test]
fn test_clear_table_cache_metadata() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection and two tables
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();
    let table1_id = catalog.add_table(conn_id, "public", "users", "").unwrap();
    let table2_id = catalog.add_table(conn_id, "public", "orders", "").unwrap();

    // Update both tables to have paths
    catalog
        .update_table_sync(table1_id, "/fake/users.parquet", "/fake/users.json")
        .unwrap();
    catalog
        .update_table_sync(table2_id, "/fake/orders.parquet", "/fake/orders.json")
        .unwrap();

    // Verify both tables have paths before clear
    let table1_before = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    let table2_before = catalog
        .get_table(conn_id, "public", "orders")
        .unwrap()
        .unwrap();
    assert!(table1_before.parquet_path.is_some());
    assert!(table1_before.last_sync.is_some());
    assert!(table2_before.parquet_path.is_some());
    assert!(table2_before.last_sync.is_some());

    // Clear only the first table (returns old table info)
    let table_info = catalog
        .clear_table_cache_metadata(conn_id, "public", "users")
        .unwrap();
    assert!(table_info.parquet_path.is_some()); // Should have old paths

    // Verify first table metadata has been cleared
    let table1_after = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    assert!(table1_after.parquet_path.is_none());
    assert!(table1_after.state_path.is_none());
    assert!(table1_after.last_sync.is_none());

    // Verify second table is unaffected
    let table2_after = catalog
        .get_table(conn_id, "public", "orders")
        .unwrap()
        .unwrap();
    assert!(table2_after.parquet_path.is_some());
    assert!(table2_after.state_path.is_some());
    assert!(table2_after.last_sync.is_some());

    // Verify connection still exists
    assert!(catalog.get_connection("test_db").unwrap().is_some());
}

#[test]
fn test_clear_table_cache_metadata_nonexistent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection but no tables
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();

    // Try to clear a table that doesn't exist
    let result = catalog.clear_table_cache_metadata(conn_id, "public", "nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_clear_table_without_cache() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add a connection and table (without cache paths)
    let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();
    let _table_id = catalog.add_table(conn_id, "public", "users", "").unwrap();

    // Verify table exists but has no cache
    let table_before = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    assert!(table_before.parquet_path.is_none());
    assert!(table_before.state_path.is_none());

    // Clear should succeed even without cache paths
    let table_info = catalog
        .clear_table_cache_metadata(conn_id, "public", "users")
        .unwrap();

    // Returned info should have None paths
    assert!(table_info.parquet_path.is_none());
    assert!(table_info.state_path.is_none());

    // Table should still exist with null paths
    let table_after = catalog
        .get_table(conn_id, "public", "users")
        .unwrap()
        .unwrap();
    assert!(table_after.parquet_path.is_none());
    assert!(table_after.state_path.is_none());
    assert!(table_after.last_sync.is_none());
}
