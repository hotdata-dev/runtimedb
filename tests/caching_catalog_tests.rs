//! Integration tests for CachingCatalogManager.
//!
//! These tests require a Redis instance. They use testcontainers to spin up
//! a Redis container for testing.

use runtimedb::catalog::{CachingCatalogManager, CatalogManager, SqliteCatalogManager};
use runtimedb::config::CacheConfig;
use std::sync::Arc;
use tempfile::tempdir;
use testcontainers::{runners::AsyncRunner, GenericImage};

/// Create a Redis container for testing.
async fn start_redis() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379.into())
        .start()
        .await
        .expect("Failed to start Redis container");

    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://127.0.0.1:{}", port);

    (container, url)
}

/// Create a SQLite catalog in a temp directory.
async fn create_sqlite_catalog() -> (tempfile::TempDir, Arc<dyn CatalogManager>) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");
    let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    catalog.run_migrations().await.unwrap();
    (dir, Arc::new(catalog))
}

#[tokio::test]
async fn test_cache_miss_then_hit() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // First call should be a cache miss - goes to inner catalog
    let conns1 = caching.list_connections().await.unwrap();
    assert!(conns1.is_empty());

    // Second call should be a cache hit - same result
    let conns2 = caching.list_connections().await.unwrap();
    assert!(conns2.is_empty());

    // Add a connection through the caching layer
    let id = caching
        .add_connection("test_conn", "postgres", r#"{"host":"localhost"}"#, None)
        .await
        .unwrap();

    // Write-through should have updated the cache
    let conns3 = caching.list_connections().await.unwrap();
    assert_eq!(conns3.len(), 1);
    assert_eq!(conns3[0].id, id);
}

#[tokio::test]
async fn test_connection_write_through() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test2:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Add connection
    let id = caching
        .add_connection("my_conn", "postgres", r#"{"host":"db.example.com"}"#, None)
        .await
        .unwrap();

    // get_connection should work and be cached
    let conn = caching.get_connection(&id).await.unwrap();
    assert!(conn.is_some());
    assert_eq!(conn.as_ref().unwrap().name, "my_conn");

    // get_connection_by_name should also be cached
    let conn2 = caching.get_connection_by_name("my_conn").await.unwrap();
    assert!(conn2.is_some());
    assert_eq!(conn2.as_ref().unwrap().id, id);

    // Delete connection
    caching.delete_connection(&id).await.unwrap();

    // Should be removed from cache
    let conns = caching.list_connections().await.unwrap();
    assert!(conns.is_empty());
}

#[tokio::test]
async fn test_table_caching() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test3:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Add a connection first
    let conn_id = caching
        .add_connection("table_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Add a table
    let table_id = caching
        .add_table(&conn_id, "public", "users", r#"{"fields":[]}"#)
        .await
        .unwrap();
    assert!(table_id > 0);

    // list_tables should be cached
    let tables = caching.list_tables(Some(&conn_id)).await.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name, "users");

    // get_table should be cached
    let table = caching
        .get_table(&conn_id, "public", "users")
        .await
        .unwrap();
    assert!(table.is_some());
}

#[tokio::test]
async fn test_special_characters_in_keys() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "test4:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Connection name with special characters
    let id = caching
        .add_connection("my:special:conn", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Should work correctly with URL encoding
    let conn = caching
        .get_connection_by_name("my:special:conn")
        .await
        .unwrap();
    assert!(conn.is_some());

    // Add table with special characters in schema/name
    let _ = caching
        .add_table(&id, "my:schema", "my:table", r#"{}"#)
        .await
        .unwrap();

    let table = caching
        .get_table(&id, "my:schema", "my:table")
        .await
        .unwrap();
    assert!(table.is_some());
    assert_eq!(table.as_ref().unwrap().schema_name, "my:schema");
    assert_eq!(table.as_ref().unwrap().table_name, "my:table");
}

#[tokio::test]
async fn test_warmup_loop_populates_cache() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add some data before creating caching layer
    let conn_id = inner
        .add_connection("warmup_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();
    let _ = inner
        .add_table(&conn_id, "public", "warmup_table", r#"{}"#)
        .await
        .unwrap();

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 1, // 1 second for test
        warmup_lock_ttl_secs: 5,
        key_prefix: "warmup_test:".to_string(),
    };

    let caching = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config)
            .await
            .unwrap(),
    );

    // Start warmup loop
    let handle = caching.start_warmup_loop("test-node".to_string());

    // Wait for warmup to run (interval is 1 second, so wait 2)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stop the warmup loop
    handle.abort();

    // Verify cache was populated by checking Redis directly
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("warmup_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should have conn:list, conn:{id}, conn:name:{name}, tbl:list:all, tbl:list:conn:{id}, etc.
    assert!(!keys.is_empty(), "Warmup should have populated cache keys");
    assert!(
        keys.iter().any(|k| k.contains("conn:list")),
        "Should have conn:list key"
    );
}

#[tokio::test]
async fn test_warmup_distributed_lock() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add some data before starting warmup
    let _ = inner
        .add_connection("lock_test_conn", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 1,
        warmup_lock_ttl_secs: 10,
        key_prefix: "lock_test:".to_string(),
    };

    // Create two caching managers
    let caching1 = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config.clone())
            .await
            .unwrap(),
    );
    let caching2 = Arc::new(
        CachingCatalogManager::new(inner.clone(), &redis_url, config)
            .await
            .unwrap(),
    );

    // Start both warmup loops
    let handle1 = caching1.start_warmup_loop("node-1".to_string());
    let handle2 = caching2.start_warmup_loop("node-2".to_string());

    // Wait for warmup cycles to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Stop both
    handle1.abort();
    handle2.abort();

    // After warmup, lock should be released (we release after successful warmup)
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let lock_value: Option<String> = redis::cmd("GET")
        .arg("lock_test:lock:warmup")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Lock should be released after successful warmup
    assert!(
        lock_value.is_none(),
        "Lock should be released after warmup completes"
    );

    // But cache should be populated (proving warmup ran)
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("lock_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        keys.iter().any(|k| k.contains("conn:list")),
        "Cache should be populated after warmup"
    );
}

/// Test that update_table_sync properly invalidates per-table and per-connection caches.
#[tokio::test]
async fn test_update_table_sync_cache_invalidation() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "update_sync_test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Add a connection and table
    let conn_id = caching
        .add_connection("sync_test_conn", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    let table_id = caching
        .add_table(&conn_id, "public", "sync_table", r#"{"fields":[]}"#)
        .await
        .unwrap();

    // Read the table to populate cache (should have no parquet_path)
    let table1 = caching
        .get_table(&conn_id, "public", "sync_table")
        .await
        .unwrap()
        .unwrap();
    assert!(
        table1.parquet_path.is_none(),
        "Should start with no parquet path"
    );

    // Also populate per-connection list cache
    let tables_by_conn = caching.list_tables(Some(&conn_id)).await.unwrap();
    assert_eq!(tables_by_conn.len(), 1);
    assert!(tables_by_conn[0].parquet_path.is_none());

    // Now update the sync path
    caching
        .update_table_sync(table_id, "/path/to/data.parquet")
        .await
        .unwrap();

    // The per-table cache should be invalidated and show the new path
    let table2 = caching
        .get_table(&conn_id, "public", "sync_table")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        table2.parquet_path,
        Some("/path/to/data.parquet".to_string()),
        "update_table_sync should invalidate per-table cache"
    );

    // The per-connection list cache should also be invalidated
    let tables_by_conn2 = caching.list_tables(Some(&conn_id)).await.unwrap();
    assert_eq!(
        tables_by_conn2[0].parquet_path,
        Some("/path/to/data.parquet".to_string()),
        "update_table_sync should invalidate per-connection list cache"
    );
}

/// Test that dataset mutations invalidate list_dataset_table_names cache.
#[tokio::test]
async fn test_dataset_cache_invalidation() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "dataset_test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    let schema_name = "datasets";

    // Initially no datasets
    let names = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert!(names.is_empty());

    // Create a dataset
    let dataset = runtimedb::catalog::DatasetInfo {
        id: "ds-001".to_string(),
        label: "Test Dataset".to_string(),
        schema_name: schema_name.to_string(),
        table_name: "my_table".to_string(),
        parquet_url: "s3://bucket/data.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    caching.create_dataset(&dataset).await.unwrap();

    // list_dataset_table_names should now include the new table
    let names = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert_eq!(
        names,
        vec!["my_table"],
        "create_dataset should invalidate cache"
    );

    // Update the dataset (rename table)
    caching
        .update_dataset("ds-001", "Renamed Dataset", "renamed_table")
        .await
        .unwrap();

    // Cache should reflect the update
    let names = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert_eq!(
        names,
        vec!["renamed_table"],
        "update_dataset should invalidate cache"
    );

    // Delete the dataset
    caching.delete_dataset("ds-001").await.unwrap();

    // Cache should reflect the deletion
    let names = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert!(names.is_empty(), "delete_dataset should invalidate cache");
}

#[tokio::test]
async fn test_redis_unavailable_at_startup_fails() {
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add data directly to inner catalog
    let _conn_id = inner
        .add_connection("fallback_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // Use a localhost address with a port that's not listening - fails faster than invalid hostname
    let invalid_url = "redis://127.0.0.1:59999";
    let config = CacheConfig {
        redis_url: Some(invalid_url.to_string()),
        hard_ttl_secs: 60,
        warmup_interval_secs: 0,
        warmup_lock_ttl_secs: 30,
        key_prefix: "fallback:".to_string(),
    };

    // This should fail to connect - use a timeout to avoid hanging on connection attempts
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        CachingCatalogManager::new(inner.clone(), invalid_url, config),
    )
    .await;

    // Connection should either timeout or fail - caching manager requires Redis to be available at startup
    // This is by design - if Redis is configured, it should be available
    match result {
        Ok(Ok(_)) => panic!("Should fail to create caching manager with invalid Redis URL"),
        Ok(Err(_)) => {} // Connection error - expected
        Err(_) => {}     // Timeout - also acceptable for unavailable Redis
    }
}
