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

    // Wait for a warmup cycle
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stop both
    handle1.abort();
    handle2.abort();

    // Check that only one lock exists (only one should win)
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let lock_value: Option<String> = redis::cmd("GET")
        .arg("lock_test:lock:warmup")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Lock should exist and be held by one node
    assert!(lock_value.is_some(), "Lock should exist after warmup cycle");
    let value = lock_value.unwrap();
    assert!(
        value == "node-1" || value == "node-2",
        "Lock should be held by one of the nodes"
    );
}
