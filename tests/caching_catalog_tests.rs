//! Integration tests for CachingCatalogManager.
//!
//! These tests require a Redis instance. They use testcontainers to spin up
//! a Redis container for testing.

use runtimedb::catalog::{
    CachingCatalogManager, CatalogManager, CreateQueryRun, QueryRunCursor, QueryRunUpdate,
    SqliteCatalogManager,
};
use runtimedb::config::CacheConfig;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
async fn test_refresh_loop_populates_cache() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add some data before creating caching layer
    let conn_id = inner
        .add_connection("refresh_test", "postgres", r#"{}"#, None)
        .await
        .unwrap();
    let _ = inner
        .add_table(&conn_id, "public", "refresh_table", r#"{}"#)
        .await
        .unwrap();

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 1, // 1 second for test
        key_prefix: "refresh_test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Initialize (starts refresh loop)
    caching.init().await.unwrap();

    // Wait for refresh to run (interval is 1 second, so wait 2)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Close to stop refresh loop gracefully
    caching.close().await.unwrap();

    // Verify cache was populated by checking Redis directly
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("refresh_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should have conn:list, conn:{id}, conn:name:{name}, tbl:list:all, tbl:list:conn:{id}, etc.
    assert!(!keys.is_empty(), "Refresh should have populated cache keys");
    assert!(
        keys.iter().any(|k| k.contains("conn:list")),
        "Should have conn:list key"
    );
}

#[tokio::test]
async fn test_refresh_distributed_lock() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add some data before starting refresh
    let _ = inner
        .add_connection("lock_test_conn", "postgres", r#"{}"#, None)
        .await
        .unwrap();

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 1,
        key_prefix: "lock_test:".to_string(),
    };

    // Create two caching managers
    let caching1 = CachingCatalogManager::new(inner.clone(), &redis_url, config.clone())
        .await
        .unwrap();
    let caching2 = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Initialize both (starts refresh loops)
    caching1.init().await.unwrap();
    caching2.init().await.unwrap();

    // Wait for refresh cycles to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Close both gracefully
    caching1.close().await.unwrap();
    caching2.close().await.unwrap();

    // After refresh, lock should be released (we release after successful refresh)
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let lock_value: Option<String> = redis::cmd("GET")
        .arg("lock_test:lock:refresh")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Lock should be released after successful refresh
    assert!(
        lock_value.is_none(),
        "Lock should be released after refresh completes"
    );

    // But cache should be populated (proving refresh ran)
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("lock_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        keys.iter().any(|k| k.contains("conn:list")),
        "Cache should be populated after refresh"
    );
}

/// Test that delete_connection does not incorrectly delete schema-scoped cache keys.
/// The key_tbl_names is schema-scoped, not connection-scoped.
/// Bug: delete_connection was calling key_tbl_names(connection_id) which would
/// incorrectly delete the schema cache if connection_id happened to match a schema_name.
#[tokio::test]
async fn test_delete_connection_preserves_schema_cache() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "del_conn_test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Create a dataset where the schema_name matches what will become a connection_id
    // This triggers the bug where delete_connection incorrectly deletes schema cache
    let schema_name = "conn-schema-collision";

    let dataset = runtimedb::catalog::DatasetInfo {
        id: "ds-collision-test".to_string(),
        label: "Collision Test".to_string(),
        schema_name: schema_name.to_string(),
        table_name: "collision_table".to_string(),
        parquet_url: "s3://bucket/collision.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    caching.create_dataset(&dataset).await.unwrap();

    // Populate the dataset table names cache
    let names = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert_eq!(names, vec!["collision_table"]);

    // Verify cache key exists in Redis
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let key = format!("del_conn_test:tbl:names:{}", schema_name);
    let exists_before: bool = redis::cmd("EXISTS")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(exists_before, "Schema cache key should exist before test");

    // Now add a connection where the ID might collide with schema name
    // We use the inner catalog to control the connection ID for this test
    // Since SQLite uses UUIDs, we'll create a connection and use its ID
    let conn_id = caching
        .add_connection(schema_name, "postgres", r#"{}"#, None)
        .await
        .unwrap();

    // The connection ID is a UUID, not matching schema_name.
    // But the BUG was using connection_id with key_tbl_names which is wrong conceptually.
    // After our fix, delete_connection should not call key_tbl_names at all.

    // Delete the connection
    caching.delete_connection(&conn_id).await.unwrap();

    // The schema cache should still exist
    let exists_after: bool = redis::cmd("EXISTS")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        exists_after,
        "Schema cache key should still exist after delete_connection"
    );

    // And the cached data should still be correct
    let names_after = caching.list_dataset_table_names(schema_name).await.unwrap();
    assert_eq!(
        names_after,
        vec!["collision_table"],
        "delete_connection should not affect schema-scoped caches"
    );
}

/// Test that update_table_sync properly invalidates per-table and per-connection caches.
#[tokio::test]
async fn test_update_table_sync_cache_invalidation() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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

/// Test that refresh aborts when the distributed lock is lost.
/// This simulates another node stealing the lock mid-refresh and verifies
/// that no new cache writes occur after lock loss is detected.
#[tokio::test]
async fn test_refresh_aborts_on_lock_loss() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    // Add many connections so refresh takes time and we can observe partial completion
    for i in 0..20 {
        inner
            .add_connection(&format!("conn_{}", i), "postgres", r#"{}"#, None)
            .await
            .unwrap();
    }

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 1,
        key_prefix: "lock_loss_test:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Initialize (starts refresh loop)
    caching.init().await.unwrap();

    // Wait for refresh to start and acquire lock
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;

    // Simulate another node stealing the lock by deleting and re-acquiring it
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Delete the lock (simulating expiry or another node's action)
    let _: () = redis::cmd("DEL")
        .arg("lock_loss_test:lock:refresh")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Set a new lock with a different node ID (simulating another node acquiring it)
    let _: () = redis::cmd("SET")
        .arg("lock_loss_test:lock:refresh")
        .arg("node-thief")
        .arg("EX")
        .arg(10)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Wait for the heartbeat to detect lock loss (heartbeat runs at lock_ttl/2)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Count cache keys at this point (refresh should have aborted)
    let keys_after_lock_loss: Vec<String> = redis::cmd("KEYS")
        .arg("lock_loss_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();
    let count_after_lock_loss = keys_after_lock_loss.len();

    // Wait additional time to see if any new keys are written (they shouldn't be)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let keys_after_wait: Vec<String> = redis::cmd("KEYS")
        .arg("lock_loss_test:*")
        .query_async(&mut conn)
        .await
        .unwrap();
    let count_after_wait = keys_after_wait.len();

    // Close the caching manager (will stop refresh loop gracefully)
    caching.close().await.unwrap();

    // Verify the lock is still held by the "thief"
    let lock_value: Option<String> = redis::cmd("GET")
        .arg("lock_loss_test:lock:refresh")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(
        lock_value,
        Some("node-thief".to_string()),
        "Lock should still be held by the thief node"
    );

    // Assert that no new cache keys were written after lock loss was detected
    // The count should remain the same (refresh stopped writing)
    assert_eq!(
        count_after_lock_loss, count_after_wait,
        "Refresh should stop writing after lock loss (keys before: {}, after: {})",
        count_after_lock_loss, count_after_wait
    );
}

/// Test that first-page query listing is cached on miss then served from cache on hit.
#[tokio::test]
async fn test_query_list_first_page_cache_miss_then_hit() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "qlist_hit:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Create a query run so the list is non-empty
    let id = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id,
            sql_text: "SELECT 1",
            sql_hash: "abc123",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // First list call (cache miss) — should populate the cache
    let (runs1, has_more1) = caching.list_query_runs(100, None).await.unwrap();
    assert_eq!(runs1.len(), 1);
    assert!(!has_more1);

    // Verify the cache key was written in Redis
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let exists: bool = redis::cmd("EXISTS")
        .arg("qlist_hit:queries:first:limit:100")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(exists, "First-page cache key should exist after miss");

    // Second list call (cache hit) — should return the same data
    let (runs2, has_more2) = caching.list_query_runs(100, None).await.unwrap();
    assert_eq!(runs2.len(), 1);
    assert_eq!(runs2[0].id, runs1[0].id);
    assert!(!has_more2);
}

/// Test that create_query_run sets the dirty marker, which causes the next
/// list_query_runs to serve stale and trigger revalidation that refreshes
/// the cached contents.
#[tokio::test]
async fn test_query_list_dirty_marker_on_create() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "qlist_dirty:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Populate cache with an empty list (limit=100 is a cacheable limit)
    let (runs, _) = caching.list_query_runs(100, None).await.unwrap();
    assert!(runs.is_empty());

    // Verify dirty marker does NOT exist yet
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let dirty_before: Option<String> = redis::cmd("GET")
        .arg("qlist_dirty:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        dirty_before.is_none(),
        "Dirty marker should not exist before write"
    );

    // Create a query run — should set dirty marker
    let id = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id,
            sql_text: "SELECT 42",
            sql_hash: "def456",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Verify dirty marker IS set
    let dirty_after: Option<String> = redis::cmd("GET")
        .arg("qlist_dirty:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        dirty_after.is_some(),
        "Dirty marker should be set after create_query_run"
    );

    // Read while dirty — should serve stale (empty) and trigger revalidation
    let (runs_stale, _) = caching.list_query_runs(100, None).await.unwrap();
    assert!(
        runs_stale.is_empty(),
        "Stale cache should still return empty list immediately"
    );

    // Poll until the cache reflects the new run (revalidation completes asynchronously)
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (runs_check, _) = caching.list_query_runs(100, None).await.unwrap();
        if runs_check.len() == 1 {
            assert_eq!(runs_check[0].id, id);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for cache revalidation to reflect the new query run"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Test that update_query_run also sets the dirty marker.
#[tokio::test]
async fn test_query_list_dirty_marker_on_update() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "qlist_upd:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Create a query run
    let id = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id,
            sql_text: "SELECT 1",
            sql_hash: "abc",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Clear the dirty marker manually to isolate the update test
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("DEL")
        .arg("qlist_upd:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Update the query run — should re-set dirty marker
    caching
        .update_query_run(
            &id,
            QueryRunUpdate::Succeeded {
                result_id: Some("rslt_test"),
                row_count: 1,
                execution_time_ms: 10,
                warning_message: None,
            },
        )
        .await
        .unwrap();

    let dirty: Option<String> = redis::cmd("GET")
        .arg("qlist_upd:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        dirty.is_some(),
        "Dirty marker should be set after update_query_run"
    );
}

/// Test that cursor-based requests and non-standard limits bypass the cache.
#[tokio::test]
async fn test_query_list_cache_bypass_for_cursors_and_odd_limits() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "qlist_bypass:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Create a query run
    let id = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id,
            sql_text: "SELECT 1",
            sql_hash: "aaa",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Request with non-cacheable limit (e.g. 30) — should NOT create a cache key
    let (runs, _) = caching.list_query_runs(30, None).await.unwrap();
    assert_eq!(runs.len(), 1);

    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let exists: bool = redis::cmd("EXISTS")
        .arg("qlist_bypass:queries:first:limit:30")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        !exists,
        "Non-standard limit should not create a cache entry"
    );

    // Request with cursor — should NOT create a cache key for limit=100
    let cursor = QueryRunCursor {
        created_at: runs[0].created_at,
        id: runs[0].id.clone(),
    };
    let (runs2, _) = caching.list_query_runs(100, Some(&cursor)).await.unwrap();
    assert!(runs2.is_empty());

    // The first-page key for limit=100 should NOT exist (cursor request shouldn't populate it)
    let exists: bool = redis::cmd("EXISTS")
        .arg("qlist_bypass:queries:first:limit:100")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        !exists,
        "Cursor request should not create first-page cache entry"
    );
}

/// Test stale-while-revalidate: stale cache is served while background refresh happens.
#[tokio::test]
async fn test_query_list_stale_while_revalidate() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "qlist_swr:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Create first query run and populate cache
    let id1 = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id1,
            sql_text: "SELECT 1",
            sql_hash: "hash1",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Clear dirty marker, then populate cache
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("DEL")
        .arg("qlist_swr:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();

    let (runs, _) = caching.list_query_runs(100, None).await.unwrap();
    assert_eq!(runs.len(), 1, "Should see 1 query run initially");

    // Now create a second query run — this sets the dirty marker
    let id2 = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id2,
            sql_text: "SELECT 2",
            sql_hash: "hash2",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Immediately read — should get stale data (1 run) while triggering async revalidation
    let (runs_stale, _) = caching.list_query_runs(100, None).await.unwrap();
    assert_eq!(
        runs_stale.len(),
        1,
        "Stale cache should still return 1 run (dirty marker triggers revalidation)"
    );

    // Poll until background revalidation clears the dirty marker and updates the cache
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let dirty_val: Option<String> = redis::cmd("GET")
            .arg("qlist_swr:queries:dirty")
            .query_async(&mut conn)
            .await
            .unwrap();
        let (runs_check, _) = caching.list_query_runs(100, None).await.unwrap();
        if dirty_val.is_none() && runs_check.len() == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for revalidation to clear dirty marker and update cache"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Test that the revalidation lock's compare-and-delete release prevents one worker
/// from deleting another worker's lock. Simulates the race where worker A's lock
/// expires, worker B acquires, and worker A's release attempt is a no-op.
#[tokio::test]
async fn test_revalidation_lock_ownership_safety() {
    let (_redis, redis_url) = start_redis().await;
    let (_dir, inner) = create_sqlite_catalog().await;

    let config = CacheConfig {
        redis_url: Some(redis_url.clone()),
        ttl_secs: 60,
        refresh_interval_secs: 0,
        key_prefix: "lock_own:".to_string(),
    };

    let caching = CachingCatalogManager::new(inner.clone(), &redis_url, config)
        .await
        .unwrap();

    // Seed a query run so the list is non-empty for cache population
    let id1 = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id1,
            sql_text: "SELECT 1",
            sql_hash: "own_test",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Prime the cache (clean first-page fetch)
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("DEL")
        .arg("lock_own:queries:dirty")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _ = caching.list_query_runs(100, None).await.unwrap();

    // Create a second query run (sets dirty marker)
    let id2 = runtimedb::id::generate_query_run_id();
    caching
        .create_query_run(CreateQueryRun {
            id: &id2,
            sql_text: "SELECT 2",
            sql_hash: "own_test2",
            metadata: &serde_json::json!({}),
            trace_id: None,
        })
        .await
        .unwrap();

    // Manually acquire the revalidation lock with a known "worker-B" token
    // BEFORE triggering revalidation, to simulate the race:
    // worker A's lock expired and worker B grabbed it.
    let lock_key = "lock_own:queries:revalidate_lock";
    let worker_b_token = "worker-B-unique-token";
    let _: () = redis::cmd("SET")
        .arg(lock_key)
        .arg(worker_b_token)
        .arg("EX")
        .arg(30u64)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Trigger revalidation — it will try to SET NX but fail because
    // worker B holds the lock. This means the revalidation is skipped entirely.
    let (_, _) = caching.list_query_runs(100, None).await.unwrap();

    // Give any spawned task a moment to attempt the lock
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Worker B's lock should still be intact
    let lock_val: Option<String> = redis::cmd("GET")
        .arg(lock_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        lock_val.as_deref(),
        Some(worker_b_token),
        "Worker B's lock should be preserved — revalidation should not have deleted it"
    );

    // Now test the direct race: simulate worker A already acquired and is about
    // to release, but worker B stole the lock in between.
    // We do this by manually calling the compare-and-delete script with a stale token.
    let stale_token = "worker-A-stale-token";
    let release_script = r#"
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    "#;
    let result: i32 = redis::cmd("EVAL")
        .arg(release_script)
        .arg(1)
        .arg(lock_key)
        .arg(stale_token)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        result, 0,
        "Compare-and-delete should return 0 (no-op) when token doesn't match"
    );

    // Worker B's lock should STILL be intact
    let lock_val_after: Option<String> = redis::cmd("GET")
        .arg(lock_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        lock_val_after.as_deref(),
        Some(worker_b_token),
        "Worker B's lock must survive a stale worker A release attempt"
    );

    // Verify the correct owner CAN release
    let result_owner: i32 = redis::cmd("EVAL")
        .arg(release_script)
        .arg(1)
        .arg(lock_key)
        .arg(worker_b_token)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        result_owner, 1,
        "Compare-and-delete should return 1 (deleted) when token matches"
    );
    let lock_gone: Option<String> = redis::cmd("GET")
        .arg(lock_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        lock_gone.is_none(),
        "Lock should be deleted after owner releases it"
    );
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
        ttl_secs: 60,
        refresh_interval_secs: 0,
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
