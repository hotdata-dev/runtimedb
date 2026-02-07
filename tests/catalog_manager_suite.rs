use runtimedb::catalog::{
    CatalogManager, CreateQueryRun, PostgresCatalogManager, QueryRunCursor, QueryRunStatus,
    QueryRunUpdate, SqliteCatalogManager,
};
use runtimedb::datasets::DEFAULT_SCHEMA;
use sqlx::{PgPool, SqlitePool};
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

struct CatalogTestContext<M, G = ()> {
    manager: M,
    _guard: G,
}

impl<M, G> CatalogTestContext<M, G> {
    fn new(manager: M, guard: G) -> Self {
        Self {
            manager,
            _guard: guard,
        }
    }
}

impl<M: CatalogManager, G> CatalogTestContext<M, G> {
    fn manager(&self) -> &M {
        &self.manager
    }
}

async fn create_sqlite_catalog() -> CatalogTestContext<SqliteCatalogManager, TempDir> {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db_path = dir.path().join("catalog.sqlite");
    let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    manager.run_migrations().await.unwrap();
    CatalogTestContext::new(manager, dir)
}

async fn create_postgres_catalog(
) -> CatalogTestContext<PostgresCatalogManager, testcontainers::ContainerAsync<Postgres>> {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let connection_string = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        host_port
    );

    let manager = PostgresCatalogManager::new(&connection_string)
        .await
        .unwrap();
    manager.run_migrations().await.unwrap();

    CatalogTestContext::new(manager, container)
}

macro_rules! catalog_manager_tests {
    ($module:ident, $setup_fn:ident) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn catalog_initialization() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let connections = catalog.list_connections().await.unwrap();
                assert!(connections.is_empty());
            }

            #[tokio::test]
            async fn add_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;

                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();

                let connections = catalog.list_connections().await.unwrap();
                assert_eq!(connections.len(), 1);
                assert_eq!(connections[0].name, "test_db");
            }

            #[tokio::test]
            async fn get_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();

                let conn = catalog.get_connection_by_name("test_db").await.unwrap();
                assert!(conn.is_some());
                assert_eq!(conn.unwrap().name, "test_db");

                let missing = catalog.get_connection_by_name("missing").await.unwrap();
                assert!(missing.is_none());
            }

            #[tokio::test]
            async fn add_table() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;

                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();

                let first_id = catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();
                let second_id = catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();
                assert_eq!(first_id, second_id);
            }

            #[tokio::test]
            async fn get_table() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();

                let table = catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(table.schema_name, "public");
                assert_eq!(table.table_name, "users");

                let missing = catalog
                    .get_table(&conn.id, "public", "missing")
                    .await
                    .unwrap();
                assert!(missing.is_none());
            }

            #[tokio::test]
            async fn update_table_sync() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                let table_id = catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();

                catalog
                    .update_table_sync(table_id, "/path/to/data.parquet")
                    .await
                    .unwrap();

                let table = catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(table.parquet_path.as_deref(), Some("/path/to/data.parquet"));
                assert!(table.last_sync.is_some());
            }

            #[tokio::test]
            async fn list_tables_multiple_connections() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config1 = r#"{"host": "localhost", "port": 5432, "database": "db1"}"#;
                catalog
                    .add_connection("neon_east", "postgres", config1, None)
                    .await
                    .unwrap();
                let conn1 = catalog.get_connection_by_name("neon_east").await.unwrap().unwrap();
                catalog
                    .add_table(&conn1.id, "public", "cities", "")
                    .await
                    .unwrap();
                catalog
                    .add_table(&conn1.id, "public", "locations", "")
                    .await
                    .unwrap();
                catalog
                    .add_table(&conn1.id, "public", "table_1", "")
                    .await
                    .unwrap();

                let config2 = r#"{"host": "localhost", "port": 5432, "database": "db2"}"#;
                catalog
                    .add_connection("connection2", "postgres", config2, None)
                    .await
                    .unwrap();
                let conn2 = catalog
                    .get_connection_by_name("connection2")
                    .await
                    .unwrap()
                    .unwrap();
                catalog
                    .add_table(&conn2.id, "public", "table_1", "")
                    .await
                    .unwrap();

                let all_tables = catalog.list_tables(None).await.unwrap();
                assert_eq!(all_tables.len(), 4);

                let conn1_tables = catalog.list_tables(Some(&conn1.id)).await.unwrap();
                assert_eq!(conn1_tables.len(), 3);
                assert!(conn1_tables.iter().all(|t| t.connection_id == conn1.id));

                let conn2_tables = catalog.list_tables(Some(&conn2.id)).await.unwrap();
                assert_eq!(conn2_tables.len(), 1);
                assert!(conn2_tables.iter().all(|t| t.connection_id == conn2.id));
            }

            #[tokio::test]
            async fn list_tables_with_cached_status() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                let cached_id = catalog
                    .add_table(&conn.id, "public", "cached_table", "")
                    .await
                    .unwrap();
                catalog
                    .add_table(&conn.id, "public", "not_cached_table", "")
                    .await
                    .unwrap();

                catalog
                    .update_table_sync(cached_id, "/fake/path/test.parquet")
                    .await
                    .unwrap();

                let tables = catalog.list_tables(Some(&conn.id)).await.unwrap();
                assert_eq!(tables.len(), 2);

                let cached = tables
                    .iter()
                    .find(|t| t.table_name == "cached_table")
                    .unwrap();
                let not_cached = tables
                    .iter()
                    .find(|t| t.table_name == "not_cached_table")
                    .unwrap();

                assert!(cached.parquet_path.is_some());
                assert!(cached.last_sync.is_some());

                assert!(not_cached.parquet_path.is_none());
                assert!(not_cached.last_sync.is_none());
            }

            #[tokio::test]
            async fn clear_connection_cache_metadata() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                let table_id = catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();

                catalog
                    .update_table_sync(table_id, "/fake/path/test.parquet")
                    .await
                    .unwrap();

                catalog
                    .clear_connection_cache_metadata(&conn.id)
                    .await
                    .unwrap();
                let table = catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(table.parquet_path.is_none());
                assert!(table.last_sync.is_none());
            }

            #[tokio::test]
            async fn delete_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();

                assert!(catalog.get_connection_by_name("test_db").await.unwrap().is_some());
                assert!(catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .is_some());

                catalog.delete_connection(&conn.id).await.unwrap();

                assert!(catalog.get_connection_by_name("test_db").await.unwrap().is_none());
                assert!(catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .is_none());
            }

            #[tokio::test]
            async fn clear_nonexistent_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let err = catalog.clear_connection_cache_metadata("missing").await;
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test]
            async fn delete_nonexistent_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let err = catalog.delete_connection("missing").await;
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test]
            async fn clear_table_cache_metadata() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                let users_id = catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();
                let orders_id = catalog
                    .add_table(&conn.id, "public", "orders", "")
                    .await
                    .unwrap();

                catalog
                    .update_table_sync(users_id, "/fake/users.parquet")
                    .await
                    .unwrap();
                catalog
                    .update_table_sync(orders_id, "/fake/orders.parquet")
                    .await
                    .unwrap();

                let table_info = catalog
                    .clear_table_cache_metadata(&conn.id, "public", "users")
                    .await
                    .unwrap();
                assert!(table_info.parquet_path.is_some());

                let users_after = catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(users_after.parquet_path.is_none());
                assert!(users_after.last_sync.is_none());

                let orders_after = catalog
                    .get_table(&conn.id, "public", "orders")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(orders_after.parquet_path.is_some());
            }

            #[tokio::test]
            async fn clear_table_cache_metadata_nonexistent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();

                let err = catalog
                    .clear_table_cache_metadata(&conn.id, "public", "missing")
                    .await;
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test]
            async fn clear_table_without_cache() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config, None)
                    .await
                    .unwrap();
                let conn = catalog.get_connection_by_name("test_db").await.unwrap().unwrap();
                catalog
                    .add_table(&conn.id, "public", "users", "")
                    .await
                    .unwrap();

                let table_info = catalog
                    .clear_table_cache_metadata(&conn.id, "public", "users")
                    .await
                    .unwrap();
                assert!(table_info.parquet_path.is_none());

                let table_after = catalog
                    .get_table(&conn.id, "public", "users")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(table_after.parquet_path.is_none());
                assert!(table_after.last_sync.is_none());
            }

            #[tokio::test]
            async fn close_is_idempotent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                catalog.close().await.unwrap();
                catalog.close().await.unwrap();
                catalog.close().await.unwrap();
            }

            #[tokio::test]
            async fn create_secret_metadata_duplicate_fails() {
                use runtimedb::secrets::{SecretMetadata, SecretStatus};

                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let now = chrono::Utc::now();

                let metadata = SecretMetadata {
                    id: "secr_test123456789012345678".to_string(),
                    name: "my-secret".to_string(),
                    provider: "encrypted".to_string(),
                    provider_ref: None,
                    status: SecretStatus::Active,
                    created_at: now,
                    updated_at: now,
                };

                // First create should succeed
                catalog.create_secret_metadata(&metadata).await.unwrap();

                // Second create with same name should fail (unique constraint)
                let result = catalog.create_secret_metadata(&metadata).await;

                assert!(result.is_err());
            }

            #[tokio::test]
            async fn dataset_create_duplicate_table_name_detected() {
                use runtimedb::catalog::{is_dataset_table_name_conflict, DatasetInfo};

                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let now = chrono::Utc::now();

                let ds1 = DatasetInfo {
                    id: "ds_first".to_string(),
                    label: "First Dataset".to_string(),
                    schema_name: DEFAULT_SCHEMA.to_string(),
                    table_name: "my_table".to_string(),
                    parquet_url: "s3://bucket/ds1.parquet".to_string(),
                    arrow_schema_json: "{}".to_string(),
                    source_type: "upload".to_string(),
                    source_config: "{}".to_string(),
                    created_at: now,
                    updated_at: now,
                };

                // First create should succeed
                catalog.create_dataset(&ds1).await.unwrap();

                // Second create with same table_name should fail
                let ds2 = DatasetInfo {
                    id: "ds_second".to_string(),
                    label: "Second Dataset".to_string(),
                    schema_name: DEFAULT_SCHEMA.to_string(),
                    table_name: "my_table".to_string(), // same table_name
                    parquet_url: "s3://bucket/ds2.parquet".to_string(),
                    arrow_schema_json: "{}".to_string(),
                    source_type: "upload".to_string(),
                    source_config: "{}".to_string(),
                    created_at: now,
                    updated_at: now,
                };

                let result = catalog.create_dataset(&ds2).await;
                assert!(result.is_err());

                // Verify structured error detection works
                let err = result.unwrap_err();
                let is_conflict = is_dataset_table_name_conflict(
                    catalog,
                    &err,
                    DEFAULT_SCHEMA,
                    "my_table",
                    None, // creating new dataset
                )
                .await;
                assert!(
                    is_conflict,
                    "Expected is_dataset_table_name_conflict to return true for: {:?}",
                    err
                );
            }

            #[tokio::test]
            async fn dataset_update_duplicate_table_name_detected() {
                use runtimedb::catalog::{is_dataset_table_name_conflict, DatasetInfo};

                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let now = chrono::Utc::now();

                // Create two datasets with different table names
                let ds1 = DatasetInfo {
                    id: "ds_first".to_string(),
                    label: "First Dataset".to_string(),
                    schema_name: DEFAULT_SCHEMA.to_string(),
                    table_name: "table_one".to_string(),
                    parquet_url: "s3://bucket/ds1.parquet".to_string(),
                    arrow_schema_json: "{}".to_string(),
                    source_type: "upload".to_string(),
                    source_config: "{}".to_string(),
                    created_at: now,
                    updated_at: now,
                };

                let ds2 = DatasetInfo {
                    id: "ds_second".to_string(),
                    label: "Second Dataset".to_string(),
                    schema_name: DEFAULT_SCHEMA.to_string(),
                    table_name: "table_two".to_string(),
                    parquet_url: "s3://bucket/ds2.parquet".to_string(),
                    arrow_schema_json: "{}".to_string(),
                    source_type: "upload".to_string(),
                    source_config: "{}".to_string(),
                    created_at: now,
                    updated_at: now,
                };

                catalog.create_dataset(&ds1).await.unwrap();
                catalog.create_dataset(&ds2).await.unwrap();

                // Try to update ds2's table_name to conflict with ds1
                let result = catalog
                    .update_dataset("ds_second", "Second Dataset", "table_one")
                    .await;
                assert!(result.is_err());

                // Verify structured error detection works
                let err = result.unwrap_err();
                let is_conflict = is_dataset_table_name_conflict(
                    catalog,
                    &err,
                    DEFAULT_SCHEMA,
                    "table_one",
                    Some("ds_second"), // exclude current dataset from conflict check
                )
                .await;
                assert!(
                    is_conflict,
                    "Expected is_dataset_table_name_conflict to return true for: {:?}",
                    err
                );
            }

            #[tokio::test]
            async fn dataset_other_errors_not_misclassified() {
                use runtimedb::catalog::is_dataset_table_name_conflict;

                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                // Try to update a non-existent dataset - this returns Ok(false)
                // rather than an error, so verify it doesn't panic
                let _result = catalog
                    .update_dataset("ds_nonexistent", "Some Label", "some_table")
                    .await;

                // Test get_dataset with a non-existent ID
                let result = catalog.get_dataset("ds_nonexistent").await;

                // This should succeed with None, not error
                assert!(result.is_ok());
                assert!(result.unwrap().is_none());

                // Create a generic anyhow error and verify it's not misclassified
                let generic_err = anyhow::anyhow!("some random error with unique word in it");
                let is_conflict = is_dataset_table_name_conflict(
                    catalog,
                    &generic_err,
                    DEFAULT_SCHEMA,
                    "some_table",
                    None,
                )
                .await;
                assert!(
                    !is_conflict,
                    "Generic error should not be classified as table_name conflict"
                );

                // Create an error with "duplicate" in the message but not from sqlx
                let misleading_err = anyhow::anyhow!("duplicate key violation somewhere");
                let is_conflict = is_dataset_table_name_conflict(
                    catalog,
                    &misleading_err,
                    DEFAULT_SCHEMA,
                    "some_table",
                    None,
                )
                .await;
                assert!(
                    !is_conflict,
                    "Non-sqlx error with 'duplicate' should not be classified as table_name conflict"
                );
            }

            // ───────────────────────────────────────────────────────────
            // Query run history tests
            // ───────────────────────────────────────────────────────────

            #[tokio::test]
            async fn query_run_create_and_get() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let id = runtimedb::id::generate_query_run_id();

                catalog
                    .create_query_run(CreateQueryRun {
                        id: &id,
                        sql_text: "SELECT 1",
                        sql_hash: "abc123",

                        trace_id: Some("trace-000"),
                    })
                    .await
                    .unwrap();

                let run = catalog.get_query_run(&id).await.unwrap().unwrap();
                assert_eq!(run.id, id);
                assert_eq!(run.sql_text, "SELECT 1");
                assert_eq!(run.sql_hash, "abc123");
                assert_eq!(run.trace_id.as_deref(), Some("trace-000"));
                assert_eq!(run.status, QueryRunStatus::Running);
                assert!(run.result_id.is_none());
                assert!(run.completed_at.is_none());
            }

            #[tokio::test]
            async fn query_run_update_succeeded() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let id = runtimedb::id::generate_query_run_id();

                catalog
                    .create_query_run(CreateQueryRun {
                        id: &id,
                        sql_text: "SELECT 42",
                        sql_hash: "def456",

                        trace_id: None,
                    })
                    .await
                    .unwrap();

                let updated = catalog
                    .update_query_run(
                        &id,
                        QueryRunUpdate::Succeeded {
                            result_id: Some("rslt_test"),
                            row_count: 1,
                            execution_time_ms: 50,
                            warning_message: None,
                        },
                    )
                    .await
                    .unwrap();
                assert!(updated);

                let run = catalog.get_query_run(&id).await.unwrap().unwrap();
                assert_eq!(run.status, QueryRunStatus::Succeeded);
                assert_eq!(run.result_id.as_deref(), Some("rslt_test"));
                assert_eq!(run.row_count, Some(1));
                assert_eq!(run.execution_time_ms, Some(50));
                assert!(run.completed_at.is_some());
            }

            #[tokio::test]
            async fn query_run_update_failed() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let id = runtimedb::id::generate_query_run_id();

                catalog
                    .create_query_run(CreateQueryRun {
                        id: &id,
                        sql_text: "SELECT bad",
                        sql_hash: "ghi789",

                        trace_id: None,
                    })
                    .await
                    .unwrap();

                let updated = catalog
                    .update_query_run(
                        &id,
                        QueryRunUpdate::Failed {
                            error_message: "syntax error",
                            execution_time_ms: Some(10),
                        },
                    )
                    .await
                    .unwrap();
                assert!(updated);

                let run = catalog.get_query_run(&id).await.unwrap().unwrap();
                assert_eq!(run.status, QueryRunStatus::Failed);
                assert_eq!(run.error_message.as_deref(), Some("syntax error"));
                assert_eq!(run.execution_time_ms, Some(10));
                assert!(run.completed_at.is_some());
            }

            #[tokio::test]
            async fn query_run_list_pagination() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();


                // Create 5 query runs
                let mut ids = Vec::new();
                for i in 0..5 {
                    let id = runtimedb::id::generate_query_run_id();
                    catalog
                        .create_query_run(CreateQueryRun {
                            id: &id,
                            sql_text: &format!("SELECT {}", i),
                            sql_hash: &format!("hash{}", i),

                            trace_id: None,
                        })
                        .await
                        .unwrap();
                    ids.push(id);
                    // Small delay to ensure distinct created_at timestamps
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }

                // First page: limit 2, no cursor
                let (page1, has_more) = catalog.list_query_runs(2, None).await.unwrap();
                assert_eq!(page1.len(), 2);
                assert!(has_more);
                // Newest first
                assert_eq!(page1[0].id, ids[4]);
                assert_eq!(page1[1].id, ids[3]);

                // Second page using cursor from last item of page 1
                let cursor = QueryRunCursor {
                    created_at: page1[1].created_at,
                    id: page1[1].id.clone(),
                };
                let (page2, has_more) = catalog.list_query_runs(2, Some(&cursor)).await.unwrap();
                assert_eq!(page2.len(), 2);
                assert!(has_more);
                assert_eq!(page2[0].id, ids[2]);
                assert_eq!(page2[1].id, ids[1]);

                // Third page
                let cursor = QueryRunCursor {
                    created_at: page2[1].created_at,
                    id: page2[1].id.clone(),
                };
                let (page3, has_more) = catalog.list_query_runs(2, Some(&cursor)).await.unwrap();
                assert_eq!(page3.len(), 1);
                assert!(!has_more);
                assert_eq!(page3[0].id, ids[0]);
            }

            #[tokio::test]
            async fn query_run_get_nonexistent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let run = catalog.get_query_run("qrun_nonexistent").await.unwrap();
                assert!(run.is_none());
            }

            #[tokio::test]
            async fn query_run_update_nonexistent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let updated = catalog
                    .update_query_run(
                        "qrun_nonexistent",
                        QueryRunUpdate::Failed {
                            error_message: "test",
                            execution_time_ms: None,
                        },
                    )
                    .await
                    .unwrap();
                assert!(!updated);
            }

        }
    };
}

catalog_manager_tests!(sqlite, create_sqlite_catalog);
catalog_manager_tests!(postgres, create_postgres_catalog);

/// Test that migration hash mismatch is detected on startup.
/// This simulates the scenario where a migration SQL file was modified after
/// being applied to a database.
#[tokio::test]
async fn sqlite_migration_hash_mismatch_fails() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db_path = dir.path().join("catalog.sqlite");
    let db_uri = format!("sqlite:{}?mode=rwc", db_path.display());

    // First: apply migrations normally
    let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    manager.run_migrations().await.unwrap();
    manager.close().await.unwrap();

    // Second: tamper with the stored hash to simulate a modified migration
    let pool = SqlitePool::connect(&db_uri).await.unwrap();
    sqlx::query("UPDATE schema_migrations SET hash = 'tampered_hash' WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Third: try to run migrations again - should fail with hash mismatch
    let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    let result = manager.run_migrations().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("modified after being applied"));
    assert!(err.contains("tampered_hash"));
}

/// Test that unknown migrations in DB (newer than compiled code) are detected.
/// This simulates the scenario where the DB has a migration version that doesn't exist in code.
#[tokio::test]
async fn sqlite_unknown_migration_fails() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db_path = dir.path().join("catalog.sqlite");
    let db_uri = format!("sqlite:{}?mode=rwc", db_path.display());

    // Manually create schema_migrations table with v99 (which doesn't exist in compiled code)
    let pool = SqlitePool::connect(&db_uri).await.unwrap();
    sqlx::query(
        "CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, hash) VALUES (99, 'somehash')")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to run migrations - should fail because v99 doesn't exist in compiled code
    let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    let result = manager.run_migrations().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("v99") && err.contains("only knows up to"),
        "Expected error about unknown migration v99, got: {}",
        err
    );
}

/// Test that migration hash mismatch is detected for PostgreSQL.
#[tokio::test]
async fn postgres_migration_hash_mismatch_fails() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let connection_string = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        host_port
    );

    // First: apply migrations normally
    let manager = PostgresCatalogManager::new(&connection_string)
        .await
        .unwrap();
    manager.run_migrations().await.unwrap();
    manager.close().await.unwrap();

    // Second: tamper with the stored hash to simulate a modified migration
    let pool = PgPool::connect(&connection_string).await.unwrap();
    sqlx::query("UPDATE schema_migrations SET hash = 'tampered_hash' WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Third: try to run migrations again - should fail with hash mismatch
    let manager = PostgresCatalogManager::new(&connection_string)
        .await
        .unwrap();
    let result = manager.run_migrations().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("modified after being applied"));
    assert!(err.contains("tampered_hash"));
}

/// Test that v3 migration correctly migrates data from v1+v2 schema.
/// This simulates upgrading a database that has existing connections and tables.
#[tokio::test]
async fn sqlite_v3_migration_preserves_data() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db_path = dir.path().join("catalog.sqlite");
    let db_uri = format!("sqlite:{}?mode=rwc", db_path.display());

    let pool = SqlitePool::connect(&db_uri).await.unwrap();

    // Create schema_migrations table
    sqlx::query(
        "CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Apply v1 and v2 migrations using actual migration files
    let v1_sql = include_str!("../migrations/sqlite/v1.sql");
    sqlx::raw_sql(v1_sql).execute(&pool).await.unwrap();

    let v2_sql = include_str!("../migrations/sqlite/v2.sql");
    sqlx::raw_sql(v2_sql).execute(&pool).await.unwrap();

    // Record v1 and v2 as applied (use placeholder hashes - won't be checked in this test)
    sqlx::query(
        "INSERT INTO schema_migrations (version, hash) VALUES (1, 'v1hash'), (2, 'v2hash')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test data using the OLD schema (integer connection_id)
    sqlx::query(
        "INSERT INTO connections (id, external_id, name, source_type, config_json)
         VALUES (1, 'conn_test123456789012345678', 'my_postgres', 'postgres', '{\"host\":\"localhost\"}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO connections (id, external_id, name, source_type, config_json)
         VALUES (2, 'conn_other12345678901234567', 'my_mysql', 'mysql', '{\"host\":\"db.example.com\"}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert tables referencing connections by INTEGER id
    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name, parquet_path)
         VALUES (1, 'public', 'users', '/cache/conn1/users.parquet')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name, parquet_path)
         VALUES (1, 'public', 'orders', '/cache/conn1/orders.parquet')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name)
         VALUES (2, 'mydb', 'products')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Apply v3 migration
    let v3_sql = include_str!("../migrations/sqlite/v3.sql");
    sqlx::raw_sql(v3_sql).execute(&pool).await.unwrap();

    // Verify the migration worked correctly

    // Check connections table structure - should now have TEXT id (was external_id)
    let conn: (String, String, String) =
        sqlx::query_as("SELECT id, name, source_type FROM connections WHERE name = 'my_postgres'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        conn.0, "conn_test123456789012345678",
        "Connection id should be the old external_id"
    );
    assert_eq!(conn.1, "my_postgres");
    assert_eq!(conn.2, "postgres");

    // Check that old integer id column is gone and external_id column is gone
    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    let columns: Vec<(i32, String, String, i32, Option<String>, i32)> =
        sqlx::query_as("PRAGMA table_info(connections)")
            .fetch_all(&pool)
            .await
            .unwrap();
    let column_names: Vec<&str> = columns.iter().map(|c| c.1.as_str()).collect();
    assert!(
        column_names.contains(&"id"),
        "connections should have 'id' column"
    );
    assert!(
        !column_names.contains(&"external_id"),
        "connections should not have 'external_id' column"
    );

    // Check tables.connection_id is now TEXT and contains the external_id value
    let table: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT connection_id, schema_name, table_name, parquet_path FROM tables WHERE table_name = 'users'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        table.0, "conn_test123456789012345678",
        "tables.connection_id should be the external_id string"
    );
    assert_eq!(table.1, "public");
    assert_eq!(table.2, "users");
    assert_eq!(table.3, Some("/cache/conn1/users.parquet".to_string()));

    // Verify all tables were migrated correctly
    let table_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tables")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(table_count.0, 3, "All 3 tables should be preserved");

    // Verify foreign key constraint exists using PRAGMA foreign_key_list
    // Columns: id, seq, table, from, to, on_update, on_delete, match
    let fk_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pragma_foreign_key_list('tables') WHERE \"table\" = 'connections' AND \"from\" = 'connection_id' AND \"to\" = 'id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        fk_count.0, 1,
        "FK from tables.connection_id to connections.id should exist"
    );

    // Verify FK is enforced by attempting insert with bogus connection_id
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    let bad_insert = sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name) VALUES ('nonexistent', 'test', 'test')",
    )
    .execute(&pool)
    .await;
    assert!(
        bad_insert.is_err(),
        "Insert with invalid connection_id should fail due to FK constraint"
    );

    // Verify join still works (data integrity check)
    let joined: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.name, t.schema_name, t.table_name
         FROM tables t
         JOIN connections c ON t.connection_id = c.id
         ORDER BY c.name, t.table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(joined.len(), 3);
    assert_eq!(
        joined[0],
        (
            "my_mysql".to_string(),
            "mydb".to_string(),
            "products".to_string()
        )
    );
    assert_eq!(
        joined[1],
        (
            "my_postgres".to_string(),
            "public".to_string(),
            "orders".to_string()
        )
    );
    assert_eq!(
        joined[2],
        (
            "my_postgres".to_string(),
            "public".to_string(),
            "users".to_string()
        )
    );

    pool.close().await;
}

/// Test that v3 migration correctly migrates data from v1+v2 schema for PostgreSQL.
/// This simulates upgrading a database that has existing connections and tables.
#[tokio::test]
async fn postgres_v3_migration_preserves_data() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let connection_string = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        host_port
    );

    let pool = PgPool::connect(&connection_string).await.unwrap();

    // Create schema_migrations table
    sqlx::query(
        "CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Apply v1 and v2 migrations using actual migration files
    let v1_sql = include_str!("../migrations/postgres/v1.sql");
    sqlx::raw_sql(v1_sql).execute(&pool).await.unwrap();

    let v2_sql = include_str!("../migrations/postgres/v2.sql");
    sqlx::raw_sql(v2_sql).execute(&pool).await.unwrap();

    // Record v1 and v2 as applied (use placeholder hashes - won't be checked in this test)
    sqlx::query(
        "INSERT INTO schema_migrations (version, hash) VALUES (1, 'v1hash'), (2, 'v2hash')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test data using the OLD schema (integer connection_id)
    sqlx::query(
        "INSERT INTO connections (id, external_id, name, source_type, config_json)
         VALUES (1, 'conn_test123456789012345678', 'my_postgres', 'postgres', '{\"host\":\"localhost\"}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO connections (id, external_id, name, source_type, config_json)
         VALUES (2, 'conn_other12345678901234567', 'my_mysql', 'mysql', '{\"host\":\"db.example.com\"}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert tables referencing connections by INTEGER id
    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name, parquet_path)
         VALUES (1, 'public', 'users', '/cache/conn1/users.parquet')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name, parquet_path)
         VALUES (1, 'public', 'orders', '/cache/conn1/orders.parquet')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name)
         VALUES (2, 'mydb', 'products')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Apply v3 migration
    let v3_sql = include_str!("../migrations/postgres/v3.sql");
    sqlx::raw_sql(v3_sql).execute(&pool).await.unwrap();

    // Verify the migration worked correctly

    // Check connections table structure - should now have TEXT id (was external_id)
    let conn: (String, String, String) =
        sqlx::query_as("SELECT id, name, source_type FROM connections WHERE name = 'my_postgres'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        conn.0, "conn_test123456789012345678",
        "Connection id should be the old external_id"
    );
    assert_eq!(conn.1, "my_postgres");
    assert_eq!(conn.2, "postgres");

    // Check that old integer id column is gone and external_id column is gone
    let columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'connections'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let column_names: Vec<&str> = columns.iter().map(|c| c.0.as_str()).collect();
    assert!(
        column_names.contains(&"id"),
        "connections should have 'id' column"
    );
    assert!(
        !column_names.contains(&"external_id"),
        "connections should not have 'external_id' column"
    );

    // Check tables.connection_id is now TEXT and contains the external_id value
    let table: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT connection_id, schema_name, table_name, parquet_path FROM tables WHERE table_name = 'users'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        table.0, "conn_test123456789012345678",
        "tables.connection_id should be the external_id string"
    );
    assert_eq!(table.1, "public");
    assert_eq!(table.2, "users");
    assert_eq!(table.3, Some("/cache/conn1/users.parquet".to_string()));

    // Verify all tables were migrated correctly
    let table_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tables")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(table_count.0, 3, "All 3 tables should be preserved");

    // Verify foreign key constraint exists by querying pg_constraint
    let fk_exists: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pg_constraint c
         JOIN pg_class t ON c.conrelid = t.oid
         WHERE t.relname = 'tables'
           AND c.contype = 'f'
           AND c.conname = 'tables_connection_id_fkey'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        fk_exists.0, 1,
        "FK constraint tables_connection_id_fkey should exist"
    );

    // Verify FK is enforced by attempting insert with bogus connection_id
    let bad_insert = sqlx::query(
        "INSERT INTO tables (connection_id, schema_name, table_name) VALUES ('nonexistent', 'test', 'test')",
    )
    .execute(&pool)
    .await;
    assert!(
        bad_insert.is_err(),
        "Insert with invalid connection_id should fail due to FK constraint"
    );

    // Verify join still works (data integrity check)
    let joined: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.name, t.schema_name, t.table_name
         FROM tables t
         JOIN connections c ON t.connection_id = c.id
         ORDER BY c.name, t.table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(joined.len(), 3);
    assert_eq!(
        joined[0],
        (
            "my_mysql".to_string(),
            "mydb".to_string(),
            "products".to_string()
        )
    );
    assert_eq!(
        joined[1],
        (
            "my_postgres".to_string(),
            "public".to_string(),
            "orders".to_string()
        )
    );
    assert_eq!(
        joined[2],
        (
            "my_postgres".to_string(),
            "public".to_string(),
            "users".to_string()
        )
    );

    pool.close().await;
}
