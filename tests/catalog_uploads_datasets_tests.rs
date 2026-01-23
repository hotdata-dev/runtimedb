//! Tests for upload and dataset catalog operations.

use chrono::Utc;
use runtimedb::catalog::{CatalogManager, DatasetInfo, SqliteCatalogManager, UploadInfo};
use tempfile::TempDir;

async fn create_test_catalog() -> (SqliteCatalogManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    catalog.run_migrations().await.unwrap();
    (catalog, temp_dir)
}

// ============================================================================
// Upload tests
// ============================================================================

#[tokio::test]
async fn test_create_and_get_upload() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let upload = UploadInfo {
        id: "upld_test123".to_string(),
        status: "pending".to_string(),
        storage_url: "s3://bucket/uploads/test.parquet".to_string(),
        content_type: Some("application/octet-stream".to_string()),
        content_encoding: Some("gzip".to_string()),
        size_bytes: 1024,
        created_at: Utc::now(),
        consumed_at: None,
    };

    catalog.create_upload(&upload).await.unwrap();

    let retrieved = catalog.get_upload("upld_test123").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, "upld_test123");
    assert_eq!(retrieved.status, "pending");
    assert_eq!(retrieved.storage_url, "s3://bucket/uploads/test.parquet");
    assert_eq!(
        retrieved.content_type,
        Some("application/octet-stream".to_string())
    );
    assert_eq!(retrieved.content_encoding, Some("gzip".to_string()));
    assert_eq!(retrieved.size_bytes, 1024);
    assert!(retrieved.consumed_at.is_none());
}

#[tokio::test]
async fn test_get_upload_not_found() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let result = catalog.get_upload("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_list_uploads_by_status() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    // Create uploads with different statuses
    let pending1 = UploadInfo {
        id: "upld_pending1".to_string(),
        status: "pending".to_string(),
        storage_url: "s3://bucket/1.parquet".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 100,
        created_at: Utc::now(),
        consumed_at: None,
    };

    let pending2 = UploadInfo {
        id: "upld_pending2".to_string(),
        status: "pending".to_string(),
        storage_url: "s3://bucket/2.parquet".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 200,
        created_at: Utc::now(),
        consumed_at: None,
    };

    let consumed = UploadInfo {
        id: "upld_consumed".to_string(),
        status: "consumed".to_string(),
        storage_url: "s3://bucket/3.parquet".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 300,
        created_at: Utc::now(),
        consumed_at: Some(Utc::now()),
    };

    catalog.create_upload(&pending1).await.unwrap();
    catalog.create_upload(&pending2).await.unwrap();
    catalog.create_upload(&consumed).await.unwrap();

    // List all uploads
    let all = catalog.list_uploads(None).await.unwrap();
    assert_eq!(all.len(), 3);

    // List only pending uploads
    let pending = catalog.list_uploads(Some("pending")).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|u| u.status == "pending"));

    // List only consumed uploads
    let consumed_list = catalog.list_uploads(Some("consumed")).await.unwrap();
    assert_eq!(consumed_list.len(), 1);
    assert_eq!(consumed_list[0].id, "upld_consumed");
}

#[tokio::test]
async fn test_consume_upload() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let upload = UploadInfo {
        id: "upld_to_consume".to_string(),
        status: "pending".to_string(),
        storage_url: "s3://bucket/test.parquet".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 500,
        created_at: Utc::now(),
        consumed_at: None,
    };

    catalog.create_upload(&upload).await.unwrap();

    // Consume the upload
    let consumed = catalog.consume_upload("upld_to_consume").await.unwrap();
    assert!(consumed);

    // Verify status changed
    let retrieved = catalog
        .get_upload("upld_to_consume")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.status, "consumed");
    assert!(retrieved.consumed_at.is_some());

    // Consuming again should return false (already consumed)
    let consumed_again = catalog.consume_upload("upld_to_consume").await.unwrap();
    assert!(!consumed_again);

    // Consuming nonexistent upload returns false
    let not_found = catalog.consume_upload("nonexistent").await.unwrap();
    assert!(!not_found);
}

// ============================================================================
// Dataset tests
// ============================================================================

#[tokio::test]
async fn test_create_and_get_dataset() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let dataset = DatasetInfo {
        id: "ds_test123".to_string(),
        label: "Test Dataset".to_string(),
        schema_name: "default".to_string(),
        table_name: "test_table".to_string(),
        parquet_url: "s3://bucket/datasets/test.parquet".to_string(),
        arrow_schema_json: r#"{"fields":[]}"#.to_string(),
        source_type: "csv_upload".to_string(),
        source_config: r#"{"upload_id":"upld_123"}"#.to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    catalog.create_dataset(&dataset).await.unwrap();

    let retrieved = catalog.get_dataset("ds_test123").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, "ds_test123");
    assert_eq!(retrieved.label, "Test Dataset");
    assert_eq!(retrieved.schema_name, "default");
    assert_eq!(retrieved.table_name, "test_table");
    assert_eq!(retrieved.parquet_url, "s3://bucket/datasets/test.parquet");
    assert_eq!(retrieved.source_type, "csv_upload");
}

#[tokio::test]
async fn test_get_dataset_by_table_name() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let dataset = DatasetInfo {
        id: "ds_lookup".to_string(),
        label: "Lookup Dataset".to_string(),
        schema_name: "default".to_string(),
        table_name: "my_unique_table".to_string(),
        parquet_url: "s3://bucket/datasets/lookup.parquet".to_string(),
        arrow_schema_json: r#"{"fields":[]}"#.to_string(),
        source_type: "csv_upload".to_string(),
        source_config: "{}".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    catalog.create_dataset(&dataset).await.unwrap();

    let found = catalog
        .get_dataset_by_table_name("default", "my_unique_table")
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, "ds_lookup");

    // Not found case
    let not_found = catalog
        .get_dataset_by_table_name("default", "nonexistent")
        .await
        .unwrap();
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_dataset_table_name_uniqueness() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let dataset1 = DatasetInfo {
        id: "ds_first".to_string(),
        label: "First Dataset".to_string(),
        schema_name: "default".to_string(),
        table_name: "unique_name".to_string(),
        parquet_url: "s3://bucket/1.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "csv_upload".to_string(),
        source_config: "{}".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let dataset2 = DatasetInfo {
        id: "ds_second".to_string(),
        label: "Second Dataset".to_string(),
        schema_name: "default".to_string(),
        table_name: "unique_name".to_string(), // Same table name!
        parquet_url: "s3://bucket/2.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "csv_upload".to_string(),
        source_config: "{}".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    catalog.create_dataset(&dataset1).await.unwrap();

    // Second create should fail due to unique constraint on (schema_name, table_name)
    let result = catalog.create_dataset(&dataset2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_dataset() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let dataset = DatasetInfo {
        id: "ds_update".to_string(),
        label: "Original Label".to_string(),
        schema_name: "default".to_string(),
        table_name: "original_table".to_string(),
        parquet_url: "s3://bucket/test.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "csv_upload".to_string(),
        source_config: "{}".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    catalog.create_dataset(&dataset).await.unwrap();

    // Update label and table_name
    let updated = catalog
        .update_dataset("ds_update", "New Label", "new_table")
        .await
        .unwrap();
    assert!(updated);

    // Verify changes
    let retrieved = catalog.get_dataset("ds_update").await.unwrap().unwrap();
    assert_eq!(retrieved.label, "New Label");
    assert_eq!(retrieved.table_name, "new_table");

    // Update nonexistent dataset returns false
    let not_found = catalog
        .update_dataset("nonexistent", "Label", "table")
        .await
        .unwrap();
    assert!(!not_found);
}

#[tokio::test]
async fn test_delete_dataset() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    let dataset = DatasetInfo {
        id: "ds_delete".to_string(),
        label: "To Delete".to_string(),
        schema_name: "default".to_string(),
        table_name: "delete_me".to_string(),
        parquet_url: "s3://bucket/delete.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "csv_upload".to_string(),
        source_config: "{}".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    catalog.create_dataset(&dataset).await.unwrap();

    // Delete returns the deleted dataset
    let deleted = catalog.delete_dataset("ds_delete").await.unwrap();
    assert!(deleted.is_some());
    assert_eq!(deleted.unwrap().id, "ds_delete");

    // Verify it's gone
    let retrieved = catalog.get_dataset("ds_delete").await.unwrap();
    assert!(retrieved.is_none());

    // Delete nonexistent returns None
    let not_found = catalog.delete_dataset("nonexistent").await.unwrap();
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_list_datasets() {
    let (catalog, _temp_dir) = create_test_catalog().await;

    // Create several datasets
    for i in 1..=3 {
        let dataset = DatasetInfo {
            id: format!("ds_{}", i),
            label: format!("Dataset {}", i),
            schema_name: "default".to_string(),
            table_name: format!("table_{}", i),
            parquet_url: format!("s3://bucket/{}.parquet", i),
            arrow_schema_json: "{}".to_string(),
            source_type: "csv_upload".to_string(),
            source_config: "{}".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        catalog.create_dataset(&dataset).await.unwrap();
    }

    let datasets = catalog.list_datasets().await.unwrap();
    assert_eq!(datasets.len(), 3);
}
