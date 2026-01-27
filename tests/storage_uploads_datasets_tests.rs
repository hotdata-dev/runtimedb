//! Tests for upload and dataset storage operations.

use runtimedb::storage::{FilesystemStorage, StorageManager};
use tempfile::TempDir;

fn create_test_storage() -> (FilesystemStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = FilesystemStorage::new(temp_dir.path().to_str().unwrap());
    (storage, temp_dir)
}

#[test]
fn test_upload_url_format() {
    let (storage, _temp) = create_test_storage();
    let url = storage.upload_url("upld_abc123");
    assert!(url.starts_with("file://"));
    assert!(url.contains("/uploads/upld_abc123/raw"));
}

#[test]
fn test_dataset_url_format() {
    let (storage, _temp) = create_test_storage();
    let url = storage.dataset_url("data_xyz", "v1");
    assert!(url.starts_with("file://"));
    assert!(url.contains("/datasets/data_xyz/v1/data.parquet"));
}

#[tokio::test]
async fn test_upload_write_roundtrip() {
    let (storage, _temp) = create_test_storage();

    let upload_id = "upld_test123";
    let data = b"hello,world\n1,2";

    // Prepare and write
    let local_path = storage.prepare_upload_write(upload_id);
    std::fs::create_dir_all(local_path.parent().unwrap()).unwrap();
    tokio::fs::write(&local_path, data).await.unwrap();

    // Finalize
    let url = storage.finalize_upload_write(upload_id).await.unwrap();
    assert!(url.contains(upload_id));

    // Read back
    let read_data = storage.read(&url).await.unwrap();
    assert_eq!(read_data, data);
}

#[tokio::test]
async fn test_dataset_write_creates_versioned_directory() {
    let (storage, _temp) = create_test_storage();

    let dataset_id = "data_test456";
    let handle = storage.prepare_dataset_write(dataset_id);

    assert!(handle.local_path.to_str().unwrap().contains(dataset_id));
    assert!(!handle.version.is_empty());
    assert_eq!(handle.dataset_id, dataset_id);

    // Write some data
    std::fs::create_dir_all(handle.local_path.parent().unwrap()).unwrap();
    tokio::fs::write(&handle.local_path, b"parquet data here")
        .await
        .unwrap();

    // Finalize
    let url = storage.finalize_dataset_write(&handle).await.unwrap();
    assert!(url.contains(dataset_id));
    assert!(url.contains(&handle.version));
    assert!(url.ends_with("data.parquet"));
}

#[test]
fn test_dataset_write_handle_unique_versions() {
    let (storage, _temp) = create_test_storage();
    let handle1 = storage.prepare_dataset_write("data_test");
    let handle2 = storage.prepare_dataset_write("data_test");
    assert_ne!(
        handle1.version, handle2.version,
        "Versions should be unique"
    );
    assert_ne!(
        handle1.local_path, handle2.local_path,
        "Paths should be unique"
    );
}

#[test]
fn test_dataset_write_handle_structure() {
    let (storage, _temp) = create_test_storage();
    let handle = storage.prepare_dataset_write("data_mydata");
    let path_str = handle.local_path.to_string_lossy();

    assert_eq!(handle.dataset_id, "data_mydata");
    assert!(!handle.version.is_empty(), "Version should not be empty");
    assert!(
        path_str.contains("/datasets/"),
        "Path should contain datasets directory"
    );
    assert!(
        path_str.contains("data_mydata"),
        "Path should contain dataset_id"
    );
    assert!(
        path_str.contains(&handle.version),
        "Path should contain version"
    );
    assert!(
        path_str.ends_with("/data.parquet"),
        "Path should end with /data.parquet"
    );
}
