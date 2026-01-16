use runtimedb::storage::{FilesystemStorage, StorageManager};
use tempfile::TempDir;

#[test]
fn filesystem_cache_url_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let url = storage.cache_url(1, "public", "users");
    assert!(url.starts_with("file://"));
    // cache_url now returns directory path (DLT creates <table>/*.parquet files)
    assert!(url.contains("/cache/1/public/users"));
    assert!(!url.ends_with(".parquet"));
}

#[test]
fn filesystem_cache_prefix_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let prefix = storage.cache_prefix(1);
    assert!(prefix.ends_with("/cache/1"));
}

#[tokio::test]
async fn filesystem_write_and_read_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let url = storage.cache_url(1, "public", "users");
    let data = b"test data";

    storage.write(&url, data).await.unwrap();
    let read_data = storage.read(&url).await.unwrap();

    assert_eq!(read_data, data);
}

#[tokio::test]
async fn filesystem_exists_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let url = storage.cache_url(1, "public", "users");

    assert!(!storage.exists(&url).await.unwrap());

    storage.write(&url, b"test").await.unwrap();

    assert!(storage.exists(&url).await.unwrap());
}

#[tokio::test]
async fn filesystem_delete_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let url = storage.cache_url(1, "public", "users");

    storage.write(&url, b"test").await.unwrap();
    assert!(storage.exists(&url).await.unwrap());

    storage.delete(&url).await.unwrap();
    assert!(!storage.exists(&url).await.unwrap());
}

#[tokio::test]
async fn filesystem_delete_prefix_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    // Write multiple files under same prefix
    let url1 = storage.cache_url(1, "public", "users");
    let url2 = storage.cache_url(1, "public", "orders");

    storage.write(&url1, b"users").await.unwrap();
    storage.write(&url2, b"orders").await.unwrap();

    // Delete entire connection prefix
    let prefix = storage.cache_prefix(1);
    storage.delete_prefix(&prefix).await.unwrap();

    assert!(!storage.exists(&url1).await.unwrap());
    assert!(!storage.exists(&url2).await.unwrap());
}

#[test]
fn test_filesystem_prepare_cache_write_returns_handle() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    let handle = storage.prepare_cache_write(1, "public", "users");

    // Verify handle fields
    assert_eq!(handle.connection_id, 1);
    assert_eq!(handle.schema, "public");
    assert_eq!(handle.table, "users");
    assert!(!handle.version.is_empty());

    // Verify path structure: {cache_base}/{conn_id}/{schema}/{table}/{version}/data.parquet
    let path_str = handle.local_path.to_str().unwrap();
    assert!(path_str.contains("/cache/1/public/users/"));
    assert!(path_str.contains(&handle.version));
    assert!(path_str.ends_with("/data.parquet"));
}

#[tokio::test]
async fn test_filesystem_finalize_cache_write_returns_url() {
    use std::fs;

    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    // Prepare write handle
    let handle = storage.prepare_cache_write(1, "public", "users");

    // Create parent directories and write a test file
    fs::create_dir_all(handle.local_path.parent().unwrap()).unwrap();
    fs::write(&handle.local_path, b"test data").unwrap();

    // Finalize should return the versioned directory URL
    let url = storage.finalize_cache_write(&handle).await.unwrap();

    // Should return versioned directory URL (for ListingTable compatibility)
    assert!(url.starts_with("file://"));
    assert!(url.contains("/cache/1/public/users/"));
    assert!(url.contains(&handle.version));
    assert!(!url.ends_with(".parquet")); // Should be directory, not file
}

#[tokio::test]
async fn test_filesystem_prepare_and_finalize_path_consistency() {
    use std::fs;

    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");

    let storage = FilesystemStorage::new(cache_base.to_str().unwrap());

    // Prepare write handle
    let handle = storage.prepare_cache_write(1, "public", "orders");

    // Create parent directories and write a test file
    fs::create_dir_all(handle.local_path.parent().unwrap()).unwrap();
    fs::write(&handle.local_path, b"test data").unwrap();

    // Finalize and get URL
    let url = storage.finalize_cache_write(&handle).await.unwrap();

    // The URL should point to the version directory (parent of the parquet file)
    let url_path = url.strip_prefix("file://").unwrap();
    let version_dir = handle.local_path.parent().unwrap();

    assert_eq!(url_path, version_dir.to_str().unwrap());
    assert!(handle.local_path.exists());
}
