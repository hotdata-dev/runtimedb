use tempfile::TempDir;
use rivetdb::storage::{FilesystemStorage, StorageManager};

#[test]
fn filesystem_cache_url_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let url = storage.cache_url(1, "public", "users");
    assert!(url.starts_with("file://"));
    // cache_url now returns directory path (DLT creates <table>/*.parquet files)
    assert!(url.contains("/cache/1/public/users"));
    assert!(!url.ends_with(".parquet"));
}

#[test]
fn filesystem_state_url_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let url = storage.state_url(1, "public", "users");
    assert!(url.starts_with("file://"));
    assert!(url.contains("/state/1/public/users.json"));
}

#[test]
fn filesystem_cache_prefix_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let prefix = storage.cache_prefix(1);
    assert!(prefix.ends_with("/cache/1"));
}

#[test]
fn filesystem_state_prefix_constructs_correct_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let prefix = storage.state_prefix(1);
    assert!(prefix.ends_with("/state/1"));
}

#[tokio::test]
async fn filesystem_write_and_read_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let url = storage.state_url(1, "public", "users");
    let data = b"test data";

    storage.write(&url, data).await.unwrap();
    let read_data = storage.read(&url).await.unwrap();

    assert_eq!(read_data, data);
}

#[tokio::test]
async fn filesystem_exists_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let url = storage.state_url(1, "public", "users");

    assert!(!storage.exists(&url).await.unwrap());

    storage.write(&url, b"test").await.unwrap();

    assert!(storage.exists(&url).await.unwrap());
}

#[tokio::test]
async fn filesystem_delete_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let url = storage.state_url(1, "public", "users");

    storage.write(&url, b"test").await.unwrap();
    assert!(storage.exists(&url).await.unwrap());

    storage.delete(&url).await.unwrap();
    assert!(!storage.exists(&url).await.unwrap());
}

#[tokio::test]
async fn filesystem_delete_prefix_works() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

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
fn test_filesystem_prepare_cache_write_path() {
    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    let path = storage.prepare_cache_write(1, "public", "users");

    // Verify path structure: {cache_base}/{conn_id}/{schema}/{table}/{table}.parquet
    let path_str = path.to_str().unwrap();
    assert!(path_str.contains("/cache/1/public/users/users.parquet"));
    assert!(path_str.ends_with("users.parquet"));
}

#[tokio::test]
async fn test_filesystem_finalize_cache_write_returns_url() {
    use std::fs;

    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    // Prepare write path
    let write_path = storage.prepare_cache_write(1, "public", "users");

    // Create parent directories and write a test file
    fs::create_dir_all(write_path.parent().unwrap()).unwrap();
    fs::write(&write_path, b"test data").unwrap();

    // Finalize should return the cache_url
    let url = storage
        .finalize_cache_write(&write_path, 1, "public", "users")
        .await
        .unwrap();

    // Should return directory URL (for ListingTable compatibility)
    assert!(url.starts_with("file://"));
    assert!(url.contains("/cache/1/public/users"));
    assert!(!url.ends_with(".parquet")); // Should be directory, not file
}

#[tokio::test]
async fn test_filesystem_prepare_and_finalize_path_consistency() {
    use std::fs;

    let temp = TempDir::new().unwrap();
    let cache_base = temp.path().join("cache");
    let state_base = temp.path().join("state");

    let storage =
        FilesystemStorage::new(cache_base.to_str().unwrap(), state_base.to_str().unwrap());

    // Prepare write path
    let write_path = storage.prepare_cache_write(1, "public", "orders");

    // Create parent directories and write a test file
    fs::create_dir_all(write_path.parent().unwrap()).unwrap();
    fs::write(&write_path, b"test data").unwrap();

    // Finalize and get URL
    let url = storage
        .finalize_cache_write(&write_path, 1, "public", "orders")
        .await
        .unwrap();

    // The file should be inside the directory pointed to by the URL
    let url_path = url.strip_prefix("file://").unwrap();
    let file_parent = write_path.parent().unwrap();

    assert_eq!(url_path, file_parent.to_str().unwrap());
    assert!(write_path.exists());
}
