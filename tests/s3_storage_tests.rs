//! S3 storage integration tests using MinIO
//!
//! These tests require MinIO running locally:
//! docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
//!
//! Or via docker-compose from the monopoly root directory.
//!
//! Run these tests with: cargo test --test s3_storage_tests -- --ignored

use rivetdb::storage::{S3Credentials, S3Storage, StorageManager};

#[tokio::test]
#[ignore] // Run with --ignored flag when MinIO is available
async fn s3_storage_write_read_delete() {
    // Set up MinIO credentials
    std::env::set_var("AWS_ACCESS_KEY_ID", "root");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "password");
    std::env::set_var("AWS_ENDPOINT", "http://localhost:9000");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let storage = S3Storage::new("test-bucket").unwrap();

    let url = storage.cache_url(1, "public", "users");
    let data = b"test data";

    // Write
    storage.write(&url, data).await.unwrap();

    // Exists
    assert!(storage.exists(&url).await.unwrap());

    // Read
    let read_data = storage.read(&url).await.unwrap();
    assert_eq!(read_data, data);

    // Delete
    storage.delete(&url).await.unwrap();
    assert!(!storage.exists(&url).await.unwrap());
}

#[tokio::test]
#[ignore] // Run with --ignored flag when MinIO is available
async fn s3_storage_delete_prefix() {
    // Set up MinIO credentials
    std::env::set_var("AWS_ACCESS_KEY_ID", "root");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "password");
    std::env::set_var("AWS_ENDPOINT", "http://localhost:9000");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let storage = S3Storage::new("test-bucket").unwrap();

    // Write multiple files under same prefix
    let url1 = storage.cache_url(1, "public", "users");
    let url2 = storage.cache_url(1, "public", "orders");

    storage.write(&url1, b"users").await.unwrap();
    storage.write(&url2, b"orders").await.unwrap();

    // Verify both exist
    assert!(storage.exists(&url1).await.unwrap());
    assert!(storage.exists(&url2).await.unwrap());

    // Delete entire connection prefix
    let prefix = storage.cache_prefix(1);
    storage.delete_prefix(&prefix).await.unwrap();

    // Verify both are deleted
    assert!(!storage.exists(&url1).await.unwrap());
    assert!(!storage.exists(&url2).await.unwrap());
}

#[tokio::test]
#[ignore] // Run with --ignored flag when MinIO is available
async fn s3_storage_path_construction() {
    // Set up MinIO credentials
    std::env::set_var("AWS_ACCESS_KEY_ID", "root");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "password");
    std::env::set_var("AWS_ENDPOINT", "http://localhost:9000");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let storage = S3Storage::new("test-bucket").unwrap();

    // Test cache URL construction (directory path for DLT's multiple parquet files)
    let cache_url = storage.cache_url(1, "public", "users");
    assert_eq!(cache_url, "s3://test-bucket/cache/1/public/users");

    // Test cache prefix construction
    let cache_prefix = storage.cache_prefix(1);
    assert_eq!(cache_prefix, "s3://test-bucket/cache/1");
}

/// Test that S3Storage created with new_with_config returns credentials via get_s3_credentials
#[test]
fn s3_storage_get_credentials_with_config() {
    let storage = S3Storage::new_with_config(
        "test-bucket",
        "http://localhost:9000",
        "test-access-key",
        "test-secret-key",
        true,
    )
    .unwrap();

    let creds = storage.get_s3_credentials();
    assert!(
        creds.is_some(),
        "Credentials should be present when created with new_with_config"
    );

    let creds = creds.unwrap();
    assert_eq!(creds.aws_access_key_id, "test-access-key");
    assert_eq!(creds.aws_secret_access_key, "test-secret-key");
    assert_eq!(creds.endpoint_url, "http://localhost:9000");
}

/// Test that S3Credentials serializes to JSON with correct field names
#[test]
fn s3_credentials_serializes_for_dlt() {
    let creds = S3Credentials {
        aws_access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        endpoint_url: "https://sfo3.digitaloceanspaces.com".to_string(),
    };

    let json = serde_json::to_value(&creds).unwrap();

    assert_eq!(json["aws_access_key_id"], "AKIAIOSFODNN7EXAMPLE");
    assert_eq!(
        json["aws_secret_access_key"],
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    );
    assert_eq!(json["endpoint_url"], "https://sfo3.digitaloceanspaces.com");
}
