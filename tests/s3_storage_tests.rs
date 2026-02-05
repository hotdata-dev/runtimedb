//! S3 storage integration tests using MinIO via testcontainers.
//!
//! These tests automatically start a MinIO container, no manual setup required.
//!
//! Run these tests with: cargo test --test s3_storage_tests

use runtimedb::storage::{S3Storage, StorageManager};
use std::process::Command;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::minio::MinIO;

/// MinIO default credentials
const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";
const MINIO_BUCKET: &str = "test-bucket";
const MINIO_REGION: &str = "us-east-1";

/// Create a bucket in MinIO using the mc (MinIO Client) via Docker
async fn create_minio_bucket(endpoint: &str, bucket: &str) {
    // MC_HOST format: http(s)://<ACCESS_KEY>:<SECRET_KEY>@<HOST>:<PORT>
    let mc_host = format!(
        "http://{}:{}@{}",
        MINIO_ROOT_USER,
        MINIO_ROOT_PASSWORD,
        endpoint.trim_start_matches("http://")
    );

    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network=host",
            "-e",
            &format!("MC_HOST_minio={}", mc_host),
            "minio/mc",
            "mb",
            "--ignore-existing",
            &format!("minio/{}", bucket),
        ])
        .output()
        .expect("Failed to run docker mc command - is Docker running?");

    if !output.status.success() {
        eprintln!(
            "Warning: mc mb command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Give MinIO time to process
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Test infrastructure that manages MinIO container lifecycle
struct MinioTestInfra {
    #[allow(dead_code)]
    minio: ContainerAsync<MinIO>,
    storage: S3Storage,
}

impl MinioTestInfra {
    async fn start() -> Self {
        let minio = MinIO::default()
            .start()
            .await
            .expect("Failed to start MinIO");

        let minio_host_port = minio.get_host_port_ipv4(9000).await.unwrap();
        let minio_host = minio.get_host().await.unwrap();
        let minio_endpoint = format!("http://{}:{}", minio_host, minio_host_port);

        // Create the test bucket
        create_minio_bucket(&minio_endpoint, MINIO_BUCKET).await;

        let storage = S3Storage::new_with_endpoint(
            MINIO_BUCKET,
            &minio_endpoint,
            MINIO_ROOT_USER,
            MINIO_ROOT_PASSWORD,
            MINIO_REGION,
            true,
            false,
        )
        .expect("Failed to create S3Storage");

        Self { minio, storage }
    }
}

#[tokio::test]
async fn s3_storage_write_read_delete() {
    let infra = MinioTestInfra::start().await;
    let storage = &infra.storage;

    // Use a specific file path (not directory) for this test
    let url = format!("s3://{}/cache/1/public/users/test.parquet", MINIO_BUCKET);
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
async fn s3_storage_delete_prefix() {
    let infra = MinioTestInfra::start().await;
    let storage = &infra.storage;

    // Write multiple files under same prefix
    let url1 = format!("s3://{}/cache/2/public/users/data.parquet", MINIO_BUCKET);
    let url2 = format!("s3://{}/cache/2/public/orders/data.parquet", MINIO_BUCKET);

    storage.write(&url1, b"users").await.unwrap();
    storage.write(&url2, b"orders").await.unwrap();

    // Verify both exist
    assert!(storage.exists(&url1).await.unwrap());
    assert!(storage.exists(&url2).await.unwrap());

    // Delete entire connection prefix
    let prefix = storage.cache_prefix("2");
    storage.delete_prefix(&prefix).await.unwrap();

    // Verify both are deleted
    assert!(!storage.exists(&url1).await.unwrap());
    assert!(!storage.exists(&url2).await.unwrap());
}

#[tokio::test]
async fn s3_storage_path_construction() {
    let infra = MinioTestInfra::start().await;
    let storage = &infra.storage;

    // Test cache URL construction (directory path for DLT's multiple parquet files)
    let cache_url = storage.cache_url("1", "public", "users");
    assert_eq!(
        cache_url,
        format!("s3://{}/cache/1/public/users", MINIO_BUCKET)
    );

    // Test cache prefix construction
    let cache_prefix = storage.cache_prefix("1");
    assert_eq!(cache_prefix, format!("s3://{}/cache/1", MINIO_BUCKET));
}

/// Test that delete_prefix removes versioned directory contents.
/// finalize_cache_write returns directory URLs like s3://bucket/.../version
/// and delete_prefix should delete the data.parquet file inside.
#[tokio::test]
async fn s3_storage_delete_prefix_removes_versioned_directory() {
    let infra = MinioTestInfra::start().await;
    let storage = &infra.storage;

    // Use prepare/finalize to create a versioned cache entry
    let handle = storage.prepare_cache_write("99", "test_schema", "test_table");

    // Write the parquet file locally
    std::fs::create_dir_all(handle.local_path.parent().unwrap()).unwrap();
    std::fs::write(&handle.local_path, b"test parquet data").unwrap();

    // Finalize uploads to S3 and returns the directory URL
    let versioned_dir_url = storage.finalize_cache_write(&handle).await.unwrap();

    // The URL should be a directory like s3://test-bucket/cache/99/test_schema/test_table/{version}
    assert!(
        !versioned_dir_url.ends_with(".parquet"),
        "URL should be a directory, not a file: {}",
        versioned_dir_url
    );

    // Verify the data.parquet file exists inside
    let file_url = format!("{}/data.parquet", versioned_dir_url);
    assert!(
        storage.exists(&file_url).await.unwrap(),
        "data.parquet should exist at {}",
        file_url
    );

    // Use delete_prefix to remove the versioned directory (this is what the deletion worker uses)
    storage.delete_prefix(&versioned_dir_url).await.unwrap();

    // The data.parquet file should be gone
    assert!(
        !storage.exists(&file_url).await.unwrap(),
        "data.parquet should be deleted when using delete_prefix on directory URL"
    );
}
