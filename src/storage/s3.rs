// src/storage/s3.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::warn;
use url::Url;

use super::{CacheWriteHandle, S3Credentials, StorageManager};

#[derive(Debug, Clone)]
struct S3Config {
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: Option<String>,
}

#[derive(Debug)]
pub struct S3Storage {
    bucket: String,
    store: Arc<dyn ObjectStore>,
    config: Option<S3Config>,
}

impl S3Storage {
    pub fn new(bucket: &str) -> Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: None,
        })
    }

    /// Create S3Storage with custom endpoint for MinIO/S3-compatible storage
    pub fn new_with_config(
        bucket: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        region: Option<&str>,
        allow_http: bool,
    ) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http);

        if let Some(region) = region {
            builder = builder.with_region(region);
        }

        // For MinIO, we need to use path-style URLs
        builder = builder.with_virtual_hosted_style_request(false);

        let store = builder.build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: Some(S3Config {
                endpoint: endpoint.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                region: region.map(|s| s.to_string()),
            }),
        })
    }

    fn url_to_path(&self, url: &str) -> Result<ObjectPath> {
        // Convert s3://bucket/path/to/file to ObjectPath
        let url = Url::parse(url)?;
        let path = url.path().trim_start_matches('/');
        Ok(ObjectPath::from(path))
    }
}

#[async_trait]
impl StorageManager for S3Storage {
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        // For S3, return directory path (DLT creates <table>/*.parquet files)
        format!(
            "s3://{}/cache/{}/{}/{}",
            self.bucket, connection_id, schema, table
        )
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        format!("s3://{}/cache/{}", self.bucket, connection_id)
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        let path = self.url_to_path(url)?;
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes.to_vec())
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        let path = self.url_to_path(url)?;
        self.store.put(&path, data.to_vec().into()).await?;
        Ok(())
    }

    async fn delete(&self, url: &str) -> Result<()> {
        let path = self.url_to_path(url)?;
        self.store.delete(&path).await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "delete_cache",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.bucket = %self.bucket,
            runtimedb.prefix = %prefix,
        )
    )]
    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let url = Url::parse(prefix)?;
        let prefix_path = url.path().trim_start_matches('/');
        let prefix_path = ObjectPath::from(prefix_path);

        // List all objects with prefix and delete them
        let list = self.store.list(Some(&prefix_path));
        let objects: Vec<_> = list.try_collect().await?;

        for obj in objects {
            self.store.delete(&obj.location).await?;
        }
        Ok(())
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        let path = self.url_to_path(url)?;
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        let url = Url::parse(&format!("s3://{}", self.bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, self.store.clone());
        Ok(())
    }

    fn get_s3_credentials(&self) -> Option<S3Credentials> {
        self.config.as_ref().map(|c| S3Credentials {
            aws_access_key_id: c.access_key.clone(),
            aws_secret_access_key: c.secret_key.clone(),
            endpoint_url: c.endpoint.clone(),
        })
    }

    fn get_object_store_config(&self) -> Option<(ObjectStoreUrl, HashMap<String, String>)> {
        self.config.as_ref().and_then(|c| {
            let url = ObjectStoreUrl::parse(&format!("s3://{}", self.bucket)).ok()?;
            let mut options = HashMap::new();
            options.insert("aws_access_key_id".to_string(), c.access_key.clone());
            options.insert(
                "aws_secret_access_key".to_string(),
                c.secret_key.clone(),
            );
            options.insert("aws_endpoint".to_string(), c.endpoint.clone());
            options.insert("aws_allow_http".to_string(), "true".to_string());
            if let Some(region) = &c.region {
                options.insert("aws_region".to_string(), region.clone());
            }
            Some((url, options))
        })
    }

    fn prepare_cache_write(
        &self,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> CacheWriteHandle {
        // Use versioned DIRECTORIES to avoid duplicate reads during grace period.
        // Path structure: {temp_dir}/{conn_id}/{schema}/{table}/{version}/data.parquet
        // This matches the S3 destination structure for consistency.
        let version = nanoid::nanoid!(8);
        let local_path = std::env::temp_dir()
            .join(connection_id.to_string())
            .join(schema)
            .join(table)
            .join(&version)
            .join("data.parquet");

        CacheWriteHandle {
            local_path,
            version,
            connection_id,
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    #[tracing::instrument(
        name = "finalize_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.bucket = %self.bucket,
            runtimedb.key = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
    async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String> {
        // Build S3 path using the version from the handle
        let versioned_dir_url = format!(
            "s3://{}/cache/{}/{}/{}/{}",
            self.bucket, handle.connection_id, handle.schema, handle.table, handle.version
        );
        let s3_key = format!(
            "cache/{}/{}/{}/{}/data.parquet",
            handle.connection_id, handle.schema, handle.table, handle.version
        );
        let s3_path = ObjectPath::from(s3_key.as_str());

        // Record the key and cache_url on the current span
        tracing::Span::current()
            .record("runtimedb.key", &s3_key)
            .record("runtimedb.cache_url", &versioned_dir_url);

        // Stream file to S3 using BufWriter to avoid loading entire file into memory.
        // BufWriter adaptively uses put() for small files or put_multipart() for large files.
        let file = tokio::fs::File::open(&handle.local_path).await?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut writer = BufWriter::new(self.store.clone(), s3_path);

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;

        // Clean up local temp file - log warning but don't fail if removal fails
        // since the S3 upload succeeded and that's what matters
        if let Err(e) = tokio::fs::remove_file(&handle.local_path).await {
            warn!(
                path = %handle.local_path.display(),
                error = %e,
                "Failed to remove local temp file after S3 upload; file may be orphaned"
            );
        }

        Ok(versioned_dir_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_write_handle_unique_versions() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let handle1 = storage.prepare_cache_write(1, "main", "orders");
        let handle2 = storage.prepare_cache_write(1, "main", "orders");
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
    fn test_cache_write_handle_structure() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let handle = storage.prepare_cache_write(42, "public", "users");
        let path_str = handle.local_path.to_string_lossy();

        assert_eq!(handle.connection_id, 42);
        assert_eq!(handle.schema, "public");
        assert_eq!(handle.table, "users");
        assert!(!handle.version.is_empty(), "Version should not be empty");

        assert!(
            path_str.contains("/42/"),
            "Path should contain connection_id"
        );
        assert!(path_str.contains("/public/"), "Path should contain schema");
        assert!(
            path_str.contains("/users/"),
            "Path should contain table directory"
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

    #[test]
    fn test_versioned_directories_are_separate() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let handle1 = storage.prepare_cache_write(1, "main", "orders");
        let handle2 = storage.prepare_cache_write(1, "main", "orders");

        // Both should end with data.parquet
        assert!(handle1.local_path.ends_with("data.parquet"));
        assert!(handle2.local_path.ends_with("data.parquet"));

        // But their parent directories (version dirs) should be different
        let dir1 = handle1.local_path.parent().unwrap();
        let dir2 = handle2.local_path.parent().unwrap();
        assert_ne!(dir1, dir2, "Version directories should be different");

        // And both should be under the same table directory
        let table_dir1 = dir1.parent().unwrap();
        let table_dir2 = dir2.parent().unwrap();
        assert_eq!(
            table_dir1, table_dir2,
            "Both should be under same table dir"
        );
    }

    #[test]
    fn test_versioned_s3_url_format() {
        let bucket = "my-bucket";
        let connection_id = 42;
        let schema = "public";
        let table = "users";
        let version = "abc12345";

        let versioned_dir_url = format!(
            "s3://{}/cache/{}/{}/{}/{}",
            bucket, connection_id, schema, table, version
        );
        let file_url = format!("{}/data.parquet", versioned_dir_url);

        assert_eq!(
            versioned_dir_url,
            "s3://my-bucket/cache/42/public/users/abc12345"
        );
        assert_eq!(
            file_url,
            "s3://my-bucket/cache/42/public/users/abc12345/data.parquet"
        );
    }

    #[tokio::test]
    async fn test_finalize_cache_write_uploads_to_correct_s3_path() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: store.clone(),
            config: None,
        };

        // Create a handle with known version
        let version = "testver1";
        let temp_base = std::env::temp_dir().join("s3_test_finalize");
        let versioned_dir = temp_base
            .join("1")
            .join("public")
            .join("orders")
            .join(version);
        std::fs::create_dir_all(&versioned_dir).unwrap();

        let temp_file = versioned_dir.join("data.parquet");
        std::fs::write(&temp_file, b"test parquet data").unwrap();

        let handle = CacheWriteHandle {
            local_path: temp_file,
            version: version.to_string(),
            connection_id: 1,
            schema: "public".to_string(),
            table: "orders".to_string(),
        };

        let result_url = storage.finalize_cache_write(&handle).await.unwrap();

        // Verify the returned URL contains the version
        assert!(
            result_url.contains(version),
            "Result URL should contain version: {}",
            result_url
        );
        assert_eq!(
            result_url,
            format!("s3://test-bucket/cache/1/public/orders/{}", version)
        );

        // Verify the file was uploaded to the correct S3 path
        let s3_path = ObjectPath::from(format!("cache/1/public/orders/{}/data.parquet", version));
        let result = store.get(&s3_path).await;
        assert!(result.is_ok(), "File should exist at versioned S3 path");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_base);
    }

    #[tokio::test]
    async fn test_finalize_cache_write_cleans_up_temp_file() {
        // This test verifies the streaming implementation cleans up the local temp file.
        // The streaming path uses tokio::fs::remove_file after upload.
        let store = Arc::new(object_store::memory::InMemory::new());
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: store.clone(),
            config: None,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let version = "cleanup_test";
        let versioned_dir = temp_dir
            .path()
            .join("1")
            .join("public")
            .join("orders")
            .join(version);
        std::fs::create_dir_all(&versioned_dir).unwrap();

        let temp_file = versioned_dir.join("data.parquet");
        std::fs::write(&temp_file, b"test data for cleanup verification").unwrap();

        // Verify temp file exists before upload
        assert!(temp_file.exists(), "Temp file should exist before upload");

        let handle = CacheWriteHandle {
            local_path: temp_file.clone(),
            version: version.to_string(),
            connection_id: 1,
            schema: "public".to_string(),
            table: "orders".to_string(),
        };

        storage.finalize_cache_write(&handle).await.unwrap();

        // Verify temp file was removed after successful upload
        assert!(
            !temp_file.exists(),
            "Temp file should be removed after upload"
        );
    }

    #[tokio::test]
    async fn test_finalize_cache_write_streams_large_file() {
        // This test verifies correctness for larger files.
        use std::io::Write;

        let store = Arc::new(object_store::memory::InMemory::new());
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: store.clone(),
            config: None,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let version = "large_file_test";
        let versioned_dir = temp_dir
            .path()
            .join("1")
            .join("public")
            .join("big_table")
            .join(version);
        std::fs::create_dir_all(&versioned_dir).unwrap();

        let temp_file = versioned_dir.join("data.parquet");

        // Create a 10MB file by writing in chunks to avoid large memory allocation.
        let total_size: usize = 10 * 1024 * 1024;
        let chunk_size: usize = 64 * 1024; // 64KB chunks
        let mut expected_checksum: u64 = 0;
        {
            let mut file = std::fs::File::create(&temp_file).unwrap();
            let chunk: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();
            let chunk_sum: u64 = chunk.iter().map(|&b| b as u64).sum();
            for _ in 0..(total_size / chunk_size) {
                file.write_all(&chunk).unwrap();
                expected_checksum = expected_checksum.wrapping_add(chunk_sum);
            }
            file.flush().unwrap();
        }

        let handle = CacheWriteHandle {
            local_path: temp_file.clone(),
            version: version.to_string(),
            connection_id: 1,
            schema: "public".to_string(),
            table: "big_table".to_string(),
        };

        let result_url = storage.finalize_cache_write(&handle).await.unwrap();

        // Verify upload succeeded
        assert!(
            result_url.contains(version),
            "Result URL should contain version"
        );

        // Verify data integrity via size and checksum
        let s3_path =
            ObjectPath::from(format!("cache/1/public/big_table/{}/data.parquet", version));
        let result = store.get(&s3_path).await.unwrap();

        // Stream the uploaded data and compute checksum without loading entire file
        let mut uploaded_checksum: u64 = 0;
        let mut uploaded_size: usize = 0;
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.try_next().await.unwrap() {
            uploaded_size += chunk.len();
            uploaded_checksum =
                uploaded_checksum.wrapping_add(chunk.iter().map(|&b| b as u64).sum::<u64>());
        }

        assert_eq!(
            uploaded_size, total_size,
            "Uploaded file size should match original"
        );
        assert_eq!(
            uploaded_checksum, expected_checksum,
            "Uploaded file checksum should match original"
        );

        // Verify temp file was cleaned up
        assert!(
            !temp_file.exists(),
            "Temp file should be removed after upload"
        );
    }
}
