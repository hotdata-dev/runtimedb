// src/storage/s3.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderValue};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::{path::Path as ObjectPath, ClientOptions, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::warn;
use url::Url;

use super::{CacheWriteHandle, DatasetWriteHandle, StorageManager};

/// ObjectStore wrapper that maps HTTP 400 errors to NotFound on `head()` calls.
///
/// Some S3-compatible stores (e.g. NVIDIA AIStore) return 400 Bad Request for HEAD
/// on non-existent "directory" paths instead of 404. DataFusion expects 404 to
/// distinguish files from directories when listing parquet tables.
#[derive(Debug)]
struct S3CompatObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for S3CompatObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3CompatObjectStore({})", self.inner)
    }
}

impl S3CompatObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Check if an object_store error is an HTTP 400 from the S3 backend.
    fn is_http_400(err: &object_store::Error) -> bool {
        let msg = err.to_string();
        msg.contains("400 Bad Request") || msg.contains("status code: 400")
    }
}

#[async_trait]
impl ObjectStore for S3CompatObjectStore {
    async fn put(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &ObjectPath,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &ObjectPath) -> object_store::Result<object_store::GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<object_store::ObjectMeta> {
        match self.inner.head(location).await {
            Ok(meta) => Ok(meta),
            Err(e) if Self::is_http_400(&e) => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: e.into(),
            }),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Explicit credentials for S3 access (used for MinIO or explicit AWS credentials)
#[derive(Debug, Clone)]
struct S3Creds {
    access_key: String,
    secret_key: String,
}

/// S3 storage configuration
#[derive(Debug, Clone)]
struct S3Config {
    region: String,
    /// Custom endpoint for MinIO/S3-compatible storage. None for standard AWS S3.
    endpoint: Option<String>,
    /// Whether HTTP is allowed (typically for local MinIO)
    allow_http: bool,
    /// Explicit credentials. None means use IAM/automatic credential discovery.
    credentials: Option<S3Creds>,
}

#[derive(Debug)]
pub struct S3Storage {
    bucket: String,
    store: Arc<dyn ObjectStore>,
    config: S3Config,
}

impl S3Storage {
    /// Create S3Storage using IAM/automatic credential discovery (for production).
    /// Credentials are automatically discovered from:
    /// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    /// - IAM EC2 instance role
    pub fn new_with_iam(bucket: &str, region: &str) -> Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region)
            .build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: S3Config {
                region: region.to_string(),
                endpoint: None,
                allow_http: false,
                credentials: None, // Use IAM/automatic discovery
            },
        })
    }

    /// Create S3Storage for AWS S3 with explicit credentials (no custom endpoint)
    pub fn new_with_credentials(
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_region(region)
            .build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: S3Config {
                region: region.to_string(),
                endpoint: None,
                allow_http: false,
                credentials: Some(S3Creds {
                    access_key: access_key.to_string(),
                    secret_key: secret_key.to_string(),
                }),
            },
        })
    }

    /// Create S3Storage with custom endpoint for MinIO/S3-compatible storage.
    ///
    /// Uses path-style URLs. When `s3_compat` is true, wraps the store with
    /// S3CompatObjectStore to handle backends (e.g. NVIDIA AIStore) that return
    /// non-standard HTTP error codes.
    ///
    /// When `authorization_header` is set, requests are not signed with SigV4.
    /// This allows S3-compatible backends to authenticate via a custom
    /// `Authorization` header.
    pub fn new_with_endpoint(
        bucket: &str,
        endpoint: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        region: &str,
        allow_http: bool,
        s3_compat: bool,
        authorization_header: Option<&str>,
    ) -> Result<Self> {
        let skip_signature = authorization_header.is_some();
        let credentials = Self::resolve_endpoint_credentials(access_key, secret_key)?;
        let client_options = Self::build_client_options(allow_http, authorization_header)?;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_region(region)
            .with_allow_http(allow_http)
            .with_client_options(client_options)
            // For MinIO/AIStore, we need to use path-style URLs
            .with_virtual_hosted_style_request(false);

        if let Some(credentials) = &credentials {
            builder = builder
                .with_access_key_id(credentials.access_key.as_str())
                .with_secret_access_key(credentials.secret_key.as_str());
        }

        if skip_signature {
            builder = builder.with_skip_signature(true);
        }

        let store = builder.build()?;
        let store: Arc<dyn ObjectStore> = if s3_compat {
            Arc::new(S3CompatObjectStore::new(Arc::new(store)))
        } else {
            Arc::new(store)
        };

        Ok(Self {
            bucket: bucket.to_string(),
            store,
            config: S3Config {
                region: region.to_string(),
                endpoint: Some(endpoint.to_string()),
                allow_http,
                credentials,
            },
        })
    }

    fn resolve_endpoint_credentials(
        access_key: Option<&str>,
        secret_key: Option<&str>,
    ) -> Result<Option<S3Creds>> {
        match (access_key, secret_key) {
            (Some(access_key), Some(secret_key)) => Ok(Some(S3Creds {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
            })),
            (None, None) => Ok(None),
            _ => anyhow::bail!(
                "S3 endpoint configuration requires both access_key and secret_key when using explicit credentials"
            ),
        }
    }

    fn build_client_options(
        allow_http: bool,
        authorization_header: Option<&str>,
    ) -> Result<ClientOptions> {
        let mut client_options = ClientOptions::new().with_allow_http(allow_http);

        if let Some(authorization_header) = authorization_header {
            let mut headers = HeaderMap::new();
            let header_value = HeaderValue::from_str(authorization_header)
                .context("Invalid S3 authorization header value")?;
            headers.insert(AUTHORIZATION, header_value);
            client_options = client_options.with_default_headers(headers);
        }

        Ok(client_options)
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
    fn cache_url(&self, connection_id: &str, schema: &str, table: &str) -> String {
        // For S3, return directory path (DLT creates <table>/*.parquet files)
        format!(
            "s3://{}/cache/{}/{}/{}",
            self.bucket, connection_id, schema, table
        )
    }

    fn cache_prefix(&self, connection_id: &str) -> String {
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

    #[tracing::instrument(
        name = "s3_download",
        skip(self),
        fields(
            runtimedb.source_url = %url,
            runtimedb.bytes_downloaded = tracing::field::Empty,
        )
    )]
    async fn get_local_path(&self, url: &str) -> Result<(std::path::PathBuf, bool)> {
        // For S3, download the file to a temp location
        let path = self.url_to_path(url)?;
        let result = self.store.get(&path).await?;

        // Create unique temp file path
        let temp_id = nanoid::nanoid!(12);
        let temp_path = std::env::temp_dir()
            .join("runtimedb_downloads")
            .join(&temp_id);

        // Ensure parent directory exists
        if let Some(parent) = temp_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Stream S3 content to local temp file
        let mut stream = result.into_stream();
        let file = tokio::fs::File::create(&temp_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);
        let mut bytes_downloaded: u64 = 0;

        while let Some(chunk) = stream.try_next().await? {
            bytes_downloaded += chunk.len() as u64;
            tokio::io::AsyncWriteExt::write_all(&mut writer, &chunk).await?;
        }
        tokio::io::AsyncWriteExt::flush(&mut writer).await?;

        tracing::Span::current().record("runtimedb.bytes_downloaded", bytes_downloaded);

        Ok((temp_path, true)) // true = caller should delete after use
    }

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        let url = Url::parse(&format!("s3://{}", self.bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, self.store.clone());
        Ok(())
    }

    fn get_object_store_config(&self) -> Option<(ObjectStoreUrl, HashMap<String, String>)> {
        let url = ObjectStoreUrl::parse(format!("s3://{}", self.bucket)).ok()?;
        let mut options = HashMap::new();

        // Always include region
        options.insert("aws_region".to_string(), self.config.region.clone());

        // Include credentials if explicitly provided, otherwise liquid-cache
        // will use IAM/automatic credential discovery
        if let Some(creds) = &self.config.credentials {
            options.insert("aws_access_key_id".to_string(), creds.access_key.clone());
            options.insert(
                "aws_secret_access_key".to_string(),
                creds.secret_key.clone(),
            );
        }

        // Include custom endpoint if provided (for MinIO/S3-compatible)
        if let Some(endpoint) = &self.config.endpoint {
            options.insert("aws_endpoint".to_string(), endpoint.clone());
            // MinIO requires path-style URLs (not virtual-hosted)
            options.insert(
                "aws_virtual_hosted_style_request".to_string(),
                "false".to_string(),
            );
        }

        // Set allow_http based on config
        if self.config.allow_http {
            options.insert("aws_allow_http".to_string(), "true".to_string());
        }

        Some((url, options))
    }

    fn get_object_store(&self) -> Option<(ObjectStoreUrl, Arc<dyn ObjectStore>)> {
        let url = ObjectStoreUrl::parse(format!("s3://{}", self.bucket)).ok()?;
        Some((url, self.store.clone()))
    }

    #[tracing::instrument(
        name = "prepare_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema,
            runtimedb.table = %table,
            runtimedb.bucket = %self.bucket,
        )
    )]
    fn prepare_cache_write(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
    ) -> CacheWriteHandle {
        // Use versioned DIRECTORIES to avoid duplicate reads during grace period.
        // Path structure: {temp_dir}/{conn_id}/{schema}/{table}/{version}/data.parquet
        // This matches the S3 destination structure for consistency.
        let version = nanoid::nanoid!(8);
        let local_path = std::env::temp_dir()
            .join(connection_id)
            .join(schema)
            .join(table)
            .join(&version)
            .join("data.parquet");

        CacheWriteHandle {
            local_path,
            version,
            connection_id: connection_id.to_string(),
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
            runtimedb.bytes_uploaded = tracing::field::Empty,
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

        // Get file size before upload for tracing
        let file_size = tokio::fs::metadata(&handle.local_path)
            .await
            .map(|m| m.len())
            .ok();

        // Record the key and cache_url on the current span
        let span = tracing::Span::current();
        span.record("runtimedb.key", &s3_key);
        span.record("runtimedb.cache_url", &versioned_dir_url);
        if let Some(size) = file_size {
            span.record("runtimedb.bytes_uploaded", size);
        }

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

    // Upload operations

    fn upload_url(&self, upload_id: &str) -> String {
        format!("s3://{}/uploads/{}/raw", self.bucket, upload_id)
    }

    fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join("uploads")
            .join(upload_id)
            .join("raw")
    }

    async fn finalize_upload_write(&self, upload_id: &str) -> Result<String> {
        let local_path = self.prepare_upload_write(upload_id);
        let s3_path = ObjectPath::from(format!("uploads/{}/raw", upload_id));

        // Stream file to S3
        let file = tokio::fs::File::open(&local_path).await?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut writer = BufWriter::new(self.store.clone(), s3_path);

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;

        // Clean up local temp file
        if let Err(e) = tokio::fs::remove_file(&local_path).await {
            warn!(
                path = %local_path.display(),
                error = %e,
                "Failed to remove local temp file after S3 upload; file may be orphaned"
            );
        }

        Ok(self.upload_url(upload_id))
    }

    // Dataset operations

    fn dataset_url(&self, dataset_id: &str, version: &str) -> String {
        format!(
            "s3://{}/datasets/{}/{}/data.parquet",
            self.bucket, dataset_id, version
        )
    }

    fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle {
        let version = nanoid::nanoid!(8);
        let local_path = std::env::temp_dir()
            .join("datasets")
            .join(dataset_id)
            .join(&version)
            .join("data.parquet");

        DatasetWriteHandle {
            local_path,
            version,
            dataset_id: dataset_id.to_string(),
        }
    }

    async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String> {
        let s3_path = ObjectPath::from(format!(
            "datasets/{}/{}/data.parquet",
            handle.dataset_id, handle.version
        ));

        // Stream file to S3
        let file = tokio::fs::File::open(&handle.local_path).await?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut writer = BufWriter::new(self.store.clone(), s3_path);

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;

        // Clean up local temp file
        if let Err(e) = tokio::fs::remove_file(&handle.local_path).await {
            warn!(
                path = %handle.local_path.display(),
                error = %e,
                "Failed to remove local temp file after S3 upload; file may be orphaned"
            );
        }

        Ok(self.dataset_url(&handle.dataset_id, &handle.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(_bucket: &str) -> S3Config {
        S3Config {
            region: "us-east-1".to_string(),
            endpoint: None,
            allow_http: false,
            credentials: None,
        }
    }

    #[test]
    fn test_new_with_endpoint_allows_skip_signature_without_credentials() {
        let credentials =
            S3Storage::resolve_endpoint_credentials(None, None).expect("credentials are optional");

        assert!(credentials.is_none());
    }

    #[test]
    fn test_new_with_endpoint_rejects_partial_credentials() {
        let result = S3Storage::resolve_endpoint_credentials(Some("access-key"), None);

        assert!(result.is_err(), "partial credentials should be rejected");
    }

    #[test]
    fn test_cache_write_handle_unique_versions() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: test_config("test-bucket"),
        };

        let handle1 = storage.prepare_cache_write("1", "main", "orders");
        let handle2 = storage.prepare_cache_write("1", "main", "orders");
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
            config: test_config("test-bucket"),
        };

        let handle = storage.prepare_cache_write("42", "public", "users");
        let path_str = handle.local_path.to_string_lossy();

        assert_eq!(handle.connection_id, "42");
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
            config: test_config("test-bucket"),
        };

        let handle1 = storage.prepare_cache_write("1", "main", "orders");
        let handle2 = storage.prepare_cache_write("1", "main", "orders");

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
            config: test_config("test-bucket"),
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
            connection_id: "1".to_string(),
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
            config: test_config("test-bucket"),
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
            connection_id: "1".to_string(),
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
            config: test_config("test-bucket"),
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
            connection_id: "1".to_string(),
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
