// src/storage/s3.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;
use url::Url;

use super::{S3Credentials, StorageManager};

#[derive(Debug, Clone)]
struct S3Config {
    endpoint: String,
    access_key: String,
    secret_key: String,
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
        allow_http: bool,
    ) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http);

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

    fn state_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        format!(
            "s3://{}/state/{}/{}/{}.json",
            self.bucket, connection_id, schema, table
        )
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        format!("s3://{}/cache/{}", self.bucket, connection_id)
    }

    fn state_prefix(&self, connection_id: i32) -> String {
        format!("s3://{}/state/{}", self.bucket, connection_id)
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

    fn prepare_cache_write(
        &self,
        _connection_id: i32,
        _schema: &str,
        table: &str,
    ) -> std::path::PathBuf {
        // Temp file path - will be uploaded to correct S3 location
        std::env::temp_dir().join(format!("{}-{}.parquet", table, uuid::Uuid::new_v4()))
    }

    async fn finalize_cache_write(
        &self,
        written_path: &std::path::Path,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> Result<String> {
        let data = std::fs::read(written_path)?;

        // Get the directory URL (s3://bucket/cache/conn_id/schema/table)
        let dir_url = self.cache_url(connection_id, schema, table);

        // Write file INSIDE the directory
        let file_url = format!("{}/{}.parquet", dir_url, table);
        self.write(&file_url, &data).await?;

        std::fs::remove_file(written_path)?;

        Ok(dir_url) // Return directory URL for ListingTable
    }
}
