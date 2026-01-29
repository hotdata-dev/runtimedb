// src/storage/mod.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::fmt::Debug;

pub mod filesystem;
pub mod s3;

// Re-exports
pub use filesystem::FilesystemStorage;
pub use s3::S3Storage;

/// Handle for a pending cache write operation.
/// Contains all information needed to finalize the write without parsing paths.
#[derive(Debug, Clone)]
pub struct CacheWriteHandle {
    /// Local path where parquet file should be written
    pub local_path: std::path::PathBuf,
    /// Unique version identifier for this write
    pub version: String,
    /// Connection ID
    pub connection_id: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
}

/// Handle for a pending dataset write operation.
#[derive(Debug, Clone)]
pub struct DatasetWriteHandle {
    /// Local path where parquet file should be written
    pub local_path: std::path::PathBuf,
    /// Unique version identifier for this write
    pub version: String,
    /// Dataset ID
    pub dataset_id: String,
}

#[async_trait]
pub trait StorageManager: Debug + Send + Sync {
    // Path construction
    fn cache_url(&self, connection_id: &str, schema: &str, table: &str) -> String;
    fn cache_prefix(&self, connection_id: &str) -> String;

    // File operations
    async fn read(&self, url: &str) -> Result<Vec<u8>>;
    async fn write(&self, url: &str, data: &[u8]) -> Result<()>;
    async fn delete(&self, url: &str) -> Result<()>;
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;
    async fn exists(&self, url: &str) -> Result<bool>;

    /// Get a local file path for reading a storage URL.
    ///
    /// For filesystem storage, returns the existing local path directly.
    /// For S3 storage, downloads the file to a temp location first.
    ///
    /// Returns the local path and a boolean indicating if the file is temporary
    /// (should be deleted by the caller after use).
    async fn get_local_path(&self, url: &str) -> Result<(std::path::PathBuf, bool)>;

    // DataFusion integration
    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()>;

    /// Get object store configuration for liquid-cache registration.
    /// Returns the object store URL and options (credentials) needed to register
    /// with liquid-cache's server.
    fn get_object_store_config(&self) -> Option<(ObjectStoreUrl, HashMap<String, String>)> {
        None
    }

    /// Prepares a cache write operation for atomic data refresh.
    ///
    /// Uses versioned DIRECTORIES to avoid duplicate reads during the grace period
    /// between writing new data and deleting old data. DataFusion's ListingTable
    /// reads all parquet files in a directory, so by placing each version in its
    /// own directory with a fixed filename (`data.parquet`), we ensure only the
    /// active version is read after the catalog is updated.
    ///
    /// Path structure: `{base}/{conn_id}/{schema}/{table}/{version}/data.parquet`
    ///
    /// Returns a handle containing the local path and version for finalization.
    fn prepare_cache_write(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
    ) -> CacheWriteHandle;

    /// Finalizes the cache write after Parquet file is written.
    /// For local storage: no-op (file already in place), returns URL.
    /// For remote storage: uploads temp file to storage, cleans up temp, returns URL.
    async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String>;

    // Upload operations

    /// Get the URL for storing an upload's raw file.
    fn upload_url(&self, upload_id: &str) -> String;

    /// Prepare an upload write (returns local path for writing).
    fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf;

    /// Finalize an upload write (upload to remote if needed).
    async fn finalize_upload_write(&self, upload_id: &str) -> Result<String>;

    // Dataset operations

    /// Get the URL for storing a dataset version.
    fn dataset_url(&self, dataset_id: &str, version: &str) -> String;

    /// Prepare a dataset write.
    fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle;

    /// Finalize dataset write and return URL.
    async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String>;
}
