// src/storage/filesystem.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::fs;
use std::path::{Path, PathBuf};

use super::StorageManager;

#[derive(Debug)]
pub struct FilesystemStorage {
    cache_base: PathBuf,
}

impl FilesystemStorage {
    pub fn new(cache_base: &str) -> Self {
        Self {
            cache_base: PathBuf::from(cache_base),
        }
    }
}

#[async_trait]
impl StorageManager for FilesystemStorage {
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        // Return directory path (DLT creates <table>/*.parquet files)
        // This matches S3Storage behavior and works with DataFusion's ListingTable
        let path = self
            .cache_base
            .join(connection_id.to_string())
            .join(schema)
            .join(table);
        format!("file://{}", path.display())
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        self.cache_base
            .join(connection_id.to_string())
            .to_string_lossy()
            .to_string()
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        Ok(fs::read(path)?)
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        let path = Path::new(path);

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(path, data)?;
        Ok(())
    }

    async fn delete(&self, url: &str) -> Result<()> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        let path = Path::new(path);

        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path)?;
            } else {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let path = Path::new(prefix);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        Ok(Path::new(path).exists())
    }

    fn register_with_datafusion(&self, _ctx: &SessionContext) -> Result<()> {
        // No-op for filesystem - DataFusion handles file:// by default
        Ok(())
    }

    fn prepare_cache_write(&self, connection_id: i32, schema: &str, table: &str) -> PathBuf {
        // Write file INSIDE the table directory (for ListingTable compatibility)
        self.cache_base
            .join(connection_id.to_string())
            .join(schema)
            .join(table) // table directory
            .join(format!("{}.parquet", table)) // file inside directory
    }

    async fn finalize_cache_write(
        &self,
        _written_path: &Path,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> Result<String> {
        // No-op for local storage - file is already in place
        Ok(self.cache_url(connection_id, schema, table))
    }
}
