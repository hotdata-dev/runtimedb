use anyhow::Result;
use std::sync::Arc;

use super::batch_writer::BatchWriter;
use super::native::StreamingParquetWriter;
use super::{DataFetchError, DataFetcher, TableMetadata};
use crate::catalog::CatalogManager;
use crate::secrets::SecretManager;
use crate::source::Source;
use crate::storage::StorageManager;

/// Orchestrates the full table fetch workflow: fetch from source → write to storage → update catalog.
#[derive(Debug)]
pub struct FetchOrchestrator {
    fetcher: Arc<dyn DataFetcher>,
    storage: Arc<dyn StorageManager>,
    catalog: Arc<dyn CatalogManager>,
    secret_manager: Arc<SecretManager>,
}

impl FetchOrchestrator {
    pub fn new(
        fetcher: Arc<dyn DataFetcher>,
        storage: Arc<dyn StorageManager>,
        catalog: Arc<dyn CatalogManager>,
        secret_manager: Arc<SecretManager>,
    ) -> Self {
        Self {
            fetcher,
            storage,
            catalog,
            secret_manager,
        }
    }

    /// Fetch table data from source, write to cache storage, and update catalog metadata.
    ///
    /// Returns the URL of the cached parquet file and the row count.
    #[tracing::instrument(
        name = "cache_table",
        skip(self, source),
        fields(
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_written = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
    pub async fn cache_table(
        &self,
        source: &Source,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(String, usize)> {
        // Prepare cache write location
        let handle = self
            .storage
            .prepare_cache_write(connection_id, schema_name, table_name);

        // Create writer
        let mut writer: Box<dyn BatchWriter> =
            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));

        // Fetch the table data into writer
        self.fetcher
            .fetch_table(
                source,
                &self.secret_manager,
                None, // catalog
                schema_name,
                table_name,
                writer.as_mut(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch table: {}", e))?;

        // Close writer and get row count
        let result = writer
            .close()
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;
        let row_count = result.rows;

        // Finalize cache write (uploads to S3 if needed, returns URL)
        let parquet_url = self
            .storage
            .finalize_cache_write(&handle)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finalize cache write: {}", e))?;

        // Update catalog with new path. If this fails, clean up orphaned files.
        if let Ok(Some(info)) = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await
        {
            if let Err(e) = self.catalog.update_table_sync(info.id, &parquet_url).await {
                // Clean up orphaned cache files to prevent storage leaks
                if let Err(cleanup_err) = self.storage.delete_prefix(&parquet_url).await {
                    tracing::warn!(
                        "Failed to clean up orphaned directory {} after catalog update failure: {}",
                        parquet_url,
                        cleanup_err
                    );
                }
                return Err(anyhow::anyhow!("Failed to update catalog: {}", e));
            }
        }

        tracing::Span::current()
            .record("runtimedb.rows_written", row_count)
            .record("runtimedb.cache_url", &parquet_url);

        Ok((parquet_url, row_count))
    }

    /// Discover tables from a remote source.
    /// Delegates to the underlying fetcher.
    pub async fn discover_tables(
        &self,
        source: &Source,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        self.fetcher
            .discover_tables(source, &self.secret_manager)
            .await
    }

    /// Refresh table data with atomic swap semantics.
    /// Writes to new versioned path, then atomically updates catalog.
    /// If catalog update fails, cleans up orphaned files to prevent storage leaks.
    /// Returns (new_url, old_path, rows_synced).
    ///
    /// Returns an error if the table doesn't exist in the catalog (use cache_table for initial sync).
    #[tracing::instrument(
        name = "orchestrator_refresh_table",
        skip(self, source),
        fields(
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_synced = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
    pub async fn refresh_table(
        &self,
        source: &Source,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(String, Option<String>, usize)> {
        // 1. Verify table exists in catalog before doing any expensive I/O.
        // This catches typos early and avoids wasted fetches.
        let old_info = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await?;

        let old_info = old_info.ok_or_else(|| DataFetchError::TableNotFound {
            connection_id: connection_id.to_string(),
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        })?;
        let old_path = old_info.parquet_path.clone();

        // 2. Prepare cache write (generates versioned path)
        let handle = self
            .storage
            .prepare_cache_write(connection_id, schema_name, table_name);

        // 3. Fetch and write to new path
        let mut writer: Box<dyn BatchWriter> =
            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
        self.fetcher
            .fetch_table(
                source,
                &self.secret_manager,
                None,
                schema_name,
                table_name,
                writer.as_mut(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch table: {}", e))?;

        // 4. Close writer and get row count
        let result = writer
            .close()
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;
        let row_count = result.rows;

        // 5. Finalize (upload to S3 if needed)
        let new_url = self
            .storage
            .finalize_cache_write(&handle)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finalize cache write: {}", e))?;

        // 6. Atomic catalog update with cleanup on failure
        // If this fails after finalize_cache_write succeeds, we have orphaned files.
        // Clean them up to prevent storage leaks.
        let catalog_result = self
            .refresh_table_catalog_update(connection_id, schema_name, table_name, &new_url)
            .await;

        if let Err(e) = catalog_result {
            // Clean up orphaned versioned directory - delete the newly written data
            if let Err(cleanup_err) = self.storage.delete_prefix(&new_url).await {
                tracing::warn!(
                    "Failed to clean up orphaned directory {} after catalog update failure: {}",
                    new_url,
                    cleanup_err
                );
            }
            return Err(e);
        }

        tracing::Span::current()
            .record("runtimedb.rows_synced", row_count)
            .record("runtimedb.cache_url", &new_url);

        Ok((new_url, old_path, row_count))
    }

    /// Helper to perform catalog update for refresh_table.
    /// Separated to allow cleanup on failure.
    async fn refresh_table_catalog_update(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        new_url: &str,
    ) -> Result<()> {
        let info = self
            .catalog
            .get_table(connection_id, schema_name, table_name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Table {}.{} not found during refresh",
                    schema_name,
                    table_name
                )
            })?;

        self.catalog
            .update_table_sync(info.id, new_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to update catalog: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MockCatalog;
    use crate::datafetch::{
        BatchWriter, ColumnMetadata, DataFetchError, DataFetcher, TableMetadata,
    };
    use crate::storage::{CacheWriteHandle, DatasetWriteHandle, StorageManager};
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Mock fetcher that produces minimal parquet data
    #[derive(Debug)]
    struct MockFetcher;

    #[async_trait]
    impl DataFetcher for MockFetcher {
        async fn discover_tables(
            &self,
            _source: &Source,
            _secret_manager: &SecretManager,
        ) -> Result<Vec<TableMetadata>, DataFetchError> {
            Ok(vec![TableMetadata {
                catalog_name: None,
                schema_name: "test".to_string(),
                table_name: "orders".to_string(),
                table_type: "BASE TABLE".to_string(),
                columns: vec![ColumnMetadata {
                    name: "id".to_string(),
                    data_type: ArrowDataType::Int32,
                    nullable: false,
                    ordinal_position: 0,
                }],
            }])
        }

        async fn fetch_table(
            &self,
            _source: &Source,
            _secret_manager: &SecretManager,
            _catalog: Option<&str>,
            _schema: &str,
            _table: &str,
            writer: &mut dyn BatchWriter,
        ) -> Result<(), DataFetchError> {
            use datafusion::arrow::array::Int32Array;
            use datafusion::arrow::datatypes::{DataType, Field, Schema};
            use datafusion::arrow::record_batch::RecordBatch;

            let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

            // Initialize the writer with the schema before writing
            writer.init(&schema)?;

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .map_err(|e| DataFetchError::Query(e.to_string()))?;

            writer.write_batch(&batch)?;
            Ok(())
        }
    }

    /// Mock storage that tracks file operations
    #[derive(Debug)]
    struct MockStorage {
        base_path: PathBuf,
        deleted_urls: Mutex<Vec<String>>,
        version_counter: AtomicUsize,
    }

    impl MockStorage {
        fn new(base_path: PathBuf) -> Self {
            Self {
                base_path,
                deleted_urls: Mutex::new(Vec::new()),
                version_counter: AtomicUsize::new(0),
            }
        }

        fn get_deleted_urls(&self) -> Vec<String> {
            self.deleted_urls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl StorageManager for MockStorage {
        fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
            format!(
                "file://{}/{}/{}/{}",
                self.base_path.display(),
                connection_id,
                schema,
                table
            )
        }

        fn cache_prefix(&self, connection_id: i32) -> String {
            format!("{}/{}", self.base_path.display(), connection_id)
        }

        async fn read(&self, _url: &str) -> Result<Vec<u8>> {
            Ok(vec![])
        }

        async fn write(&self, _url: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, url: &str) -> Result<()> {
            self.deleted_urls.lock().unwrap().push(url.to_string());
            // Also delete the actual file/directory if it exists
            if let Some(path) = url.strip_prefix("file://") {
                let path = std::path::Path::new(path);
                if path.exists() {
                    if path.is_dir() {
                        let _ = std::fs::remove_dir_all(path);
                    } else {
                        let _ = std::fs::remove_file(path);
                    }
                }
            }
            Ok(())
        }

        async fn delete_prefix(&self, prefix: &str) -> Result<()> {
            self.deleted_urls.lock().unwrap().push(prefix.to_string());
            // Also delete the actual directory if it exists
            if let Some(path) = prefix.strip_prefix("file://") {
                let path = std::path::Path::new(path);
                if path.exists() && path.is_dir() {
                    let _ = std::fs::remove_dir_all(path);
                }
            }
            Ok(())
        }

        async fn exists(&self, _url: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get_local_path(&self, url: &str) -> Result<(PathBuf, bool)> {
            // Mock storage uses file:// URLs that are already local
            let path = url
                .strip_prefix("file://")
                .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
            Ok((PathBuf::from(path), false))
        }

        fn register_with_datafusion(&self, _ctx: &SessionContext) -> Result<()> {
            Ok(())
        }

        fn prepare_cache_write(
            &self,
            connection_id: i32,
            schema: &str,
            table: &str,
        ) -> CacheWriteHandle {
            let version = format!("v{}", self.version_counter.fetch_add(1, Ordering::SeqCst));
            let local_path = self
                .base_path
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

        async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String> {
            let version_dir = self
                .base_path
                .join(handle.connection_id.to_string())
                .join(&handle.schema)
                .join(&handle.table)
                .join(&handle.version);
            Ok(format!("file://{}", version_dir.display()))
        }

        fn upload_url(&self, upload_id: &str) -> String {
            format!(
                "file://{}/uploads/{}/raw",
                self.base_path.display(),
                upload_id
            )
        }

        fn prepare_upload_write(&self, upload_id: &str) -> PathBuf {
            self.base_path.join("uploads").join(upload_id).join("raw")
        }

        async fn finalize_upload_write(&self, upload_id: &str) -> Result<String> {
            Ok(self.upload_url(upload_id))
        }

        fn dataset_url(&self, dataset_id: &str, version: &str) -> String {
            format!(
                "file://{}/datasets/{}/{}/data.parquet",
                self.base_path.display(),
                dataset_id,
                version
            )
        }

        fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle {
            let version = format!("v{}", self.version_counter.fetch_add(1, Ordering::SeqCst));
            let local_path = self
                .base_path
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
            Ok(self.dataset_url(&handle.dataset_id, &handle.version))
        }
    }

    /// Create a test SecretManager
    async fn create_test_secret_manager(temp_dir: &std::path::Path) -> SecretManager {
        use crate::catalog::SqliteCatalogManager;
        use crate::secrets::{EncryptedCatalogBackend, ENCRYPTED_PROVIDER_TYPE};

        let db_path = temp_dir.join("test_secrets.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );
        catalog.run_migrations().await.unwrap();

        let key = [0x42u8; 32];
        let backend = Arc::new(EncryptedCatalogBackend::new(key, catalog.clone()));

        SecretManager::new(backend, catalog, ENCRYPTED_PROVIDER_TYPE)
    }

    #[tokio::test]
    async fn test_refresh_table_cleans_up_on_catalog_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_path).unwrap();

        let fetcher = Arc::new(MockFetcher);
        let storage = Arc::new(MockStorage::new(cache_path.clone()));
        let catalog = Arc::new(MockCatalog::new());
        let secret_manager = Arc::new(create_test_secret_manager(temp_dir.path()).await);

        // Add a table to the mock catalog
        catalog.add_table(1, "test", "orders");

        // Configure catalog to fail on update
        catalog.set_fail_update(true);

        let orchestrator =
            FetchOrchestrator::new(fetcher, storage.clone(), catalog, secret_manager);

        let source = Source::Duckdb {
            path: ":memory:".to_string(),
        };

        // This should fail because catalog update is configured to fail
        let result = orchestrator
            .refresh_table(&source, 1, "test", "orders")
            .await;

        assert!(result.is_err(), "refresh_table should fail");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Simulated catalog update failure"),
            "Error should be from catalog, got: {}",
            err_msg
        );

        // Verify that cleanup was attempted (storage.delete was called)
        let deleted = storage.get_deleted_urls();
        assert_eq!(
            deleted.len(),
            1,
            "Should have deleted exactly one URL (the orphaned file)"
        );

        // Verify the deleted URL matches the expected pattern
        let deleted_url = &deleted[0];
        assert!(
            deleted_url.contains("/1/test/orders/"),
            "Deleted URL should be for the test table: {}",
            deleted_url
        );
    }

    #[tokio::test]
    async fn test_refresh_table_succeeds_without_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_path).unwrap();

        let fetcher = Arc::new(MockFetcher);
        let storage = Arc::new(MockStorage::new(cache_path.clone()));
        let catalog = Arc::new(MockCatalog::new());
        let secret_manager = Arc::new(create_test_secret_manager(temp_dir.path()).await);

        // Add a table to the mock catalog
        catalog.add_table(1, "test", "orders");

        // Catalog update should succeed (default)
        let orchestrator =
            FetchOrchestrator::new(fetcher, storage.clone(), catalog, secret_manager);

        let source = Source::Duckdb {
            path: ":memory:".to_string(),
        };

        let result = orchestrator
            .refresh_table(&source, 1, "test", "orders")
            .await;

        assert!(result.is_ok(), "refresh_table should succeed");

        // Verify that no cleanup was performed
        let deleted = storage.get_deleted_urls();
        assert!(
            deleted.is_empty(),
            "Should not have deleted any URLs on success"
        );

        // Verify the returned URL and row count
        let (new_url, _old_path, row_count) = result.unwrap();
        assert!(
            new_url.contains("/1/test/orders/"),
            "New URL should be for the test table"
        );
        assert_eq!(row_count, 3, "Should have synced 3 rows from MockFetcher");
    }

    #[tokio::test]
    async fn test_refresh_table_fails_when_table_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_path).unwrap();

        let fetcher = Arc::new(MockFetcher);
        let storage = Arc::new(MockStorage::new(cache_path.clone()));
        let catalog = Arc::new(MockCatalog::new());
        let secret_manager = Arc::new(create_test_secret_manager(temp_dir.path()).await);

        // Don't add the table - it should fail early with TableNotFound

        let orchestrator =
            FetchOrchestrator::new(fetcher, storage.clone(), catalog, secret_manager);

        let source = Source::Duckdb {
            path: ":memory:".to_string(),
        };

        let result = orchestrator
            .refresh_table(&source, 1, "test", "orders")
            .await;

        assert!(result.is_err(), "refresh_table should fail");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not found"),
            "Error should indicate table not found: {}",
            err
        );

        // Verify no cleanup was needed - the early existence check prevents any I/O
        let deleted = storage.get_deleted_urls();
        assert_eq!(
            deleted.len(),
            0,
            "No cleanup should be needed since we fail before writing any data"
        );
    }

    #[tokio::test]
    async fn test_cache_table_cleans_up_on_catalog_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_path).unwrap();

        let fetcher = Arc::new(MockFetcher);
        let storage = Arc::new(MockStorage::new(cache_path.clone()));
        let catalog = Arc::new(MockCatalog::new());
        let secret_manager = Arc::new(create_test_secret_manager(temp_dir.path()).await);

        // Add a table to the mock catalog
        catalog.add_table(1, "test", "orders");

        // Configure catalog to fail on update
        catalog.set_fail_update(true);

        let orchestrator =
            FetchOrchestrator::new(fetcher, storage.clone(), catalog, secret_manager);

        let source = Source::Duckdb {
            path: ":memory:".to_string(),
        };

        // This should fail because catalog update is configured to fail
        let result = orchestrator.cache_table(&source, 1, "test", "orders").await;

        assert!(result.is_err(), "cache_table should fail");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to update catalog"),
            "Error should be from catalog, got: {}",
            err_msg
        );

        // Verify that cleanup was attempted (storage.delete_prefix was called)
        let deleted = storage.get_deleted_urls();
        assert_eq!(
            deleted.len(),
            1,
            "Should have deleted exactly one URL (the orphaned file)"
        );

        // Verify the deleted URL matches the expected pattern
        let deleted_url = &deleted[0];
        assert!(
            deleted_url.contains("/1/test/orders/"),
            "Deleted URL should be for the test table: {}",
            deleted_url
        );
    }
}
