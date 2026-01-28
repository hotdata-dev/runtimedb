//! Mock catalog implementation for testing.
//!
//! Provides a configurable mock implementation of `CatalogManager` that can be used
//! in tests to avoid needing a real database.

use super::{
    CatalogManager, ConnectionInfo, DatasetInfo, OptimisticLock, PendingDeletion, QueryResult,
    TableInfo, UploadInfo,
};
use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Mock catalog that can be configured to fail for testing error handling.
#[derive(Debug)]
pub struct MockCatalog {
    tables: Mutex<HashMap<(String, String, String), TableInfo>>,
    fail_update: AtomicBool,
    next_id: AtomicUsize,
}

impl MockCatalog {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            fail_update: AtomicBool::new(false),
            next_id: AtomicUsize::new(1),
        }
    }

    /// Add a table entry to the mock catalog.
    pub fn add_table_entry(&self, connection_id: &str, schema: &str, table: &str) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst) as i32;
        self.tables.lock().unwrap().insert(
            (
                connection_id.to_string(),
                schema.to_string(),
                table.to_string(),
            ),
            TableInfo {
                id,
                connection_id: connection_id.to_string(),
                schema_name: schema.to_string(),
                table_name: table.to_string(),
                parquet_path: None,
                last_sync: None,
                arrow_schema_json: None,
            },
        );
    }

    /// Configure whether update operations should fail.
    pub fn set_fail_update(&self, fail: bool) {
        self.fail_update.store(fail, Ordering::SeqCst);
    }
}

impl Default for MockCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CatalogManager for MockCatalog {
    async fn run_migrations(&self) -> Result<()> {
        Ok(())
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        Ok(vec![])
    }

    async fn add_connection(
        &self,
        _name: &str,
        _source_type: &str,
        _config_json: &str,
        _secret_id: Option<&str>,
    ) -> Result<String> {
        Ok("conn_mock123".to_string())
    }

    async fn get_connection(&self, _id: &str) -> Result<Option<ConnectionInfo>> {
        Ok(None)
    }

    async fn get_connection_by_name(&self, _name: &str) -> Result<Option<ConnectionInfo>> {
        Ok(None)
    }

    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        _arrow_schema_json: &str,
    ) -> Result<i32> {
        self.add_table_entry(connection_id, schema_name, table_name);
        Ok(self.next_id.load(Ordering::SeqCst) as i32 - 1)
    }

    async fn list_tables(&self, _connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        Ok(self.tables.lock().unwrap().values().cloned().collect())
    }

    async fn get_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        Ok(self
            .tables
            .lock()
            .unwrap()
            .get(&(
                connection_id.to_string(),
                schema_name.to_string(),
                table_name.to_string(),
            ))
            .cloned())
    }

    async fn update_table_sync(&self, _table_id: i32, _parquet_path: &str) -> Result<()> {
        if self.fail_update.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Simulated catalog update failure"));
        }
        Ok(())
    }

    async fn clear_table_cache_metadata(
        &self,
        _connection_id: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> Result<TableInfo> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    async fn clear_connection_cache_metadata(&self, _connection_id: &str) -> Result<()> {
        Ok(())
    }

    async fn delete_connection(&self, _connection_id: &str) -> Result<()> {
        Ok(())
    }

    async fn schedule_file_deletion(
        &self,
        _path: &str,
        _delete_after: DateTime<Utc>,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>> {
        Ok(vec![])
    }

    async fn increment_deletion_retry(&self, _id: i32) -> Result<i32> {
        Ok(1)
    }

    async fn remove_pending_deletion(&self, _id: i32) -> Result<()> {
        Ok(())
    }

    async fn get_secret_metadata(&self, _name: &str) -> Result<Option<SecretMetadata>> {
        Ok(None)
    }

    async fn get_secret_metadata_any_status(&self, _name: &str) -> Result<Option<SecretMetadata>> {
        Ok(None)
    }

    async fn create_secret_metadata(&self, _metadata: &SecretMetadata) -> Result<()> {
        Ok(())
    }

    async fn update_secret_metadata(
        &self,
        _metadata: &SecretMetadata,
        _lock: Option<OptimisticLock>,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn set_secret_status(&self, _name: &str, _status: SecretStatus) -> Result<bool> {
        Ok(true)
    }

    async fn delete_secret_metadata(&self, _name: &str) -> Result<bool> {
        Ok(true)
    }

    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>> {
        Ok(vec![])
    }

    async fn get_encrypted_secret(&self, _name: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn put_encrypted_secret_value(&self, _name: &str, _encrypted_value: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete_encrypted_secret_value(&self, _secret_id: &str) -> Result<bool> {
        Ok(true)
    }

    async fn get_secret_metadata_by_id(&self, _id: &str) -> Result<Option<SecretMetadata>> {
        Ok(None)
    }

    async fn store_result(&self, _result: &QueryResult) -> Result<()> {
        Ok(())
    }

    async fn get_result(&self, _id: &str) -> Result<Option<QueryResult>> {
        Ok(None)
    }

    async fn list_results(
        &self,
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<QueryResult>, bool)> {
        Ok((vec![], false))
    }

    async fn count_connections_by_secret_id(&self, _secret_id: &str) -> Result<i64> {
        Ok(0)
    }

    async fn create_upload(&self, _upload: &UploadInfo) -> Result<()> {
        Ok(())
    }

    async fn get_upload(&self, _id: &str) -> Result<Option<UploadInfo>> {
        Ok(None)
    }

    async fn list_uploads(&self, _status: Option<&str>) -> Result<Vec<UploadInfo>> {
        Ok(vec![])
    }

    async fn consume_upload(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }

    async fn claim_upload(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }

    async fn release_upload(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }

    async fn create_dataset(&self, _dataset: &DatasetInfo) -> Result<()> {
        Ok(())
    }

    async fn get_dataset(&self, _id: &str) -> Result<Option<DatasetInfo>> {
        Ok(None)
    }

    async fn get_dataset_by_table_name(
        &self,
        _schema_name: &str,
        _table_name: &str,
    ) -> Result<Option<DatasetInfo>> {
        Ok(None)
    }

    async fn list_datasets(
        &self,
        _limit: usize,
        _offset: usize,
    ) -> Result<(Vec<DatasetInfo>, bool)> {
        Ok((vec![], false))
    }

    async fn list_all_datasets(&self) -> Result<Vec<DatasetInfo>> {
        Ok(vec![])
    }

    async fn list_dataset_table_names(&self, _schema_name: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn update_dataset(&self, _id: &str, _label: &str, _table_name: &str) -> Result<bool> {
        Ok(false)
    }

    async fn delete_dataset(&self, _id: &str) -> Result<Option<DatasetInfo>> {
        Ok(None)
    }
}
