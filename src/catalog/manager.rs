use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt::Debug;

/// Used to conditionally update a secret only if it hasn't been modified.
#[derive(Debug, Clone, Copy)]
pub struct OptimisticLock {
    pub created_at: DateTime<Utc>,
}

impl From<DateTime<Utc>> for OptimisticLock {
    fn from(created_at: DateTime<Utc>) -> Self {
        Self { created_at }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ConnectionInfo {
    pub id: i32,
    pub external_id: String,
    pub name: String,
    pub source_type: String,
    pub config_json: String,
    /// ID of the secret in the secret manager (if any).
    pub secret_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TableInfo {
    pub id: i32,
    pub connection_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_path: Option<String>,
    pub last_sync: Option<String>,
    pub arrow_schema_json: Option<String>,
}

/// Record for deferred file deletion (survives restarts)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PendingDeletion {
    pub id: i32,
    pub path: String,
    pub delete_after: DateTime<Utc>,
    /// Number of failed deletion attempts. After MAX_DELETION_RETRIES,
    /// the record is removed to prevent indefinite accumulation.
    pub retry_count: i32,
}

/// A persisted query result.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct QueryResult {
    pub id: String, // nanoid
    pub parquet_path: String,
    pub created_at: DateTime<Utc>,
}

/// A pending file upload.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UploadInfo {
    pub id: String,
    pub status: String,
    pub storage_url: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}

/// A user-curated dataset.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DatasetInfo {
    pub id: String,
    pub label: String,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_url: String,
    pub arrow_schema_json: String,
    pub source_type: String,
    pub source_config: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Async interface for catalog operations.
#[async_trait]
pub trait CatalogManager: Debug + Send + Sync {
    /// Close the catalog connection. This is idempotent and can be called multiple times.
    async fn close(&self) -> Result<()> {
        // Default implementation does nothing - sqlx pools handle cleanup automatically
        Ok(())
    }

    /// Apply any pending schema migrations. Should be idempotent.
    async fn run_migrations(&self) -> Result<()>;

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>>;
    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String>;
    async fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>>;
    async fn get_connection_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<ConnectionInfo>>;
    async fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32>;
    async fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>>;
    async fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>>;
    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()>;

    /// Clear table cache metadata (set paths to NULL) without deleting files.
    async fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo>;

    /// Clear cache metadata for all tables in a connection (set paths to NULL).
    async fn clear_connection_cache_metadata(&self, name: &str) -> Result<()>;

    /// Delete connection and all associated table rows from metadata.
    async fn delete_connection(&self, name: &str) -> Result<()>;

    /// Get connection by internal ID.
    async fn get_connection_by_id(&self, id: i32) -> Result<Option<ConnectionInfo>>;

    // NOTE: Stale table detection (tables removed from remote source) is intentionally
    // not implemented. The naive approach of comparing discovered vs existing tables
    // is error-prone and doesn't handle data cleanup properly. Stale tables will
    // remain in the catalog until a more robust solution is implemented.

    /// Schedule a file path for deletion after a grace period.
    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()>;

    /// Get all pending file deletions that are due for cleanup.
    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>>;

    /// Increment the retry count for a failed deletion. Returns the new count.
    async fn increment_deletion_retry(&self, id: i32) -> Result<i32>;

    /// Remove a pending deletion record after successful delete or max retries.
    async fn remove_pending_deletion(&self, id: i32) -> Result<()>;

    // Secret management methods - metadata (used by all secret providers)

    /// Get metadata for an active secret (without value).
    /// Returns None for secrets with status != 'active'.
    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>>;

    /// Get metadata for a secret regardless of status (for internal cleanup).
    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>>;

    /// Create secret metadata. Fails if the secret already exists.
    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()>;

    /// Update existing secret metadata.
    /// If `lock` is Some, only updates if created_at matches (returns false on mismatch).
    /// If `lock` is None, updates unconditionally.
    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool>;

    /// Set the status of a secret.
    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool>;

    /// Delete secret metadata. Returns true if the secret existed.
    async fn delete_secret_metadata(&self, name: &str) -> Result<bool>;

    /// List all active secrets (metadata only).
    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>>;

    // Secret management methods - encrypted storage (used by EncryptedSecretManager only)

    /// Get the encrypted value for a secret by its ID.
    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>>;

    /// Store or update an encrypted secret value.
    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()>;

    /// Delete an encrypted secret value. Returns true if it existed.
    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool>;

    /// Get secret metadata by ID.
    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>>;

    /// Count how many connections reference a given secret_id.
    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64>;

    // Query result persistence methods

    /// Store a query result. The result is persisted permanently until explicitly deleted.
    async fn store_result(&self, result: &QueryResult) -> Result<()>;

    /// Get a query result by ID. Returns None if not found.
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>>;

    /// List query results with pagination.
    /// Results are ordered by created_at descending (newest first).
    /// Returns (results, has_more) where has_more indicates if there are more results after this page.
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)>;

    // Upload management methods

    /// Create a new upload record.
    async fn create_upload(&self, upload: &UploadInfo) -> Result<()>;

    /// Get an upload by ID.
    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>>;

    /// List uploads, optionally filtered by status.
    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>>;

    /// Mark an upload as consumed. Returns true if the upload was pending/processing and is now consumed.
    async fn consume_upload(&self, id: &str) -> Result<bool>;

    /// Atomically claim an upload for processing. Returns true if the upload was pending
    /// and is now in "processing" state. This prevents concurrent dataset creation.
    async fn claim_upload(&self, id: &str) -> Result<bool>;

    /// Release a claimed upload back to pending state. Used when dataset creation fails
    /// after claiming but before consuming. Returns true if the upload was processing.
    async fn release_upload(&self, id: &str) -> Result<bool>;

    // Dataset management methods

    /// Create a new dataset record.
    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()>;

    /// Get a dataset by ID.
    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;

    /// Get a dataset by schema and table name.
    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>>;

    /// List all datasets with pagination.
    /// Datasets are ordered by label ascending.
    /// Returns (datasets, has_more) where has_more indicates if there are more datasets after this page.
    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)>;

    /// Update a dataset's label and table_name. Returns true if the dataset existed.
    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool>;

    /// Delete a dataset by ID. Returns the deleted dataset if it existed.
    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;
}
