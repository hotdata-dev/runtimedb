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
    pub name: String,
    pub source_type: String,
    pub config_json: String,
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
    async fn add_connection(&self, name: &str, source_type: &str, config_json: &str)
        -> Result<i32>;
    async fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>>;
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

    /// Get the encrypted value for a secret.
    async fn get_encrypted_secret(&self, name: &str) -> Result<Option<Vec<u8>>>;

    /// Store or update an encrypted secret value.
    async fn put_encrypted_secret_value(&self, name: &str, encrypted_value: &[u8]) -> Result<()>;

    /// Delete an encrypted secret value. Returns true if it existed.
    async fn delete_encrypted_secret_value(&self, name: &str) -> Result<bool>;
}
