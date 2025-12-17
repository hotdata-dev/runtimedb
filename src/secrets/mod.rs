mod backend;
mod encrypted_catalog_backend;
mod encryption;
mod validation;

pub use backend::{BackendError, BackendRead, BackendWrite, SecretBackend, SecretRecord, WriteStatus};
pub use encrypted_catalog_backend::{EncryptedCatalogBackend, PROVIDER_TYPE as ENCRYPTED_PROVIDER_TYPE};
pub use encryption::{decrypt, encrypt, DecryptError, EncryptError};
pub use validation::{validate_and_normalize_name, ValidationError};

use crate::catalog::CatalogManager;
use chrono::Utc;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{DateTime, Utc as ChronoUtc};

/// Metadata about a stored secret (no sensitive data).
#[derive(Debug, Clone)]
pub struct SecretMetadata {
    pub name: String,
    pub provider_ref: Option<String>,
    pub created_at: DateTime<ChronoUtc>,
    pub updated_at: DateTime<ChronoUtc>,
}

/// Errors from secret manager operations.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("Secret '{0}' not found")]
    NotFound(String),

    #[error("Secret manager not configured")]
    NotConfigured,

    #[error("Invalid secret name '{0}': must be 1-128 characters, alphanumeric with _ and - only")]
    InvalidName(String),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Invalid secret value: not valid UTF-8")]
    InvalidUtf8,

    #[error("Database error: {0}")]
    Database(String),
}

impl From<ValidationError> for SecretError {
    fn from(e: ValidationError) -> Self {
        match e {
            ValidationError::InvalidSecretName(name) => SecretError::InvalidName(name),
        }
    }
}

impl From<BackendError> for SecretError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound(name) => SecretError::NotFound(name),
            BackendError::Storage(msg) => SecretError::Backend(msg),
        }
    }
}

/// Coordinates secret storage between a backend and catalog metadata.
///
/// The `SecretManager` is the main entry point for secret operations. It:
/// - Validates and normalizes secret names
/// - Manages metadata (timestamps, provider info) in the catalog
/// - Delegates value storage to a `SecretBackend`
///
/// Different backends handle storage differently (local encrypted, Vault, KMS, etc).
#[derive(Debug)]
pub struct SecretManager {
    backend: Arc<dyn SecretBackend>,
    catalog: Arc<dyn CatalogManager>,
    provider_type: String,
}

impl SecretManager {
    /// Creates a new SecretManager.
    ///
    /// # Arguments
    /// * `backend` - Backend for storing secret values
    /// * `catalog` - Catalog manager for metadata persistence
    /// * `provider_type` - Provider identifier stored in metadata (e.g., "encrypted", "vault")
    pub fn new(
        backend: Arc<dyn SecretBackend>,
        catalog: Arc<dyn CatalogManager>,
        provider_type: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            catalog,
            provider_type: provider_type.into(),
        }
    }

    /// Get a secret's raw bytes by name.
    pub async fn get(&self, name: &str) -> Result<Vec<u8>, SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Fetch metadata to get provider_ref for the backend
        let metadata = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        let record = SecretRecord {
            name: normalized.clone(),
            provider_ref: metadata.provider_ref,
        };

        let read = self
            .backend
            .get(&record)
            .await?
            .ok_or_else(|| SecretError::NotFound(normalized))?;

        Ok(read.value)
    }

    /// Get a secret as a UTF-8 string.
    pub async fn get_string(&self, name: &str) -> Result<String, SecretError> {
        let bytes = self.get(name).await?;
        String::from_utf8(bytes).map_err(|_| SecretError::InvalidUtf8)
    }

    /// Get a secret's metadata by name (no value).
    pub async fn get_metadata(&self, name: &str) -> Result<SecretMetadata, SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        self.catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized))
    }

    /// Store a secret (create or update).
    ///
    /// The `record` should contain the secret name and optionally the `provider_ref`
    /// from existing metadata (for updates). For new secrets, `provider_ref` should be `None`.
    pub async fn put(&self, record: &SecretRecord, value: &[u8]) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(&record.name)?;

        let normalized_record = SecretRecord {
            name: normalized.clone(),
            provider_ref: record.provider_ref.clone(),
        };

        // Store via backend
        let write = self.backend.put(&normalized_record, value).await?;

        // Update metadata
        let now = Utc::now();
        if let Err(e) = self
            .catalog
            .put_secret_metadata(&normalized, &self.provider_type, write.provider_ref.as_deref(), now)
            .await
        {
            // Rollback: delete the value we just stored (only if it was created)
            if write.status == WriteStatus::Created {
                let _ = self.backend.delete(&normalized_record).await;
            }
            return Err(SecretError::Database(e.to_string()));
        }

        Ok(())
    }

    /// Delete a secret.
    pub async fn delete(&self, name: &str) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Fetch metadata to get provider_ref for the backend
        let metadata = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        // Delete metadata
        let _ = self
            .catalog
            .delete_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        // Delete from backend using provider_ref from metadata
        let record = SecretRecord {
            name: normalized,
            provider_ref: metadata.provider_ref,
        };
        let _ = self.backend.delete(&record).await;

        Ok(())
    }

    /// List all secrets (metadata only, no values).
    pub async fn list(&self) -> Result<Vec<SecretMetadata>, SecretError> {
        self.catalog
            .list_secrets()
            .await
            .map_err(|e| SecretError::Database(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqliteCatalogManager;
    use tempfile::TempDir;

    fn test_key() -> [u8; 32] {
        [0x42; 32]
    }

    async fn test_manager() -> (SecretManager, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );
        catalog.run_migrations().await.unwrap();

        let backend = Arc::new(EncryptedCatalogBackend::new(test_key(), catalog.clone()));

        (
            SecretManager::new(backend, catalog, ENCRYPTED_PROVIDER_TYPE),
            dir,
        )
    }

    fn record(name: &str) -> SecretRecord {
        SecretRecord {
            name: name.to_string(),
            provider_ref: None,
        }
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (manager, _dir) = test_manager().await;
        let value = b"my-secret-password";

        manager.put(&record("my-secret"), value).await.unwrap();
        let retrieved = manager.get("my-secret").await.unwrap();

        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_put_and_get_string() {
        let (manager, _dir) = test_manager().await;
        let value = "my-string-secret";

        manager
            .put(&record("string-secret"), value.as_bytes())
            .await
            .unwrap();
        let retrieved = manager.get_string("string-secret").await.unwrap();

        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (manager, _dir) = test_manager().await;
        let result = manager.get("nonexistent").await;

        assert!(matches!(result, Err(SecretError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete() {
        let (manager, _dir) = test_manager().await;

        manager.put(&record("to-delete"), b"value").await.unwrap();
        manager.delete("to-delete").await.unwrap();

        let result = manager.get("to-delete").await;
        assert!(matches!(result, Err(SecretError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let (manager, _dir) = test_manager().await;
        let result = manager.delete("nonexistent").await;

        assert!(matches!(result, Err(SecretError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_list_and_metadata() {
        let (manager, _dir) = test_manager().await;

        manager.put(&record("secret-a"), b"value-a").await.unwrap();
        manager.put(&record("secret-b"), b"value-b").await.unwrap();

        let list = manager.list().await.unwrap();
        assert_eq!(list.len(), 2);

        let metadata = manager.get_metadata("secret-a").await.unwrap();
        assert_eq!(metadata.name, "secret-a");
    }

    #[tokio::test]
    async fn test_name_normalization() {
        let (manager, _dir) = test_manager().await;

        manager.put(&record("My-Secret"), b"value").await.unwrap();

        // Should be able to retrieve with different case
        let retrieved = manager.get("my-secret").await.unwrap();
        assert_eq!(retrieved, b"value");
    }

    #[tokio::test]
    async fn test_invalid_name() {
        let (manager, _dir) = test_manager().await;

        let result = manager.put(&record("invalid name!"), b"value").await;
        assert!(matches!(result, Err(SecretError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_update_existing() {
        let (manager, _dir) = test_manager().await;

        manager.put(&record("updatable"), b"old-value").await.unwrap();
        manager.put(&record("updatable"), b"new-value").await.unwrap();

        let retrieved = manager.get("updatable").await.unwrap();
        assert_eq!(retrieved, b"new-value");
    }
}