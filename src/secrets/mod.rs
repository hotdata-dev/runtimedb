mod backend;
mod encrypted_catalog_backend;
mod encryption;
mod validation;

pub use backend::{BackendError, BackendRead, BackendWrite, SecretBackend, SecretRecord};
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

    #[error("Secret '{0}' already exists")]
    AlreadyExists(String),

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

    /// Create a new secret.
    ///
    /// Fails with `AlreadyExists` if an active secret with this name exists.
    /// If a `pending_delete` record exists, it will be cleaned up and replaced.
    /// For updating an existing secret, use `update` instead.
    pub async fn create(&self, name: &str, value: &[u8]) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Check if an active secret already exists
        let existing_active = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        if existing_active.is_some() {
            return Err(SecretError::AlreadyExists(normalized));
        }

        // Check for stale pending_delete record and clean it up
        let existing_any = self
            .catalog
            .get_secret_metadata_any_status(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        if existing_any.is_some() {
            // Stale record from failed delete - clean it up
            tracing::info!(
                secret = %normalized,
                "Cleaning up stale pending_delete metadata before create"
            );
            let _ = self.catalog.delete_secret_metadata(&normalized).await;
        }

        let record = SecretRecord {
            name: normalized.clone(),
            provider_ref: None, // New secrets never have a provider_ref
        };

        // Store via backend
        let write = self.backend.put(&record, value).await?;

        // Create metadata
        let now = Utc::now();
        if let Err(e) = self
            .catalog
            .create_secret_metadata(
                &normalized,
                &self.provider_type,
                write.provider_ref.as_deref(),
                now,
            )
            .await
        {
            // Rollback: delete the value we just stored
            let _ = self.backend.delete(&record).await;
            return Err(SecretError::Database(e.to_string()));
        }

        Ok(())
    }

    /// Update an existing secret's value.
    ///
    /// Fails with `NotFound` if the secret doesn't exist.
    /// For creating a new secret, use `create` instead.
    pub async fn update(&self, name: &str, value: &[u8]) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Load existing metadata to get provider_ref
        let metadata = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        let existing_ref = metadata.provider_ref;

        let record = SecretRecord {
            name: normalized.clone(),
            provider_ref: existing_ref.clone(),
        };

        // Store via backend (update path)
        let write = self.backend.put(&record, value).await?;

        // Use new provider_ref if backend returned one, otherwise preserve existing
        let updated_ref = write.provider_ref.or(existing_ref);

        // Update metadata timestamp
        let now = Utc::now();
        self.catalog
            .update_secret_metadata(
                &normalized,
                &self.provider_type,
                updated_ref.as_deref(),
                now,
            )
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        Ok(())
    }

    /// Delete a secret using three-phase commit.
    ///
    /// 1. Mark metadata as 'pending_delete' (secret becomes invisible)
    /// 2. Delete from backend
    /// 3. Delete metadata row
    ///
    /// If step 2 fails, the secret remains in 'pending_delete' state.
    /// Subsequent delete calls can retry (we use any_status lookup).
    /// If step 3 fails, the secret is effectively deleted (value gone).
    pub async fn delete(&self, name: &str) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Fetch metadata to get provider_ref for the backend.
        // Use any_status so we can retry deletion of pending_delete secrets.
        let metadata = self
            .catalog
            .get_secret_metadata_any_status(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        // Phase 1: Mark as pending_delete (secret becomes invisible to reads)
        // This is idempotent if already pending_delete.
        self.catalog
            .set_secret_status(&normalized, "pending_delete")
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        // Phase 2: Delete from backend
        let record = SecretRecord {
            name: normalized.clone(),
            provider_ref: metadata.provider_ref,
        };

        if let Err(e) = self.backend.delete(&record).await {
            tracing::error!(
                secret = %normalized,
                error = %e,
                "Failed to delete secret from backend; secret left in pending_delete state"
            );
            return Err(e.into());
        }

        // Phase 3: Delete metadata row
        if let Err(e) = self.catalog.delete_secret_metadata(&normalized).await {
            // Backend value is gone but metadata remains in pending_delete
            // This is acceptable - create will clean it up
            tracing::warn!(
                secret = %normalized,
                error = %e,
                "Secret value deleted but failed to remove metadata; will be cleaned up on next create"
            );
        }

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

    #[tokio::test]
    async fn test_create_and_get() {
        let (manager, _dir) = test_manager().await;
        let value = b"my-secret-password";

        manager.create("my-secret", value).await.unwrap();
        let retrieved = manager.get("my-secret").await.unwrap();

        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_create_and_get_string() {
        let (manager, _dir) = test_manager().await;
        let value = "my-string-secret";

        manager.create("string-secret", value.as_bytes()).await.unwrap();
        let retrieved = manager.get_string("string-secret").await.unwrap();

        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_create_already_exists() {
        let (manager, _dir) = test_manager().await;

        manager.create("duplicate", b"first").await.unwrap();
        let result = manager.create("duplicate", b"second").await;

        assert!(matches!(result, Err(SecretError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (manager, _dir) = test_manager().await;
        let result = manager.get("nonexistent").await;

        assert!(matches!(result, Err(SecretError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_update_existing() {
        let (manager, _dir) = test_manager().await;

        manager.create("updatable", b"old-value").await.unwrap();
        manager.update("updatable", b"new-value").await.unwrap();

        let retrieved = manager.get("updatable").await.unwrap();
        assert_eq!(retrieved, b"new-value");
    }

    #[tokio::test]
    async fn test_update_not_found() {
        let (manager, _dir) = test_manager().await;
        let result = manager.update("nonexistent", b"value").await;

        assert!(matches!(result, Err(SecretError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete() {
        let (manager, _dir) = test_manager().await;

        manager.create("to-delete", b"value").await.unwrap();
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

        manager.create("secret-a", b"value-a").await.unwrap();
        manager.create("secret-b", b"value-b").await.unwrap();

        let list = manager.list().await.unwrap();
        assert_eq!(list.len(), 2);

        let metadata = manager.get_metadata("secret-a").await.unwrap();
        assert_eq!(metadata.name, "secret-a");
    }

    #[tokio::test]
    async fn test_name_normalization() {
        let (manager, _dir) = test_manager().await;

        manager.create("My-Secret", b"value").await.unwrap();

        // Should be able to retrieve with different case
        let retrieved = manager.get("my-secret").await.unwrap();
        assert_eq!(retrieved, b"value");
    }

    #[tokio::test]
    async fn test_invalid_name_on_create() {
        let (manager, _dir) = test_manager().await;

        let result = manager.create("invalid name!", b"value").await;
        assert!(matches!(result, Err(SecretError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_invalid_name_on_update() {
        let (manager, _dir) = test_manager().await;

        let result = manager.update("invalid name!", b"value").await;
        assert!(matches!(result, Err(SecretError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_delete_retry_after_pending_delete() {
        // Simulates the scenario where backend delete fails, leaving secret in pending_delete.
        // Subsequent delete calls should still work (not return NotFound).
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );
        catalog.run_migrations().await.unwrap();

        let backend = Arc::new(EncryptedCatalogBackend::new(test_key(), catalog.clone()));
        let manager = SecretManager::new(backend, catalog.clone(), ENCRYPTED_PROVIDER_TYPE);

        // Create a secret
        manager.create("retry-delete", b"value").await.unwrap();

        // Simulate a failed delete by setting status to pending_delete directly
        catalog
            .set_secret_status("retry-delete", "pending_delete")
            .await
            .unwrap();

        // Secret should no longer be visible to normal reads
        let result = manager.get("retry-delete").await;
        assert!(matches!(result, Err(SecretError::NotFound(_))));

        // But delete should still work (this was the bug - it would return NotFound)
        manager.delete("retry-delete").await.unwrap();

        // Verify it's fully deleted
        let any_status = catalog
            .get_secret_metadata_any_status("retry-delete")
            .await
            .unwrap();
        assert!(any_status.is_none());
    }
}