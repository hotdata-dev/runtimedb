mod backend;
mod encrypted_catalog_backend;
mod encryption;
mod validation;

pub use backend::{BackendError, BackendRead, BackendWrite, SecretBackend, SecretRecord};
pub use encrypted_catalog_backend::{
    EncryptedCatalogBackend, PROVIDER_TYPE as ENCRYPTED_PROVIDER_TYPE,
};
pub use encryption::{decrypt, encrypt, DecryptError, EncryptError};
pub use validation::validate_and_normalize_name;

use crate::catalog::CatalogManager;
use crate::id::generate_secret_id;
use chrono::Utc;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{DateTime, Utc as ChronoUtc};

/// Status of a secret in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretStatus {
    /// Secret is being created; backend write in progress.
    Creating,
    /// Secret is active and available.
    Active,
    /// Secret is marked for deletion; cleanup in progress.
    PendingDelete,
}

impl SecretStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretStatus::Creating => "creating",
            SecretStatus::Active => "active",
            SecretStatus::PendingDelete => "pending_delete",
        }
    }
}

impl std::str::FromStr for SecretStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "creating" => Ok(SecretStatus::Creating),
            "active" => Ok(SecretStatus::Active),
            "pending_delete" => Ok(SecretStatus::PendingDelete),
            _ => Err(()),
        }
    }
}

/// Metadata about a stored secret (no sensitive data).
#[derive(Debug, Clone)]
pub struct SecretMetadata {
    /// Resource ID (e.g., "secr...")
    pub id: String,
    /// User-friendly name (unique, normalized)
    pub name: String,
    pub provider: String,
    pub provider_ref: Option<String>,
    pub status: SecretStatus,
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

    #[error(
        "Secret '{0}' is being created by another process; delete it first if you want to retry"
    )]
    CreationInProgress(String),

    #[error("Secret manager not configured")]
    NotConfigured,

    #[error(
        "Invalid secret name '{0:.128}': must be 1-128 characters, alphanumeric with _ and - only"
    )]
    InvalidName(String),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Invalid secret value: not valid UTF-8")]
    InvalidUtf8,

    #[error("Database error: {0}")]
    Database(String),
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

        // Fetch metadata to get id and provider_ref for the backend
        let metadata = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        let record = SecretRecord {
            id: metadata.id,
            provider_ref: metadata.provider_ref,
        };

        let read = self
            .backend
            .get(&record)
            .await?
            .ok_or(SecretError::NotFound(normalized))?;

        Ok(read.value)
    }

    /// Get a secret's raw bytes by ID.
    pub async fn get_by_id(&self, id: &str) -> Result<Vec<u8>, SecretError> {
        // Fetch metadata to get provider_ref for the backend
        let metadata = self
            .catalog
            .get_secret_metadata_by_id(id)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(id.to_string()))?;

        let record = SecretRecord {
            id: metadata.id,
            provider_ref: metadata.provider_ref,
        };

        let read = self
            .backend
            .get(&record)
            .await?
            .ok_or_else(|| SecretError::NotFound(id.to_string()))?;

        Ok(read.value)
    }

    /// Get a secret as a UTF-8 string by ID.
    pub async fn get_string_by_id(&self, id: &str) -> Result<String, SecretError> {
        let bytes = self.get_by_id(id).await?;
        String::from_utf8(bytes).map_err(|_| SecretError::InvalidUtf8)
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
            .ok_or(SecretError::NotFound(normalized))
    }

    /// Create a new secret.
    ///
    /// Uses optimistic locking to handle concurrent creation attempts safely:
    /// 1. Insert metadata with status=Creating (claims the name)
    /// 2. Store value in backend
    /// 3. Update metadata to Active with optimistic lock on created_at
    ///
    /// If step 2 fails, the Creating record remains (user must delete to retry).
    /// If another request races us at step 1, we detect it and return appropriate error.
    ///
    /// Returns the generated secret ID on success.
    pub async fn create(&self, name: &str, value: &[u8]) -> Result<String, SecretError> {
        use crate::catalog::OptimisticLock;

        let normalized = validate_and_normalize_name(name)?;

        // Check existing state
        let existing = self
            .catalog
            .get_secret_metadata_any_status(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        if let Some(metadata) = existing {
            match metadata.status {
                SecretStatus::Active => return Err(SecretError::AlreadyExists(normalized)),
                SecretStatus::Creating => return Err(SecretError::CreationInProgress(normalized)),
                SecretStatus::PendingDelete => {
                    // Stale record from failed delete - clean it up
                    tracing::info!(
                        secret = %normalized,
                        "Cleaning up stale pending_delete metadata before create"
                    );
                    let _ = self.catalog.delete_secret_metadata(&normalized).await;
                }
            }
        }

        // Generate a new secret ID
        let secret_id = generate_secret_id();

        // Step 1: Insert metadata with status=Creating to claim the name
        let now = Utc::now();
        let creating_metadata = SecretMetadata {
            id: secret_id.clone(),
            name: normalized.clone(),
            provider: self.provider_type.clone(),
            provider_ref: None,
            status: SecretStatus::Creating,
            created_at: now,
            updated_at: now,
        };

        if let Err(e) = self
            .catalog
            .create_secret_metadata(&creating_metadata)
            .await
        {
            // Another request likely beat us - check what's there now
            let current = self
                .catalog
                .get_secret_metadata_any_status(&normalized)
                .await
                .map_err(|e2| SecretError::Database(e2.to_string()))?;

            return match current.map(|m| m.status) {
                Some(SecretStatus::Active) => Err(SecretError::AlreadyExists(normalized)),
                Some(SecretStatus::Creating) => Err(SecretError::CreationInProgress(normalized)),
                _ => Err(SecretError::Database(e.to_string())),
            };
        }

        // Step 2: Store value in backend (keyed by ID)
        let record = SecretRecord {
            id: secret_id.clone(),
            provider_ref: None,
        };

        let write = match self.backend.put(&record, value).await {
            Ok(w) => w,
            Err(e) => {
                // Backend failed - leave Creating record in place.
                // User must delete to retry.
                tracing::error!(
                    secret = %normalized,
                    error = %e,
                    "Backend write failed during create; secret left in Creating state"
                );
                return Err(e.into());
            }
        };

        // Step 3: Update metadata to Active with optimistic lock
        let active_metadata = SecretMetadata {
            id: secret_id.clone(),
            name: normalized.clone(),
            provider: self.provider_type.clone(),
            provider_ref: write.provider_ref,
            status: SecretStatus::Active,
            created_at: now,
            updated_at: Utc::now(),
        };

        let lock = OptimisticLock::from(now);
        let updated = self
            .catalog
            .update_secret_metadata(&active_metadata, Some(lock))
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        if !updated {
            // Our Creating record was deleted while we were writing to backend.
            // The backend value is now orphaned (harmless, will be overwritten on next create).
            return Err(SecretError::Database(format!(
                "Secret '{}' was deleted by another process while being created; please retry",
                normalized
            )));
        }

        Ok(secret_id)
    }

    /// Update an existing secret's value.
    ///
    /// Fails with `NotFound` if the secret doesn't exist.
    /// For creating a new secret, use `create` instead.
    pub async fn update(&self, name: &str, value: &[u8]) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        // Load existing metadata to get id and provider_ref
        let metadata = self
            .catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        let existing_ref = metadata.provider_ref.clone();

        let record = SecretRecord {
            id: metadata.id.clone(),
            provider_ref: existing_ref.clone(),
        };

        // Store via backend (update path)
        let write = self.backend.put(&record, value).await?;

        // Use new provider_ref if backend returned one, otherwise preserve existing
        let updated_ref = write.provider_ref.or(existing_ref);

        // Update metadata timestamp (no optimistic lock for updates - last write wins)
        let now = Utc::now();
        let updated_metadata = SecretMetadata {
            id: metadata.id,
            name: normalized.clone(),
            provider: self.provider_type.clone(),
            provider_ref: updated_ref,
            status: SecretStatus::Active,
            created_at: metadata.created_at,
            updated_at: now,
        };

        self.catalog
            .update_secret_metadata(&updated_metadata, None)
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

        // Fetch metadata to get id and provider_ref for the backend.
        // Use any_status so we can retry deletion of pending_delete secrets.
        let metadata = match self
            .catalog
            .get_secret_metadata_any_status(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
        {
            Some(m) => m,
            None => {
                // Secret doesn't exist - this is idempotent, return success
                return Ok(());
            }
        };

        // Phase 1: Mark as pending_delete (secret becomes invisible to reads)
        // This is idempotent if already pending_delete.
        self.catalog
            .set_secret_status(&normalized, SecretStatus::PendingDelete)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        // Phase 2: Delete from backend (keyed by ID)
        let record = SecretRecord {
            id: metadata.id,
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

        manager
            .create("string-secret", value.as_bytes())
            .await
            .unwrap();
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
    async fn test_delete_idempotent() {
        let (manager, _dir) = test_manager().await;

        // Deleting a non-existent secret should succeed (idempotent)
        let result = manager.delete("nonexistent").await;
        assert!(
            result.is_ok(),
            "Delete of non-existent secret should succeed"
        );

        // Create and delete a secret
        manager.create("to-delete-twice", b"value").await.unwrap();
        manager.delete("to-delete-twice").await.unwrap();

        // Second delete should also succeed (idempotent)
        let result = manager.delete("to-delete-twice").await;
        assert!(result.is_ok(), "Second delete should also succeed");
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
            .set_secret_status("retry-delete", SecretStatus::PendingDelete)
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

    #[tokio::test]
    async fn test_create_blocked_by_creating_status() {
        // If a secret is stuck in "creating" status (e.g., backend write failed),
        // subsequent creates should return CreationInProgress error.
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

        // Manually insert a secret in "creating" status (simulating failed backend write)
        let now = chrono::Utc::now();
        let creating_metadata = SecretMetadata {
            id: generate_secret_id(),
            name: "stuck-secret".to_string(),
            provider: ENCRYPTED_PROVIDER_TYPE.to_string(),
            provider_ref: None,
            status: SecretStatus::Creating,
            created_at: now,
            updated_at: now,
        };
        catalog
            .create_secret_metadata(&creating_metadata)
            .await
            .unwrap();

        // Attempt to create the same secret should return CreationInProgress
        let result = manager.create("stuck-secret", b"value").await;
        assert!(matches!(result, Err(SecretError::CreationInProgress(_))));

        // User can delete the stuck secret to retry
        manager.delete("stuck-secret").await.unwrap();

        // Now create should succeed
        manager.create("stuck-secret", b"value").await.unwrap();
        let retrieved = manager.get("stuck-secret").await.unwrap();
        assert_eq!(retrieved, b"value");
    }
}
