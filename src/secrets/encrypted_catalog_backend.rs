//! SecretBackend implementation that encrypts values and stores in catalog tables.

use crate::catalog::CatalogManager;
use crate::secrets::backend::{
    BackendError, BackendRead, BackendWrite, SecretBackend, SecretRecord, WriteStatus,
};
use crate::secrets::{decrypt, encrypt};
use async_trait::async_trait;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Provider type identifier for this backend.
pub const PROVIDER_TYPE: &str = "encrypted";

/// SecretBackend that encrypts values with AES-256-GCM-SIV and stores in catalog.
///
/// This backend handles:
/// - AES-256-GCM-SIV encryption/decryption
/// - Storage in the catalog's `encrypted_secret_values` table
///
/// The secret name is used as AAD (additional authenticated data) to prevent
/// ciphertext from being swapped between secrets.
pub struct EncryptedCatalogBackend {
    key: [u8; 32],
    catalog: Arc<dyn CatalogManager>,
}

impl EncryptedCatalogBackend {
    /// Creates a new EncryptedCatalogBackend.
    ///
    /// # Arguments
    /// * `key` - 32-byte AES-256 key
    /// * `catalog` - Catalog manager for database access
    pub fn new(key: [u8; 32], catalog: Arc<dyn CatalogManager>) -> Self {
        Self { key, catalog }
    }

    /// Creates from base64-encoded key string.
    pub fn from_base64_key(
        key_base64: &str,
        catalog: Arc<dyn CatalogManager>,
    ) -> Result<Self, BackendError> {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let key_bytes = STANDARD
            .decode(key_base64)
            .map_err(|e| BackendError::Storage(format!("Invalid base64 key: {}", e)))?;

        if key_bytes.len() != 32 {
            return Err(BackendError::Storage(format!(
                "Key must be exactly 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let key: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| BackendError::Storage("Key conversion failed".into()))?;

        Ok(Self::new(key, catalog))
    }

    /// Returns the provider type for this backend.
    pub fn provider_type(&self) -> &'static str {
        PROVIDER_TYPE
    }
}

impl Debug for EncryptedCatalogBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedCatalogBackend")
            .field("catalog", &self.catalog)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretBackend for EncryptedCatalogBackend {
    async fn get(&self, record: &SecretRecord) -> Result<Option<BackendRead>, BackendError> {
        let encrypted = self
            .catalog
            .get_encrypted_secret(&record.name)
            .await
            .map_err(|e| BackendError::Storage(e.to_string()))?;

        match encrypted {
            Some(ciphertext) => {
                let plaintext = decrypt(&self.key, &ciphertext, &record.name)
                    .map_err(|e| BackendError::Storage(format!("Decryption failed: {}", e)))?;

                Ok(Some(BackendRead {
                    value: plaintext,
                    provider_ref: None,
                }))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, record: &SecretRecord, value: &[u8]) -> Result<BackendWrite, BackendError> {
        // Encrypt the value using the secret name as AAD
        let ciphertext = encrypt(&self.key, value, &record.name)
            .map_err(|e| BackendError::Storage(format!("Encryption failed: {}", e)))?;

        // Check if secret already exists to determine Created vs Updated
        let exists = self
            .catalog
            .get_encrypted_secret(&record.name)
            .await
            .map_err(|e| BackendError::Storage(e.to_string()))?
            .is_some();

        // Store encrypted value
        self.catalog
            .put_encrypted_secret_value(&record.name, &ciphertext)
            .await
            .map_err(|e| BackendError::Storage(e.to_string()))?;

        Ok(BackendWrite {
            status: if exists {
                WriteStatus::Updated
            } else {
                WriteStatus::Created
            },
            provider_ref: None,
        })
    }

    async fn delete(&self, record: &SecretRecord) -> Result<bool, BackendError> {
        self.catalog
            .delete_encrypted_secret_value(&record.name)
            .await
            .map_err(|e| BackendError::Storage(e.to_string()))
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

    async fn test_backend() -> (EncryptedCatalogBackend, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );
        catalog.run_migrations().await.unwrap();
        (EncryptedCatalogBackend::new(test_key(), catalog), dir)
    }

    fn record(name: &str) -> SecretRecord {
        SecretRecord {
            name: name.to_string(),
            provider_ref: None,
        }
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (backend, _dir) = test_backend().await;
        let rec = record("test-secret");
        let value = b"my-secret-password";

        let write = backend.put(&rec, value).await.unwrap();
        assert_eq!(write.status, WriteStatus::Created);

        let read = backend.get(&rec).await.unwrap().unwrap();
        assert_eq!(read.value, value);
    }

    #[tokio::test]
    async fn test_encryption_is_applied() {
        let (backend, _dir) = test_backend().await;
        let rec = record("encrypted-test");
        let plaintext = b"sensitive-data";

        backend.put(&rec, plaintext).await.unwrap();

        // Read raw from catalog - should be encrypted, not plaintext
        let raw = backend
            .catalog
            .get_encrypted_secret(&rec.name)
            .await
            .unwrap()
            .unwrap();

        assert_ne!(raw, plaintext, "Value should be encrypted in storage");
        assert!(raw.len() > plaintext.len(), "Ciphertext should be larger");
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (backend, _dir) = test_backend().await;
        let rec = record("nonexistent");

        let result = backend.get(&rec).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_put_update() {
        let (backend, _dir) = test_backend().await;
        let rec = record("updatable");

        let write1 = backend.put(&rec, b"old-value").await.unwrap();
        assert_eq!(write1.status, WriteStatus::Created);

        let write2 = backend.put(&rec, b"new-value").await.unwrap();
        assert_eq!(write2.status, WriteStatus::Updated);

        let read = backend.get(&rec).await.unwrap().unwrap();
        assert_eq!(read.value, b"new-value");
    }

    #[tokio::test]
    async fn test_delete() {
        let (backend, _dir) = test_backend().await;
        let rec = record("to-delete");

        backend.put(&rec, b"value").await.unwrap();

        let deleted = backend.delete(&rec).await.unwrap();
        assert!(deleted);

        let result = backend.get(&rec).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let (backend, _dir) = test_backend().await;
        let rec = record("nonexistent");

        let deleted = backend.delete(&rec).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_wrong_key_fails_decrypt() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );
        catalog.run_migrations().await.unwrap();

        // Store with one key
        let backend1 = EncryptedCatalogBackend::new([0x42; 32], catalog.clone());
        let rec = record("key-test");
        backend1.put(&rec, b"secret").await.unwrap();

        // Try to read with different key
        let backend2 = EncryptedCatalogBackend::new([0x43; 32], catalog);
        let result = backend2.get(&rec).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Decryption failed"));
    }

    #[tokio::test]
    async fn test_from_base64_key() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let key = [0x42u8; 32];
        let key_base64 = STANDARD.encode(key);

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );

        let result = EncryptedCatalogBackend::from_base64_key(&key_base64, catalog);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_from_base64_key_invalid() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(
            SqliteCatalogManager::new(db_path.to_str().unwrap())
                .await
                .unwrap(),
        );

        // Too short
        let result = EncryptedCatalogBackend::from_base64_key("dG9vLXNob3J0", catalog.clone());
        assert!(result.is_err());

        // Invalid base64
        let result = EncryptedCatalogBackend::from_base64_key("not-valid-base64!!!", catalog);
        assert!(result.is_err());
    }
}
