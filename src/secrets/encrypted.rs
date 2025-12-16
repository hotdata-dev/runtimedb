//! SecretManager implementation that encrypts values using AES-256-GCM-SIV.

use crate::catalog::CatalogManager;
use crate::secrets::{
    decrypt, encrypt, validate_and_normalize_name, SecretError, SecretManager, SecretMetadata,
};
use async_trait::async_trait;
use chrono::Utc;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// SecretManager that encrypts values with AES-256-GCM-SIV and stores in the catalog database.
pub struct EncryptedSecretManager {
    key: [u8; 32],
    catalog: Arc<dyn CatalogManager>,
}

impl EncryptedSecretManager {
    /// Creates a new EncryptedSecretManager.
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
    ) -> Result<Self, SecretError> {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let key_bytes = STANDARD
            .decode(key_base64)
            .map_err(|e| SecretError::EncryptionFailed(format!("Invalid base64 key: {}", e)))?;

        if key_bytes.len() != 32 {
            return Err(SecretError::EncryptionFailed(format!(
                "Key must be exactly 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let key: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| SecretError::EncryptionFailed("Key conversion failed".into()))?;

        Ok(Self::new(key, catalog))
    }
}

impl Debug for EncryptedSecretManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedSecretManager")
            .field("catalog", &self.catalog)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretManager for EncryptedSecretManager {
    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        let encrypted = self
            .catalog
            .get_encrypted_secret(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized.clone()))?;

        decrypt(&self.key, &encrypted, &normalized)
            .map_err(|e| SecretError::DecryptionFailed(e.to_string()))
    }

    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        let encrypted = encrypt(&self.key, value, &normalized)
            .map_err(|e| SecretError::EncryptionFailed(e.to_string()))?;

        let now = Utc::now();

        self.catalog
            .put_secret(&normalized, "encrypted", None, &encrypted, now)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))
    }

    async fn delete(&self, name: &str) -> Result<(), SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        let deleted = self
            .catalog
            .delete_secret(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?;

        if !deleted {
            return Err(SecretError::NotFound(normalized));
        }

        Ok(())
    }

    async fn get_metadata(&self, name: &str) -> Result<SecretMetadata, SecretError> {
        let normalized = validate_and_normalize_name(name)?;

        self.catalog
            .get_secret_metadata(&normalized)
            .await
            .map_err(|e| SecretError::Database(e.to_string()))?
            .ok_or_else(|| SecretError::NotFound(normalized))
    }

    async fn list(&self) -> Result<Vec<SecretMetadata>, SecretError> {
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

    async fn test_manager() -> (EncryptedSecretManager, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();
        catalog.run_migrations().await.unwrap();
        (EncryptedSecretManager::new(test_key(), Arc::new(catalog)), dir)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (manager, _dir) = test_manager().await;
        let value = b"my-secret-password";

        manager.put("my-secret", value).await.unwrap();
        let retrieved = manager.get("my-secret").await.unwrap();

        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_put_and_get_string() {
        let (manager, _dir) = test_manager().await;
        let value = "my-string-secret";

        manager.put("string-secret", value.as_bytes()).await.unwrap();
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

        manager.put("to-delete", b"value").await.unwrap();
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

        manager.put("secret-a", b"value-a").await.unwrap();
        manager.put("secret-b", b"value-b").await.unwrap();

        let list = manager.list().await.unwrap();
        assert_eq!(list.len(), 2);

        let metadata = manager.get_metadata("secret-a").await.unwrap();
        assert_eq!(metadata.name, "secret-a");
    }

    #[tokio::test]
    async fn test_name_normalization() {
        let (manager, _dir) = test_manager().await;

        manager.put("My-Secret", b"value").await.unwrap();

        // Should be able to retrieve with different case
        let retrieved = manager.get("my-secret").await.unwrap();
        assert_eq!(retrieved, b"value");
    }

    #[tokio::test]
    async fn test_invalid_name() {
        let (manager, _dir) = test_manager().await;

        let result = manager.put("invalid name!", b"value").await;
        assert!(matches!(result, Err(SecretError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_update_existing() {
        let (manager, _dir) = test_manager().await;

        manager.put("updatable", b"old-value").await.unwrap();
        manager.put("updatable", b"new-value").await.unwrap();

        let retrieved = manager.get("updatable").await.unwrap();
        assert_eq!(retrieved, b"new-value");
    }

    #[tokio::test]
    async fn test_from_base64_key() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let key = [0x42u8; 32];
        let key_base64 = STANDARD.encode(key);

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();

        let result = EncryptedSecretManager::from_base64_key(&key_base64, Arc::new(catalog));
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
        let result = EncryptedSecretManager::from_base64_key("dG9vLXNob3J0", catalog.clone());
        assert!(matches!(result, Err(SecretError::EncryptionFailed(_))));

        // Invalid base64
        let result = EncryptedSecretManager::from_base64_key("not-valid-base64!!!", catalog);
        assert!(matches!(result, Err(SecretError::EncryptionFailed(_))));
    }
}