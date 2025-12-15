mod validation;

pub use validation::{validate_and_normalize_name, ValidationError};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::fmt::Debug;

/// Metadata about a stored secret (no sensitive data).
#[derive(Debug, Clone)]
pub struct SecretMetadata {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Errors from secret manager operations.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("Secret '{0}' not found")]
    NotFound(String),

    #[error("Secret manager not configured: RIVETDB_SECRET_KEY not set")]
    NotConfigured,

    #[error("Invalid secret name '{0}': must be 1-128 characters, alphanumeric with _ and - only")]
    InvalidName(String),

    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

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

/// Trait for secret storage and retrieval.
#[async_trait]
pub trait SecretManager: Debug + Send + Sync {
    /// Get a secret's raw bytes by name.
    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretError>;

    /// Get a secret's metadata by name (no value).
    async fn get_metadata(&self, name: &str) -> Result<SecretMetadata, SecretError>;

    /// Store a secret (create or update).
    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretError>;

    /// Delete a secret.
    async fn delete(&self, name: &str) -> Result<(), SecretError>;

    /// List all secrets (metadata only, no values).
    async fn list(&self) -> Result<Vec<SecretMetadata>, SecretError>;

    /// Get a secret as a UTF-8 string (convenience method).
    async fn get_string(&self, name: &str) -> Result<String, SecretError> {
        let bytes = self.get(name).await?;
        String::from_utf8(bytes).map_err(|_| SecretError::InvalidUtf8)
    }
}