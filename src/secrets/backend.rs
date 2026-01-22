//! Low-level storage abstraction for secrets.
//!
//! The `SecretBackend` trait defines raw storage operations, decoupling
//! the manager layer (validation, encryption, metadata) from persistence.
//! This allows different storage backends (catalog tables, Vault, KMS)
//! without duplicating manager logic.

use async_trait::async_trait;
use std::fmt::Debug;

/// Error type for backend storage operations.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("Secret '{0}' not found")]
    NotFound(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Catalog metadata the manager already knows about a secret.
/// Backends can use this information (e.g. provider_ref) to locate the raw value.
#[derive(Debug, Clone)]
pub struct SecretRecord {
    pub id: String,
    pub provider_ref: Option<String>,
    // Extend with additional fields (version, tags, etc.) as needed.
}

/// Raw read result returned by a backend.
#[derive(Debug, Clone)]
pub struct BackendRead {
    pub value: Vec<u8>,
    pub provider_ref: Option<String>,
}

/// Result of writing a secret to a backend.
#[derive(Debug, Clone)]
pub struct BackendWrite {
    pub status: WriteStatus,
    pub provider_ref: Option<String>,
}

/// Whether a write created a new secret or updated an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStatus {
    Created,
    Updated,
}

/// Low-level storage trait for secret values.
///
/// Managers own name validation, encryption/decryption, and catalog metadata.
/// Backends are pure key-value stores that may rely on manager-provided
/// metadata (e.g. `provider_ref`) to locate entries and can return updated
/// metadata for the manager to persist.
#[async_trait]
pub trait SecretBackend: Debug + Send + Sync {
    /// Retrieve the raw stored value for `record`, returning `None` if not found.
    async fn get(&self, record: &SecretRecord) -> Result<Option<BackendRead>, BackendError>;

    /// Store `value` for `record` (create or update).
    ///
    /// Returns a `BackendWrite` so the manager knows whether it was a create
    /// vs update and can persist any backend-provided metadata.
    async fn put(&self, record: &SecretRecord, value: &[u8]) -> Result<BackendWrite, BackendError>;

    /// Delete the entry identified by `record`.
    ///
    /// Returns `true` if something was deleted, `false` if it didn't exist.
    async fn delete(&self, record: &SecretRecord) -> Result<bool, BackendError>;
}
