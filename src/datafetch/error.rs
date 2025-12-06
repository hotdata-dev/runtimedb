//! Error types for data fetching operations

use thiserror::Error;

/// Errors that can occur during data fetching operations
#[derive(Debug, Error)]
pub enum DataFetchError {
    /// Failed to load driver library
    #[error("driver load failed: {0}")]
    DriverLoad(String),

    /// Failed to establish connection to remote database
    #[error("connection failed: {0}")]
    Connection(String),

    /// Query execution failed
    #[error("query failed: {0}")]
    Query(String),

    /// Failed to write data to storage
    #[error("storage write failed: {0}")]
    Storage(String),

    /// Requested driver is not supported or not available
    #[error("unsupported driver: {0}")]
    UnsupportedDriver(String),

    /// Failed to discover tables or metadata
    #[error("discovery failed: {0}")]
    Discovery(String),

    /// Failed to serialize or deserialize Arrow schema
    #[error("schema serialization failed: {0}")]
    SchemaSerialization(String),
}

impl From<std::io::Error> for DataFetchError {
    fn from(e: std::io::Error) -> Self {
        DataFetchError::Storage(e.to_string())
    }
}

impl From<sqlx::Error> for DataFetchError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::Configuration(_) => DataFetchError::Connection(e.to_string()),
            sqlx::Error::Database(_) => DataFetchError::Query(e.to_string()),
            sqlx::Error::Io(_) => DataFetchError::Connection(e.to_string()),
            _ => DataFetchError::Connection(e.to_string()),
        }
    }
}