//! Error types for dataset operations.

use super::validation::TableNameError;

/// Errors that can occur during dataset operations.
#[derive(Debug)]
pub enum DatasetError {
    /// Dataset not found.
    NotFound(String),
    /// Label is empty or whitespace-only.
    InvalidLabel,
    /// Table name validation failed.
    InvalidTableName(TableNameError),
    /// Auto-generated table name from label is invalid.
    InvalidGeneratedTableName {
        label: String,
        error: TableNameError,
    },
    /// Table name is already in use by another dataset.
    TableNameInUse(String),
    /// Upload not found.
    UploadNotFound(String),
    /// Upload has already been consumed.
    UploadConsumed(String),
    /// Upload is being processed by another request.
    UploadProcessing(String),
    /// Upload is not available (unknown state).
    UploadNotAvailable(String),
    /// Unsupported data format.
    UnsupportedFormat(String),
    /// No data found in the input.
    EmptyData,
    /// Storage error (reading, writing, etc.).
    Storage(anyhow::Error),
    /// Catalog error (database operations).
    Catalog(anyhow::Error),
    /// Data parsing error.
    ParseError(anyhow::Error),
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "Dataset '{}' not found", id),
            Self::InvalidLabel => write!(f, "Dataset label cannot be empty"),
            Self::InvalidTableName(e) => write!(f, "Invalid table name: {}", e),
            Self::InvalidGeneratedTableName { label, error } => {
                write!(
                    f,
                    "Cannot create valid table name from label '{}': {}",
                    label, error
                )
            }
            Self::TableNameInUse(name) => write!(f, "Table name '{}' is already in use", name),
            Self::UploadNotFound(id) => write!(f, "Upload '{}' not found", id),
            Self::UploadConsumed(id) => write!(f, "Upload '{}' has already been consumed", id),
            Self::UploadProcessing(id) => {
                write!(
                    f,
                    "Upload '{}' is currently being processed by another request",
                    id
                )
            }
            Self::UploadNotAvailable(id) => write!(f, "Upload '{}' is not available", id),
            Self::UnsupportedFormat(format) => write!(f, "Unsupported format: {}", format),
            Self::EmptyData => write!(f, "No data found in the input"),
            Self::Storage(e) => write!(f, "Storage error: {}", e),
            Self::Catalog(e) => write!(f, "Catalog error: {}", e),
            Self::ParseError(e) => write!(f, "Parse error: {}", e),
        }
    }
}

impl std::error::Error for DatasetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidTableName(e) => Some(e),
            Self::InvalidGeneratedTableName { error, .. } => Some(error),
            Self::Storage(e) | Self::Catalog(e) | Self::ParseError(e) => e.source(),
            _ => None,
        }
    }
}

impl DatasetError {
    /// Returns true if this is a client error (4xx).
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::NotFound(_)
                | Self::InvalidLabel
                | Self::InvalidTableName(_)
                | Self::InvalidGeneratedTableName { .. }
                | Self::TableNameInUse(_)
                | Self::UploadNotFound(_)
                | Self::UploadConsumed(_)
                | Self::UploadNotAvailable(_)
                | Self::UnsupportedFormat(_)
                | Self::EmptyData
                | Self::ParseError(_)
        )
    }

    /// Returns true if this is a not found error (404).
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_) | Self::UploadNotFound(_))
    }

    /// Returns true if this is a conflict error (409).
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::UploadProcessing(_))
    }
}
