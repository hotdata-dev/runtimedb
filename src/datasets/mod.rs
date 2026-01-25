//! Dataset management and utilities.

pub mod error;
pub mod validation;

pub use error::DatasetError;
pub use validation::*;

/// The default schema name for datasets.
pub const DEFAULT_SCHEMA: &str = "default";

/// Upload status values.
pub mod upload_status {
    /// Upload is waiting to be processed.
    pub const PENDING: &str = "pending";
    /// Upload is currently being processed into a dataset.
    pub const PROCESSING: &str = "processing";
    /// Upload has been consumed and converted to a dataset.
    pub const CONSUMED: &str = "consumed";
}
