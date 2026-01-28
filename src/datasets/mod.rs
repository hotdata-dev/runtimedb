//! Dataset management and utilities.

pub mod error;
pub mod schema;
pub mod validation;

pub use error::DatasetError;
pub use schema::{
    build_schema_from_columns, build_schema_from_columns_for_json,
    build_schema_from_columns_unchecked, parse_column_type, ColumnTypeError, SchemaError,
};
pub use validation::*;

/// The default schema name for datasets.
pub const DEFAULT_SCHEMA: &str = "main";

/// Upload status values.
pub mod upload_status {
    /// Upload is waiting to be processed.
    pub const PENDING: &str = "pending";
    /// Upload is currently being processed into a dataset.
    pub const PROCESSING: &str = "processing";
    /// Upload has been consumed and converted to a dataset.
    pub const CONSUMED: &str = "consumed";
}
