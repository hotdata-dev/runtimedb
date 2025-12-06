mod error;
mod fetcher;
pub mod native;
mod types;

pub use error::DataFetchError;
pub use fetcher::{ConnectionConfig, DataFetcher};
pub use native::{NativeFetcher, StreamingParquetWriter};
pub use types::{deserialize_arrow_schema, ColumnMetadata, TableMetadata};