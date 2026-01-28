mod batch_writer;
mod error;
mod fetcher;
pub mod native;
mod orchestrator;
pub(crate) mod types;

pub use batch_writer::{BatchWriteResult, BatchWriter};
pub use error::DataFetchError;
pub use fetcher::DataFetcher;
pub use native::{parse_geoparquet_metadata, NativeFetcher, StreamingParquetWriter};
pub use orchestrator::FetchOrchestrator;
pub use types::{
    deserialize_arrow_schema, extract_geometry_columns, ColumnMetadata, GeometryColumnInfo,
    TableMetadata, GEOMETRY_COLUMNS_METADATA_KEY,
};
