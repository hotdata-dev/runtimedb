use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

use super::DataFetchError;

/// Summary returned when a BatchWriter is closed.
#[derive(Debug, Clone)]
pub struct BatchWriteResult {
    /// Total number of rows written.
    pub rows: usize,
    /// Total bytes written (if known).
    pub bytes_written: Option<u64>,
}

/// A trait for writing Arrow RecordBatches to storage.
///
/// Implementors must follow this lifecycle:
/// 1. `init(schema)` - Initialize with the Arrow schema (must be called first)
/// 2. `write_batch(batch)` - Write batches (can be called zero or more times)
/// 3. `close()` - Finalize and return metadata (consumes the writer)
///
/// All methods are synchronous. When used in async contexts, callers should
/// ensure writes are batched to minimize blocking time.
pub trait BatchWriter: Send {
    /// Initialize the writer with the schema for the data to be written.
    fn init(&mut self, schema: &Schema) -> Result<(), DataFetchError>;

    /// Write a single RecordBatch. May be called multiple times.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFetchError>;

    /// Close the writer and return write statistics.
    /// Consumes self to enforce the lifecycle.
    fn close(self: Box<Self>) -> Result<BatchWriteResult, DataFetchError>;
}
