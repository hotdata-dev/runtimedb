//! A BatchWriter implementation that captures Arrow data in memory for testing.

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use runtimedb::datafetch::{BatchWriteResult, BatchWriter, DataFetchError};
use std::sync::Arc;

/// A BatchWriter implementation that captures Arrow schema and batches in memory.
///
/// This is useful for testing: it allows inspection of the schema and batches
/// produced by a fetch operation without writing to disk.
///
/// # Example
///
/// ```ignore
/// let mut writer = CapturingBatchWriter::new();
/// fetcher.fetch_table(&source, &secrets, None, "public", "my_table", &mut writer).await?;
///
/// // Inspect captured data
/// let schema = writer.schema().unwrap();
/// let batches = writer.batches();
/// ```
#[derive(Debug, Default)]
pub struct CapturingBatchWriter {
    schema: Option<Arc<Schema>>,
    batches: Vec<RecordBatch>,
}

impl CapturingBatchWriter {
    /// Create a new empty capturing writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the captured schema (if init was called).
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref().map(|s| s.as_ref())
    }

    /// Get the captured batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Consume the writer and return the batches.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Get total row count across all batches.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

impl BatchWriter for CapturingBatchWriter {
    fn init(&mut self, schema: &Schema) -> Result<(), DataFetchError> {
        self.schema = Some(Arc::new(schema.clone()));
        self.batches.clear();
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFetchError> {
        if self.schema.is_none() {
            return Err(DataFetchError::Storage(
                "BatchWriter::init must be called before write_batch".to_string(),
            ));
        }
        self.batches.push(batch.clone());
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<BatchWriteResult, DataFetchError> {
        let rows = self.total_rows();
        Ok(BatchWriteResult {
            rows,
            bytes_written: None, // In-memory, no bytes written to disk
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn test_capturing_writer_lifecycle() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = CapturingBatchWriter::new();

        // Init
        writer.init(&schema).unwrap();
        assert!(writer.schema().is_some());
        assert_eq!(writer.batches().len(), 0);

        // Write batches
        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )
        .unwrap();

        writer.write_batch(&batch1).unwrap();
        writer.write_batch(&batch2).unwrap();

        assert_eq!(writer.batches().len(), 2);
        assert_eq!(writer.total_rows(), 5);

        // Close
        let result = Box::new(writer).close().unwrap();
        assert_eq!(result.rows, 5);
        assert!(result.bytes_written.is_none());
    }

    #[test]
    fn test_write_before_init_fails() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();

        let mut writer = CapturingBatchWriter::new();
        let result = writer.write_batch(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_batches() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = CapturingBatchWriter::new();
        writer.init(&schema).unwrap();

        // Write empty batch
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        writer.write_batch(&empty_batch).unwrap();

        assert_eq!(writer.total_rows(), 0);
        assert_eq!(writer.batches().len(), 1);
    }

    #[test]
    fn test_into_batches() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = CapturingBatchWriter::new();
        writer.init(&schema).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();

        let batches = writer.into_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
