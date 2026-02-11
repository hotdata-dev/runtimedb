//! A batch writer that collects batches in memory for later processing.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

use super::batch_writer::{BatchWriteResult, BatchWriter};
use super::DataFetchError;

/// A writer that collects all batches in memory.
/// Used when we need to process the data multiple times (e.g., for projections).
pub struct CollectingBatchWriter {
    schema: Option<Arc<Schema>>,
    batches: Vec<RecordBatch>,
}

impl CollectingBatchWriter {
    pub fn new() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
        }
    }

    /// Get the schema after init() has been called.
    pub fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    /// Consume this writer and return the collected batches.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Total row count across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

impl Default for CollectingBatchWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchWriter for CollectingBatchWriter {
    fn init(&mut self, schema: &Schema) -> Result<(), DataFetchError> {
        self.schema = Some(Arc::new(schema.clone()));
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFetchError> {
        if self.schema.is_none() {
            return Err(DataFetchError::Storage(
                "Writer not initialized - call init() first".into(),
            ));
        }
        self.batches.push(batch.clone());
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<BatchWriteResult, DataFetchError> {
        let rows = self.row_count();
        Ok(BatchWriteResult {
            rows,
            bytes_written: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn test_collecting_writer() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = CollectingBatchWriter::new();
        writer.init(&schema).unwrap();

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

        assert_eq!(writer.row_count(), 5);

        let batches = writer.into_batches();
        assert_eq!(batches.len(), 2);
    }
}
