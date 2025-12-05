//! Shared streaming Parquet writer for all drivers

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

use crate::datafetch::DataFetchError;

/// Streaming Parquet writer that writes batches incrementally
pub struct StreamingParquetWriter {
    writer: ArrowWriter<File>,
    path: String,
}

impl StreamingParquetWriter {
    /// Create a new streaming writer at the given path
    pub fn new(path: &str, schema: &Schema) -> Result<Self, DataFetchError> {
        let file = File::create(path).map_err(|e| DataFetchError::Storage(e.to_string()))?;
        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), None)
            .map_err(|e| DataFetchError::Storage(e.to_string()))?;
        Ok(Self {
            writer,
            path: path.to_string(),
        })
    }

    /// Write a batch to the Parquet file
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFetchError> {
        self.writer
            .write(batch)
            .map_err(|e| DataFetchError::Storage(e.to_string()))
    }

    /// Finalize the writer and return the path
    pub fn finish(self) -> Result<String, DataFetchError> {
        self.writer
            .close()
            .map_err(|e| DataFetchError::Storage(e.to_string()))?;
        Ok(self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use tempfile::tempdir;

    #[test]
    fn test_streaming_writer_writes_batches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let path_str = path.to_str().unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = StreamingParquetWriter::new(path_str, &schema).unwrap();

        // Write two batches
        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap();

        writer.write_batch(&batch1).unwrap();
        writer.write_batch(&batch2).unwrap();

        let result_path = writer.finish().unwrap();
        assert_eq!(result_path, path_str);
        assert!(path.exists());
    }

    #[test]
    fn test_streaming_writer_empty_batches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        let path_str = path.to_str().unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer = StreamingParquetWriter::new(path_str, &schema).unwrap();

        // Write empty batch
        let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        writer.write_batch(&empty_batch).unwrap();

        let result_path = writer.finish().unwrap();
        assert!(std::path::Path::new(&result_path).exists());
    }
}