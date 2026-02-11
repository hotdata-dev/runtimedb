//! Centralized streaming Parquet writer with configurable compression

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::{BloomFilterPosition, WriterProperties, WriterVersion};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::datafetch::batch_writer::{BatchWriteResult, BatchWriter};
use crate::datafetch::DataFetchError;

/// Configuration for parquet file writing.
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub max_row_group_size: usize,
    pub bloom_filter_enabled: bool,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: 100_000,
            bloom_filter_enabled: false,
        }
    }
}

/// Build shared writer properties for parquet files (default config).
fn writer_properties() -> WriterProperties {
    writer_properties_from_config(&ParquetConfig::default())
}

/// Build writer properties from a config.
fn writer_properties_from_config(config: &ParquetConfig) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::LZ4)
        .set_max_row_group_size(config.max_row_group_size);

    if config.bloom_filter_enabled {
        builder = builder
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.01)
            .set_bloom_filter_position(BloomFilterPosition::End);
    }

    builder.build()
}

/// Streaming Parquet writer that writes batches incrementally to disk.
///
/// Lifecycle: new(path) -> init(schema) -> write_batch()* -> close()
pub struct StreamingParquetWriter {
    path: PathBuf,
    config: Option<ParquetConfig>,
    writer: Option<ArrowWriter<File>>,
    row_count: usize,
}

impl StreamingParquetWriter {
    /// Create a new writer that will write to the given path.
    /// Call `init()` before writing batches.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            config: None,
            writer: None,
            row_count: 0,
        }
    }

    /// Create a new writer with a custom parquet config.
    pub fn with_config(path: PathBuf, config: ParquetConfig) -> Self {
        Self {
            path,
            config: Some(config),
            writer: None,
            row_count: 0,
        }
    }

    /// Get the path this writer will write to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl BatchWriter for StreamingParquetWriter {
    fn init(&mut self, schema: &Schema) -> Result<(), DataFetchError> {
        // Create parent directories if needed
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DataFetchError::Storage(format!("Failed to create directory: {}", e))
            })?;
        }

        let file = File::create(&self.path)
            .map_err(|e| DataFetchError::Storage(format!("Failed to create file: {}", e)))?;

        let props = match &self.config {
            Some(config) => writer_properties_from_config(config),
            None => writer_properties(),
        };

        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .map_err(|e| DataFetchError::Storage(e.to_string()))?;

        self.writer = Some(writer);
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFetchError> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            DataFetchError::Storage("Writer not initialized - call init() first".into())
        })?;

        self.row_count += batch.num_rows();

        writer
            .write(batch)
            .map_err(|e| DataFetchError::Storage(e.to_string()))
    }

    fn close(mut self: Box<Self>) -> Result<BatchWriteResult, DataFetchError> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| DataFetchError::Storage("Writer not initialized".into()))?;

        writer
            .close()
            .map_err(|e| DataFetchError::Storage(e.to_string()))?;

        // Get file size for bytes_written
        let bytes_written = std::fs::metadata(&self.path).ok().map(|m| m.len());

        Ok(BatchWriteResult {
            rows: self.row_count,
            bytes_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use tempfile::tempdir;

    #[test]
    fn test_streaming_writer_lifecycle() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

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

        let result = writer.close().unwrap();
        assert_eq!(result.rows, 6); // 3 rows from each batch
        assert!(result.bytes_written.is_some());
        assert!(path.exists());
    }

    #[test]
    fn test_streaming_writer_empty_table() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.parquet");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

        // Write empty batch
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        writer.write_batch(&empty_batch).unwrap();

        let result = writer.close().unwrap();
        assert_eq!(result.rows, 0);
        assert!(path.exists());
    }

    #[test]
    fn test_write_batch_before_init_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path));

        // Attempt to write without calling init() first
        let result = writer.write_batch(&batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));
    }

    #[test]
    fn test_close_before_init_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path));

        // Attempt to close without calling init() first
        let result = writer.close();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));
    }

    #[test]
    fn test_schema_only_no_batches_written() {
        // Tests the case where init() is called but no batches are written before close().
        // This can happen when DataFusion returns an empty Vec<RecordBatch>.
        let dir = tempdir().unwrap();
        let path = dir.path().join("schema_only.parquet");

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

        // Close without writing any batches
        let result = writer.close().unwrap();
        assert_eq!(result.rows, 0);
        assert!(path.exists());

        // Verify the parquet file is readable and has the correct schema
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;

        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_schema = reader.metadata().file_metadata().schema_descr();

        // Should have 2 columns matching our schema
        assert_eq!(parquet_schema.num_columns(), 2);
        assert_eq!(parquet_schema.column(0).name(), "id");
        assert_eq!(parquet_schema.column(1).name(), "name");
    }

    #[test]
    fn test_lz4_compression() {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;

        let dir = tempdir().unwrap();
        let path = dir.path().join("compressed.parquet");

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

        // Write some data
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    Some("Alice"),
                    Some("Bob"),
                    Some("Charlie"),
                    Some("David"),
                    Some("Eve"),
                ])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        // Read back and verify LZ4 compression
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        for row_group in metadata.row_groups() {
            for column in row_group.columns() {
                let compression = column.compression();
                assert_eq!(
                    compression,
                    datafusion::parquet::basic::Compression::LZ4,
                    "Expected LZ4, got {:?}",
                    compression
                );
            }
        }
    }
}
