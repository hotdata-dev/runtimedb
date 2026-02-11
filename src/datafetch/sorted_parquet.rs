//! Utilities for writing sorted and unsorted parquet files.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::*;
use std::path::Path;
use std::sync::Arc;

use super::batch_writer::BatchWriter;
use super::native::{ParquetConfig, StreamingParquetWriter};
use super::DataFetchError;

/// Create a StreamingParquetWriter with optional config.
fn make_writer(path: std::path::PathBuf, config: Option<&ParquetConfig>) -> Box<dyn BatchWriter> {
    match config {
        Some(c) => Box::new(StreamingParquetWriter::with_config(path, c.clone())),
        None => Box::new(StreamingParquetWriter::new(path)),
    }
}

/// Write batches to a parquet file, sorted by specified columns.
pub async fn write_sorted_parquet(
    batches: &[RecordBatch],
    schema: &Schema,
    sort_columns: &[String],
    output_path: &Path,
    parquet_config: Option<&ParquetConfig>,
) -> Result<usize, DataFetchError> {
    if batches.is_empty() {
        // Write empty file with schema
        let mut writer = make_writer(output_path.to_path_buf(), parquet_config);
        writer.init(schema)?;
        let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        writer.write_batch(&empty_batch)?;
        writer.close()?;
        return Ok(0);
    }

    // Create a DataFusion context to do the sorting
    let ctx = SessionContext::new();

    // Create a memory table from batches
    let schema_ref = Arc::new(schema.clone());
    let mem_table =
        datafusion::datasource::MemTable::try_new(schema_ref.clone(), vec![batches.to_vec()])
            .map_err(|e| {
                DataFetchError::Storage(format!("Failed to create memory table: {}", e))
            })?;

    ctx.register_table("data", Arc::new(mem_table))
        .map_err(|e| DataFetchError::Storage(format!("Failed to register table: {}", e)))?;

    // Build sort expression
    let sort_exprs: Vec<SortExpr> = sort_columns
        .iter()
        .map(|c| col(c).sort(true, true)) // ASC, NULLS FIRST
        .collect();

    // Execute sorted query
    let df = ctx
        .table("data")
        .await
        .map_err(|e| DataFetchError::Storage(format!("Failed to get table: {}", e)))?
        .sort(sort_exprs)
        .map_err(|e| DataFetchError::Storage(format!("Failed to sort: {}", e)))?;

    let sorted_batches = df
        .collect()
        .await
        .map_err(|e| DataFetchError::Storage(format!("Failed to collect sorted data: {}", e)))?;

    // Write sorted batches to parquet
    let mut writer = make_writer(output_path.to_path_buf(), parquet_config);
    writer.init(schema)?;

    let mut row_count = 0;
    for batch in &sorted_batches {
        row_count += batch.num_rows();
        writer.write_batch(batch)?;
    }

    writer.close()?;
    Ok(row_count)
}

/// Write batches to a parquet file without sorting.
pub fn write_parquet(
    batches: &[RecordBatch],
    schema: &Schema,
    output_path: &Path,
    parquet_config: Option<&ParquetConfig>,
) -> Result<usize, DataFetchError> {
    let mut writer = make_writer(output_path.to_path_buf(), parquet_config);
    writer.init(schema)?;

    if batches.is_empty() {
        let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        writer.write_batch(&empty_batch)?;
        writer.close()?;
        return Ok(0);
    }

    let mut row_count = 0;
    for batch in batches {
        row_count += batch.num_rows();
        writer.write_batch(batch)?;
    }

    writer.close()?;
    Ok(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_sorted_parquet() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sorted.parquet");

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        // Create unsorted data
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        )
        .unwrap();

        let rows = write_sorted_parquet(&[batch], &schema, &["id".to_string()], &path, None)
            .await
            .unwrap();

        assert_eq!(rows, 3);
        assert!(path.exists());

        // Read back and verify sorted order
        let ctx = SessionContext::new();
        ctx.register_parquet("test", path.to_str().unwrap(), Default::default())
            .await
            .unwrap();

        let df = ctx.sql("SELECT id FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();

        let ids: Vec<i32> = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(ids, vec![1, 2, 3], "Data should be sorted by id");
    }

    #[tokio::test]
    async fn test_write_sorted_parquet_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.parquet");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let rows = write_sorted_parquet(&[], &schema, &["id".to_string()], &path, None)
            .await
            .unwrap();

        assert_eq!(rows, 0);
        assert!(path.exists());
    }

    #[test]
    fn test_write_parquet() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unsorted.parquet");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![3, 1, 2]))],
        )
        .unwrap();

        let rows = write_parquet(&[batch], &schema, &path, None).unwrap();

        assert_eq!(rows, 3);
        assert!(path.exists());
    }
}
