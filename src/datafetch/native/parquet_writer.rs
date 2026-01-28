//! Centralized streaming Parquet writer with configurable compression and GeoParquet support

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::{BloomFilterPosition, WriterProperties, WriterVersion};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::datafetch::batch_writer::{BatchWriteResult, BatchWriter};
use crate::datafetch::types::GeometryColumnInfo;
use crate::datafetch::DataFetchError;

/// Streaming Parquet writer that writes batches incrementally to disk.
/// Supports GeoParquet metadata for geometry columns.
///
/// Lifecycle: new(path) -> set_geometry_columns() (optional) -> init(schema) -> write_batch()* -> close()
pub struct StreamingParquetWriter {
    path: PathBuf,
    writer: Option<ArrowWriter<File>>,
    row_count: usize,
    /// Geometry column info for GeoParquet metadata
    geometry_columns: HashMap<String, GeometryColumnInfo>,
}

impl StreamingParquetWriter {
    /// Create a new writer that will write to the given path.
    /// Call `init()` before writing batches.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            writer: None,
            row_count: 0,
            geometry_columns: HashMap::new(),
        }
    }

    /// Get the path this writer will write to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// GeoParquet metadata structure (version 1.1.0)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeoParquetMetadata {
    version: String,
    primary_column: String,
    columns: HashMap<String, GeoColumnMetadata>,
}

/// Per-column GeoParquet metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeoColumnMetadata {
    encoding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    geometry_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    crs: Option<CrsMetadata>,
}

/// CRS metadata in PROJJSON-style format (simplified for EPSG codes)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrsMetadata {
    id: CrsId,
}

/// CRS identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrsId {
    authority: String,
    code: i32,
}

impl GeoParquetMetadata {
    /// Create GeoParquet metadata from geometry column info
    fn from_geometry_columns(columns: &HashMap<String, GeometryColumnInfo>) -> Option<Self> {
        if columns.is_empty() {
            return None;
        }

        // Pick the alphabetically first geometry column as primary for determinism
        let primary_column = columns.keys().min()?.clone();

        let geo_columns: HashMap<String, GeoColumnMetadata> = columns
            .iter()
            .map(|(name, info)| {
                let crs = if info.srid > 0 {
                    Some(CrsMetadata {
                        id: CrsId {
                            authority: "EPSG".to_string(),
                            code: info.srid,
                        },
                    })
                } else {
                    None
                };

                let geometry_types = info
                    .geometry_type
                    .as_ref()
                    .map(|t| vec![normalize_geometry_type(t)]);

                (
                    name.clone(),
                    GeoColumnMetadata {
                        encoding: "WKB".to_string(),
                        geometry_types,
                        crs,
                    },
                )
            })
            .collect();

        Some(GeoParquetMetadata {
            version: "1.1.0".to_string(),
            primary_column,
            columns: geo_columns,
        })
    }

    /// Serialize to JSON string
    fn to_json(&self) -> Result<String, DataFetchError> {
        serde_json::to_string(self).map_err(|e| {
            DataFetchError::Storage(format!("Failed to serialize GeoParquet metadata: {}", e))
        })
    }
}

/// Extract GeometryColumnInfo from GeoParquet metadata JSON string.
/// This parses the "geo" key-value metadata from a GeoParquet file.
pub fn parse_geoparquet_metadata(geo_json: &str) -> HashMap<String, GeometryColumnInfo> {
    #[derive(Deserialize)]
    struct GeoMeta {
        columns: HashMap<String, GeoColMeta>,
    }

    #[derive(Deserialize)]
    struct GeoColMeta {
        #[serde(default)]
        geometry_types: Option<Vec<String>>,
        crs: Option<CrsMeta>,
    }

    #[derive(Deserialize)]
    struct CrsMeta {
        id: Option<CrsIdMeta>,
    }

    #[derive(Deserialize)]
    struct CrsIdMeta {
        #[serde(default)]
        code: i32,
    }

    let Ok(geo_meta) = serde_json::from_str::<GeoMeta>(geo_json) else {
        return HashMap::new();
    };

    geo_meta
        .columns
        .into_iter()
        .map(|(name, col)| {
            let srid = col.crs.and_then(|c| c.id).map(|id| id.code).unwrap_or(0);
            let geometry_type = col
                .geometry_types
                .and_then(|types| types.into_iter().next());

            (
                name,
                GeometryColumnInfo {
                    srid,
                    geometry_type,
                },
            )
        })
        .collect()
}

use super::super::types::normalize_geometry_type;

impl BatchWriter for StreamingParquetWriter {
    fn set_geometry_columns(&mut self, columns: HashMap<String, GeometryColumnInfo>) {
        self.geometry_columns = columns;
    }

    fn init(&mut self, schema: &Schema) -> Result<(), DataFetchError> {
        // Create parent directories if needed
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DataFetchError::Storage(format!("Failed to create directory: {}", e))
            })?;
        }

        let file = File::create(&self.path)
            .map_err(|e| DataFetchError::Storage(format!("Failed to create file: {}", e)))?;

        // Build writer properties with bloom filters and optional GeoParquet metadata
        let mut props_builder = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::LZ4)
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.01)
            .set_bloom_filter_position(BloomFilterPosition::End);

        // Add GeoParquet metadata if geometry columns are present
        if let Some(geo_meta) = GeoParquetMetadata::from_geometry_columns(&self.geometry_columns) {
            let geo_json = geo_meta.to_json()?;
            props_builder = props_builder.set_key_value_metadata(Some(vec![
                datafusion::parquet::file::metadata::KeyValue::new("geo".to_string(), geo_json),
            ]));
        }

        let props = props_builder.build();

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
    use crate::datafetch::types::GeometryColumnInfo;
    use datafusion::arrow::array::{BinaryArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
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

    #[test]
    fn test_bloom_filter_enabled() {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;

        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom_filter.parquet");

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

        // Write some data with distinct values to trigger bloom filter creation
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    Some("Alice"),
                    Some("Bob"),
                    Some("Charlie"),
                    Some("David"),
                    Some("Eve"),
                    Some("Frank"),
                    Some("Grace"),
                    Some("Henry"),
                    Some("Ivy"),
                    Some("Jack"),
                ])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        // Read back and verify bloom filters exist
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        // Check that bloom filter offsets are set for columns
        let mut bloom_filter_found = false;
        for row_group in metadata.row_groups() {
            for column in row_group.columns() {
                if column.bloom_filter_offset().is_some() {
                    bloom_filter_found = true;
                    break;
                }
            }
        }

        assert!(
            bloom_filter_found,
            "Expected bloom filter to be present in parquet metadata"
        );
    }

    #[test]
    fn test_geoparquet_metadata() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("geoparquet.parquet");

        // Schema with geometry column
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("geom", DataType::Binary, true),
        ]);

        // Create writer with geometry column info
        let mut writer = StreamingParquetWriter::new(path.clone());

        // Set geometry column metadata
        let mut geom_cols = HashMap::new();
        geom_cols.insert(
            "geom".to_string(),
            GeometryColumnInfo {
                srid: 4326,
                geometry_type: Some("Point".to_string()),
            },
        );
        writer.set_geometry_columns(geom_cols);

        // Init and write some data
        writer.init(&schema).unwrap();

        // Simple WKB for POINT(0 0)
        let wkb_point: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type = Point (1)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
        ];

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(BinaryArray::from(vec![Some(wkb_point.as_slice())])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        Box::new(writer).close().unwrap();

        // Verify GeoParquet metadata was written
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let file_metadata = reader.metadata().file_metadata();

        // Find the "geo" key in file metadata
        let geo_metadata = file_metadata
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|item| item.key == "geo"));

        assert!(
            geo_metadata.is_some(),
            "GeoParquet 'geo' metadata not found"
        );

        let geo_value = geo_metadata.unwrap().value.as_ref().unwrap();

        // Parse and verify the GeoParquet metadata
        let parsed: serde_json::Value = serde_json::from_str(geo_value).unwrap();

        assert_eq!(parsed["version"], "1.1.0");
        assert_eq!(parsed["primary_column"], "geom");
        assert_eq!(parsed["columns"]["geom"]["encoding"], "WKB");
        assert_eq!(parsed["columns"]["geom"]["crs"]["id"]["authority"], "EPSG");
        assert_eq!(parsed["columns"]["geom"]["crs"]["id"]["code"], 4326);
    }

    #[test]
    fn test_geoparquet_metadata_serialization() {
        let mut columns = HashMap::new();
        columns.insert(
            "the_geom".to_string(),
            GeometryColumnInfo {
                srid: 4326,
                geometry_type: Some("Polygon".to_string()),
            },
        );

        let geo_meta = GeoParquetMetadata::from_geometry_columns(&columns);
        assert!(geo_meta.is_some());

        let meta = geo_meta.unwrap();
        assert_eq!(meta.version, "1.1.0");
        assert_eq!(meta.primary_column, "the_geom");

        let json = meta.to_json().unwrap();
        assert!(json.contains("\"version\":\"1.1.0\""));
        assert!(json.contains("\"encoding\":\"WKB\""));
        assert!(json.contains("\"EPSG\""));
        assert!(json.contains("4326"));
    }

    #[test]
    fn test_no_geoparquet_metadata_without_geometry_columns() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("regular.parquet");

        // Schema without geometry column
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(path.clone()));
        writer.init(&schema).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        // Verify no GeoParquet metadata
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let file_metadata = reader.metadata().file_metadata();

        let geo_metadata = file_metadata
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|item| item.key == "geo"));

        assert!(
            geo_metadata.is_none(),
            "Regular parquet should not have 'geo' metadata"
        );
    }

    #[test]
    fn test_primary_column_deterministic_with_multiple_geometry_columns() {
        // When multiple geometry columns exist, primary_column must be
        // deterministic (alphabetically first), not random HashMap order.
        let mut columns = HashMap::new();
        columns.insert(
            "z_geom".to_string(),
            GeometryColumnInfo {
                srid: 4326,
                geometry_type: Some("Point".to_string()),
            },
        );
        columns.insert(
            "a_geom".to_string(),
            GeometryColumnInfo {
                srid: 4326,
                geometry_type: Some("Polygon".to_string()),
            },
        );
        columns.insert(
            "m_geom".to_string(),
            GeometryColumnInfo {
                srid: 0,
                geometry_type: None,
            },
        );

        // Run multiple times to catch nondeterminism
        for _ in 0..20 {
            let meta = GeoParquetMetadata::from_geometry_columns(&columns).unwrap();
            assert_eq!(
                meta.primary_column, "a_geom",
                "primary_column should always be alphabetically first"
            );
        }
    }
}
