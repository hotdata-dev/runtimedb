//! DuckDB/MotherDuck native driver implementation
//!
//! Note: Due to arrow version mismatch between duckdb (arrow 56) and datafusion (arrow 57),
//! we use IPC serialization to bridge the versions.

use duckdb::Connection;
use std::collections::HashMap;
use urlencoding::encode;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

// Arrow 56 IPC for duckdb compatibility (duckdb uses arrow 56)
use arrow_ipc_56 as arrow56_ipc;

/// Check connectivity to a DuckDB/MotherDuck source
pub async fn check_health(source: &Source, secrets: &SecretManager) -> Result<(), DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&connection_string)
            .map_err(|e| DataFetchError::Connection(e.to_string()))?;
        conn.execute("SELECT 1", [])
            .map_err(|e| DataFetchError::Query(e.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|e| DataFetchError::Connection(e.to_string()))?
}

/// Discover tables and columns from DuckDB/MotherDuck
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let catalog = source.catalog().map(|s| s.to_string());

    tokio::task::spawn_blocking(move || {
        discover_tables_sync(&connection_string, catalog.as_deref())
    })
    .await
    .map_err(|e| DataFetchError::Connection(e.to_string()))?
}

/// Resolve credentials and build connection string for DuckDB or Motherduck source.
pub async fn resolve_connection_string(
    source: &Source,
    secrets: &SecretManager,
) -> Result<String, DataFetchError> {
    match source {
        Source::Duckdb { path } => Ok(path.clone()),
        Source::Motherduck {
            database,
            credential,
        } => {
            let token = credential
                .resolve(secrets)
                .await
                .map_err(|e| DataFetchError::Connection(e.to_string()))?;
            Ok(format!(
                "md:{}?motherduck_token={}",
                encode(database),
                encode(&token)
            ))
        }
        _ => Err(DataFetchError::Connection(
            "Expected DuckDB or Motherduck source".to_string(),
        )),
    }
}

fn discover_tables_sync(
    connection_string: &str,
    catalog: Option<&str>,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let conn = Connection::open(connection_string)
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    let query = r#"
        SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.table_type,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.ordinal_position
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
          AND ($1 IS NULL OR t.table_catalog = $1)
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        "#;

    let mut stmt = conn
        .prepare(query)
        .map_err(|e| DataFetchError::Discovery(e.to_string()))?;

    let rows = stmt
        .query_map([&catalog], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?, // catalog
                row.get::<_, String>(1)?,         // schema
                row.get::<_, String>(2)?,         // table
                row.get::<_, String>(3)?,         // table_type
                row.get::<_, String>(4)?,         // column_name
                row.get::<_, String>(5)?,         // data_type
                row.get::<_, String>(6)?,         // is_nullable
                row.get::<_, i32>(7)?,            // ordinal_position
            ))
        })
        .map_err(|e| DataFetchError::Discovery(e.to_string()))?;

    let mut table_map: HashMap<(Option<String>, String, String), TableMetadata> = HashMap::new();

    for row_result in rows {
        let (catalog, schema, table, table_type, col_name, data_type, is_nullable, ordinal) =
            row_result.map_err(|e| DataFetchError::Discovery(e.to_string()))?;

        let column = ColumnMetadata {
            name: col_name,
            data_type: duckdb_type_to_arrow(&data_type),
            nullable: is_nullable.eq_ignore_ascii_case("YES"),
            ordinal_position: ordinal,
        };

        let key = (catalog.clone(), schema.clone(), table.clone());

        table_map
            .entry(key)
            .and_modify(|t| t.columns.push(column.clone()))
            .or_insert_with(|| TableMetadata {
                catalog_name: catalog,
                schema_name: schema,
                table_name: table,
                table_type,
                columns: vec![column],
            });
    }

    let tables: Vec<TableMetadata> = table_map.into_values().collect();

    Ok(tables)
}

/// Message sent from blocking task to async writer
enum FetchMessage {
    Schema(datafusion::arrow::datatypes::Schema),
    Batch(datafusion::arrow::record_batch::RecordBatch),
}

/// Fetch table data and write to Parquet using streaming to avoid OOM on large tables
pub async fn fetch_table(
    source: &Source,
    secrets: &SecretManager,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut dyn BatchWriter,
) -> Result<(), DataFetchError> {
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let connection_string = resolve_connection_string(source, secrets).await?;
    let schema = schema.to_string();
    let table = table.to_string();

    // Channel to stream batches from blocking task
    let (tx, mut rx) = tokio::sync::mpsc::channel::<FetchMessage>(4);

    // Spawn blocking task to fetch data
    let handle = tokio::task::spawn_blocking(move || {
        fetch_table_to_channel(&connection_string, &schema, &table, tx)
    });

    // Receive schema first
    let arrow_schema = match rx.recv().await {
        Some(FetchMessage::Schema(s)) => s,
        Some(FetchMessage::Batch(_)) => {
            return Err(DataFetchError::Query("Expected schema, got batch".into()))
        }
        None => return Err(DataFetchError::Query("Channel closed before schema".into())),
    };

    writer.init(&arrow_schema)?;

    // Receive and write batches
    let mut wrote_any = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            FetchMessage::Batch(batch) => {
                writer.write_batch(&batch)?;
                wrote_any = true;
            }
            FetchMessage::Schema(_) => {
                return Err(DataFetchError::Query("Unexpected schema message".into()))
            }
        }
    }

    // Handle empty table
    if !wrote_any {
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        writer.write_batch(&empty_batch)?;
    }

    // Wait for blocking task to complete and propagate any errors
    handle
        .await
        .map_err(|e| DataFetchError::Query(e.to_string()))??;

    Ok(())
}

fn fetch_table_to_channel(
    connection_string: &str,
    schema: &str,
    table: &str,
    tx: tokio::sync::mpsc::Sender<FetchMessage>,
) -> Result<(), DataFetchError> {
    let conn = Connection::open(connection_string)
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    let query = format!(
        "SELECT * FROM \"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );

    let mut stmt = conn
        .prepare(&query)
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    let arrow_result = stmt
        .query_arrow([])
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    // Send schema first (convert from arrow 56 to arrow 57)
    let duckdb_schema = arrow_result.get_schema();
    let datafusion_schema = convert_arrow_schema(duckdb_schema)?;
    if tx
        .blocking_send(FetchMessage::Schema(datafusion_schema))
        .is_err()
    {
        return Ok(()); // Receiver dropped
    }

    // Stream batches (convert from arrow 56 to arrow 57)
    for batch in arrow_result {
        let converted_batch = convert_arrow_batch(&batch)?;
        if tx
            .blocking_send(FetchMessage::Batch(converted_batch))
            .is_err()
        {
            break; // Receiver dropped
        }
    }

    Ok(())
}

/// Convert an arrow 56 Schema (from duckdb) to datafusion arrow Schema (arrow 57)
/// using IPC serialization as a bridge between arrow versions.
fn convert_arrow_schema(
    duckdb_schema: std::sync::Arc<duckdb::arrow::datatypes::Schema>,
) -> Result<datafusion::arrow::datatypes::Schema, DataFetchError> {
    // Create an empty batch with the schema to serialize, then convert
    let empty_batch = duckdb::arrow::record_batch::RecordBatch::new_empty(duckdb_schema);
    let converted = convert_arrow_batch(&empty_batch)?;
    Ok((*converted.schema()).clone())
}

/// Convert an arrow 56 RecordBatch (from duckdb) to datafusion arrow RecordBatch (arrow 57)
/// using IPC serialization as a bridge between arrow versions.
fn convert_arrow_batch(
    duckdb_batch: &duckdb::arrow::record_batch::RecordBatch,
) -> Result<datafusion::arrow::record_batch::RecordBatch, DataFetchError> {
    use arrow56_ipc::writer::StreamWriter as Arrow56StreamWriter;
    use datafusion::arrow::ipc::reader::StreamReader as DatafusionStreamReader;

    // Serialize with arrow 56
    let mut buffer = Vec::new();
    {
        let mut stream_writer =
            Arrow56StreamWriter::try_new(&mut buffer, duckdb_batch.schema().as_ref())
                .map_err(|e| DataFetchError::Query(format!("IPC write error: {}", e)))?;
        stream_writer
            .write(duckdb_batch)
            .map_err(|e| DataFetchError::Query(format!("IPC write error: {}", e)))?;
        stream_writer
            .finish()
            .map_err(|e| DataFetchError::Query(format!("IPC finish error: {}", e)))?;
    }

    // Deserialize with datafusion's arrow (57)
    let cursor = std::io::Cursor::new(buffer);
    let mut stream_reader = DatafusionStreamReader::try_new(cursor, None)
        .map_err(|e| DataFetchError::Query(format!("IPC read error: {}", e)))?;

    stream_reader
        .next()
        .ok_or_else(|| DataFetchError::Query("Empty IPC stream".to_string()))?
        .map_err(|e| DataFetchError::Query(format!("IPC read error: {}", e)))
}

/// Convert DuckDB type name to Arrow DataType.
///
/// This is the authoritative type mapping implementation. Tests validate against this.
pub fn duckdb_type_to_arrow(duckdb_type: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    let type_upper = duckdb_type.to_uppercase();

    // Handle parameterized types by extracting base type
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "INT1" => DataType::Int8,
        "SMALLINT" | "INT2" => DataType::Int16,
        "INTEGER" | "INT" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,
        "UTINYINT" => DataType::UInt8,
        "USMALLINT" => DataType::UInt16,
        "UINTEGER" => DataType::UInt32,
        "UBIGINT" => DataType::UInt64,
        "REAL" | "FLOAT4" | "FLOAT" => DataType::Float32,
        "DOUBLE" | "FLOAT8" => DataType::Float64,
        "DECIMAL" | "NUMERIC" => parse_decimal_params(&type_upper),
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => DataType::Utf8,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" | "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "UUID" => DataType::Utf8,
        "JSON" => DataType::Utf8,
        "INTERVAL" => DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        _ => DataType::Utf8, // Default fallback
    }
}

/// Parse DECIMAL(precision, scale) parameters from type string.
/// Returns Decimal128 with extracted parameters, or defaults to (38, 10) if parsing fails.
fn parse_decimal_params(type_str: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    // Use shared parsing logic, fall back to DuckDB's default of (38, 10)
    super::parse_decimal_params(type_str).unwrap_or(DataType::Decimal128(38, 10))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

    // =========================================================================
    // Boolean types
    // =========================================================================

    #[test]
    fn test_duckdb_boolean_types() {
        assert!(matches!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(duckdb_type_to_arrow("BOOL"), DataType::Boolean));
    }

    // =========================================================================
    // Integer types - Signed
    // =========================================================================

    #[test]
    fn test_duckdb_tinyint_type() {
        assert!(matches!(duckdb_type_to_arrow("TINYINT"), DataType::Int8));
        assert!(matches!(duckdb_type_to_arrow("INT1"), DataType::Int8));
    }

    #[test]
    fn test_duckdb_smallint_type() {
        assert!(matches!(duckdb_type_to_arrow("SMALLINT"), DataType::Int16));
        assert!(matches!(duckdb_type_to_arrow("INT2"), DataType::Int16));
    }

    #[test]
    fn test_duckdb_integer_type() {
        assert!(matches!(duckdb_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(duckdb_type_to_arrow("INT"), DataType::Int32));
        assert!(matches!(duckdb_type_to_arrow("INT4"), DataType::Int32));
    }

    #[test]
    fn test_duckdb_bigint_type() {
        assert!(matches!(duckdb_type_to_arrow("BIGINT"), DataType::Int64));
        assert!(matches!(duckdb_type_to_arrow("INT8"), DataType::Int64));
    }

    #[test]
    fn test_duckdb_hugeint_type() {
        // HUGEINT is not explicitly handled, falls back to Utf8
        assert!(matches!(duckdb_type_to_arrow("HUGEINT"), DataType::Utf8));
    }

    // =========================================================================
    // Integer types - Unsigned
    // =========================================================================

    #[test]
    fn test_duckdb_utinyint_type() {
        assert!(matches!(duckdb_type_to_arrow("UTINYINT"), DataType::UInt8));
    }

    #[test]
    fn test_duckdb_usmallint_type() {
        assert!(matches!(
            duckdb_type_to_arrow("USMALLINT"),
            DataType::UInt16
        ));
    }

    #[test]
    fn test_duckdb_uinteger_type() {
        assert!(matches!(duckdb_type_to_arrow("UINTEGER"), DataType::UInt32));
    }

    #[test]
    fn test_duckdb_ubigint_type() {
        assert!(matches!(duckdb_type_to_arrow("UBIGINT"), DataType::UInt64));
    }

    // =========================================================================
    // Float types
    // =========================================================================

    #[test]
    fn test_duckdb_float_types() {
        assert!(matches!(duckdb_type_to_arrow("FLOAT"), DataType::Float32));
        assert!(matches!(duckdb_type_to_arrow("REAL"), DataType::Float32));
        assert!(matches!(duckdb_type_to_arrow("FLOAT4"), DataType::Float32));
    }

    #[test]
    fn test_duckdb_double_types() {
        assert!(matches!(duckdb_type_to_arrow("DOUBLE"), DataType::Float64));
        assert!(matches!(duckdb_type_to_arrow("FLOAT8"), DataType::Float64));
    }

    // =========================================================================
    // Decimal types
    // NOTE: Core decimal parsing is tested in mod.rs::tests (parse_decimal_params).
    // This test verifies DuckDB-specific fallback behavior only.
    // =========================================================================

    #[test]
    fn test_duckdb_decimal_types_unconstrained() {
        // Unconstrained DECIMAL/NUMERIC defaults to (38, 10) per DuckDB spec
        match duckdb_type_to_arrow("DECIMAL") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 38);
                assert_eq!(scale, 10);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }

        match duckdb_type_to_arrow("NUMERIC") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 38);
                assert_eq!(scale, 10);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    // =========================================================================
    // String types
    // =========================================================================

    #[test]
    fn test_duckdb_varchar_types() {
        assert!(matches!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8));
        assert!(matches!(
            duckdb_type_to_arrow("VARCHAR(255)"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_duckdb_text_type() {
        assert!(matches!(duckdb_type_to_arrow("TEXT"), DataType::Utf8));
    }

    #[test]
    fn test_duckdb_string_type() {
        assert!(matches!(duckdb_type_to_arrow("STRING"), DataType::Utf8));
    }

    #[test]
    fn test_duckdb_char_types() {
        assert!(matches!(duckdb_type_to_arrow("CHAR"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("CHAR(10)"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("BPCHAR"), DataType::Utf8));
    }

    // =========================================================================
    // Binary types
    // =========================================================================

    #[test]
    fn test_duckdb_binary_types() {
        assert!(matches!(duckdb_type_to_arrow("BLOB"), DataType::Binary));
        assert!(matches!(duckdb_type_to_arrow("BYTEA"), DataType::Binary));
        assert!(matches!(duckdb_type_to_arrow("BINARY"), DataType::Binary));
        assert!(matches!(
            duckdb_type_to_arrow("VARBINARY"),
            DataType::Binary
        ));
    }

    // =========================================================================
    // Date/Time types
    // =========================================================================

    #[test]
    fn test_duckdb_date_type() {
        assert!(matches!(duckdb_type_to_arrow("DATE"), DataType::Date32));
    }

    #[test]
    fn test_duckdb_time_type() {
        match duckdb_type_to_arrow("TIME") {
            DataType::Time64(unit) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
            }
            other => panic!("Expected Time64, got {:?}", other),
        }
    }

    #[test]
    fn test_duckdb_timestamp_type() {
        match duckdb_type_to_arrow("TIMESTAMP") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }

        match duckdb_type_to_arrow("DATETIME") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_duckdb_timestamptz_type() {
        match duckdb_type_to_arrow("TIMESTAMPTZ") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }

        match duckdb_type_to_arrow("TIMESTAMP WITH TIME ZONE") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }
    }

    #[test]
    fn test_duckdb_interval_type() {
        match duckdb_type_to_arrow("INTERVAL") {
            DataType::Interval(unit) => {
                assert!(matches!(unit, IntervalUnit::MonthDayNano));
            }
            other => panic!("Expected Interval, got {:?}", other),
        }
    }

    // =========================================================================
    // UUID type
    // =========================================================================

    #[test]
    fn test_duckdb_uuid_type() {
        assert!(matches!(duckdb_type_to_arrow("UUID"), DataType::Utf8));
    }

    // =========================================================================
    // JSON type
    // =========================================================================

    #[test]
    fn test_duckdb_json_type() {
        assert!(matches!(duckdb_type_to_arrow("JSON"), DataType::Utf8));
    }

    // =========================================================================
    // Unknown types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_duckdb_unknown_type_fallback() {
        assert!(matches!(
            duckdb_type_to_arrow("UNKNOWN_TYPE"),
            DataType::Utf8
        ));
        assert!(matches!(
            duckdb_type_to_arrow("CUSTOM_TYPE"),
            DataType::Utf8
        ));
        assert!(matches!(duckdb_type_to_arrow("LIST"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("STRUCT"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("MAP"), DataType::Utf8));
    }

    // =========================================================================
    // Case insensitivity
    // =========================================================================

    #[test]
    fn test_duckdb_case_insensitivity() {
        // DuckDB uses uppercase internally, but should handle lowercase too
        assert!(matches!(duckdb_type_to_arrow("boolean"), DataType::Boolean));
        assert!(matches!(duckdb_type_to_arrow("Boolean"), DataType::Boolean));
        assert!(matches!(duckdb_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(duckdb_type_to_arrow("Integer"), DataType::Int32));
        assert!(matches!(duckdb_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("VarChar"), DataType::Utf8));
        assert!(matches!(duckdb_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(duckdb_type_to_arrow("BigInt"), DataType::Int64));
    }

    // =========================================================================
    // Parameterized types (with size info)
    // =========================================================================

    #[test]
    fn test_duckdb_parameterized_types() {
        // Ensure types with size parameters are handled correctly
        assert!(matches!(
            duckdb_type_to_arrow("VARCHAR(255)"),
            DataType::Utf8
        ));
        assert!(matches!(duckdb_type_to_arrow("CHAR(10)"), DataType::Utf8));

        // Decimal with parameters
        match duckdb_type_to_arrow("DECIMAL(18,4)") {
            DataType::Decimal128(_, _) => {}
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }
}
