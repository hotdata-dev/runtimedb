//! DuckDB/MotherDuck native driver implementation

use duckdb::Connection;
use std::collections::HashMap;
use urlencoding::encode;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::source::Source;

use super::StreamingParquetWriter;

/// Discover tables and columns from DuckDB/MotherDuck
pub async fn discover_tables(
    source: &Source,
    token: Option<&str>,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let connection_string = resolve_connection_string(source, token)?;
    let catalog = source.catalog().map(|s| s.to_string());

    tokio::task::spawn_blocking(move || {
        discover_tables_sync(&connection_string, catalog.as_deref())
    })
    .await
    .map_err(|e| DataFetchError::Connection(e.to_string()))?
}

/// Build connection string for DuckDB or Motherduck source.
/// For Motherduck, the token parameter must be provided.
pub fn resolve_connection_string(
    source: &Source,
    token: Option<&str>,
) -> Result<String, DataFetchError> {
    match source {
        Source::Duckdb { path } => Ok(path.clone()),
        Source::Motherduck { database, .. } => {
            let token = token.ok_or_else(|| {
                DataFetchError::Connection("Token required for Motherduck".to_string())
            })?;
            Ok(format!(
                "md:{}?motherduck_token={}",
                encode(database),
                encode(token)
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
    token: Option<&str>,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let connection_string = resolve_connection_string(source, token)?;
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

    // Send schema first
    let arrow_schema = arrow_result.get_schema();
    if tx
        .blocking_send(FetchMessage::Schema((*arrow_schema).clone()))
        .is_err()
    {
        return Ok(()); // Receiver dropped
    }

    // Stream batches
    for batch in arrow_result {
        if tx.blocking_send(FetchMessage::Batch(batch)).is_err() {
            break; // Receiver dropped
        }
    }

    Ok(())
}

/// Convert DuckDB type name to Arrow DataType
fn duckdb_type_to_arrow(duckdb_type: &str) -> datafusion::arrow::datatypes::DataType {
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
        "DECIMAL" | "NUMERIC" => DataType::Decimal128(38, 10),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_type_mapping() {
        use datafusion::arrow::datatypes::DataType;

        assert!(matches!(duckdb_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8));
        assert!(matches!(
            duckdb_type_to_arrow("VARCHAR(255)"),
            DataType::Utf8
        ));
        assert!(matches!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(duckdb_type_to_arrow("BIGINT"), DataType::Int64));
    }
}
