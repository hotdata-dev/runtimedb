//! DuckDB/MotherDuck native driver implementation

use duckdb::Connection;
use std::collections::HashMap;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::source::Source;

use super::StreamingParquetWriter;

/// Discover tables and columns from DuckDB/MotherDuck
pub async fn discover_tables(source: &Source) -> Result<Vec<TableMetadata>, DataFetchError> {
    let connection_string = source.connection_string();
    let catalog = source.catalog().map(|s| s.to_string());

    tokio::task::spawn_blocking(move || {
        discover_tables_sync(&connection_string, catalog.as_deref())
    })
    .await
    .map_err(|e| DataFetchError::Connection(e.to_string()))?
}

fn discover_tables_sync(
    connection_string: &str,
    catalog: Option<&str>,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let conn = Connection::open(connection_string)
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    // Build query with optional catalog filter
    let catalog_filter = match catalog {
        Some(cat) => format!(
            "AND t.table_catalog = '{}'",
            cat.replace('\'', "''") // escape single quotes
        ),
        None => String::new(),
    };

    let query = format!(
        r#"
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
        {}
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        "#,
        catalog_filter
    );

    let mut stmt = conn
        .prepare(&query)
        .map_err(|e| DataFetchError::Discovery(e.to_string()))?;

    let rows = stmt
        .query_map([], |row| {
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

/// Fetch table data and write to Parquet
pub async fn fetch_table(
    source: &Source,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let connection_string = source.connection_string();
    let schema = schema.to_string();
    let table = table.to_string();

    // DuckDB is sync, so spawn_blocking
    let (arrow_schema, batches) =
        tokio::task::spawn_blocking(move || fetch_table_sync(&connection_string, &schema, &table))
            .await
            .map_err(|e| DataFetchError::Query(e.to_string()))??;

    // Initialize writer with schema
    writer.init(&arrow_schema)?;

    // Write batches (or empty batch for empty tables)
    if batches.is_empty() {
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        writer.write_batch(&empty_batch)?;
    } else {
        for batch in batches {
            writer.write_batch(&batch)?;
        }
    }

    Ok(())
}

fn fetch_table_sync(
    connection_string: &str,
    schema: &str,
    table: &str,
) -> Result<
    (
        datafusion::arrow::datatypes::Schema,
        Vec<datafusion::arrow::record_batch::RecordBatch>,
    ),
    DataFetchError,
> {
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

    let arrow_schema = arrow_result.get_schema();
    let batches: Vec<_> = arrow_result.collect();

    Ok(((*arrow_schema).clone(), batches))
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
