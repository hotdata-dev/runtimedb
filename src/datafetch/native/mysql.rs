//! MySQL native driver implementation using sqlx

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sqlx::mysql::{MySqlColumn, MySqlConnection, MySqlRow};
use sqlx::{Column, Connection, Row, TypeInfo};
use std::sync::Arc;
use urlencoding::encode;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

use super::StreamingParquetWriter;

/// Build a MySQL connection string from source configuration and resolved password.
fn build_connection_string(
    host: &str,
    port: u16,
    user: &str,
    database: &str,
    password: &str,
) -> String {
    format!(
        "mysql://{}:{}@{}:{}/{}",
        encode(user),
        encode(password),
        encode(host),
        port,
        encode(database)
    )
}

/// Resolve credentials and build connection string for a MySQL source.
pub async fn resolve_connection_string(
    source: &Source,
    secrets: &SecretManager,
) -> Result<String, DataFetchError> {
    let (host, port, user, database, credential) = match source {
        Source::Mysql {
            host,
            port,
            user,
            database,
            credential,
        } => (host, *port, user, database, credential),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected MySQL source".to_string(),
            ))
        }
    };

    let password = credential
        .resolve(secrets)
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    Ok(build_connection_string(
        host, port, user, database, &password,
    ))
}

/// Connect to MySQL with automatic SSL retry.
/// If the initial connection fails with an SSL-related error,
/// automatically retries with `ssl-mode=required` appended to the connection string.
async fn connect_with_ssl_retry(connection_string: &str) -> Result<MySqlConnection, sqlx::Error> {
    match MySqlConnection::connect(connection_string).await {
        Ok(conn) => Ok(conn),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            // Check if the error indicates SSL is required
            if error_msg.contains("ssl") || error_msg.contains("tls") {
                // Append ssl-mode=required and retry
                let ssl_connection_string = if connection_string.contains('?') {
                    format!("{}&ssl-mode=required", connection_string)
                } else {
                    format!("{}?ssl-mode=required", connection_string)
                };
                MySqlConnection::connect(&ssl_connection_string).await
            } else {
                Err(e)
            }
        }
    }
}

/// Discover tables and columns from MySQL
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;

    // Get the database name for filtering
    let database = match source {
        Source::Mysql { database, .. } => database.as_str(),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected MySQL source".to_string(),
            ))
        }
    };

    let rows = sqlx::query(
        r#"
        SELECT
            t.TABLE_CATALOG,
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_TYPE,
            c.COLUMN_NAME,
            c.COLUMN_TYPE,
            c.IS_NULLABLE,
            c.ORDINAL_POSITION
        FROM information_schema.TABLES t
        JOIN information_schema.COLUMNS c
            ON t.TABLE_CATALOG = c.TABLE_CATALOG
            AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
        WHERE t.TABLE_SCHEMA = ?
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        "#,
    )
    .bind(database)
    .fetch_all(&mut conn)
    .await?;

    let mut tables: Vec<TableMetadata> = Vec::new();

    for row in rows {
        let catalog: Option<String> = row.get(0);
        let schema: String = row.get(1);
        let table: String = row.get(2);
        let table_type: String = row.get(3);
        let col_name: String = row.get(4);
        let data_type: String = row.get(5);
        let is_nullable: String = row.get(6);
        let ordinal: u32 = row.get(7);

        let column = ColumnMetadata {
            name: col_name,
            data_type: mysql_type_to_arrow(&data_type),
            nullable: is_nullable.to_uppercase() == "YES",
            ordinal_position: ordinal as i32,
        };

        // Find or create table entry
        if let Some(existing) = tables
            .iter_mut()
            .find(|t| t.catalog_name == catalog && t.schema_name == schema && t.table_name == table)
        {
            existing.columns.push(column);
        } else {
            tables.push(TableMetadata {
                catalog_name: catalog,
                schema_name: schema,
                table_name: table,
                table_type,
                columns: vec![column],
            });
        }
    }

    Ok(tables)
}

/// Fetch table data and write to Parquet using streaming to avoid OOM on large tables
pub async fn fetch_table(
    source: &Source,
    secrets: &SecretManager,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;

    // Build query - use backticks for MySQL identifier escaping
    let query = format!(
        "SELECT * FROM `{}`.`{}`",
        schema.replace('`', "``"),
        table.replace('`', "``")
    );

    const BATCH_SIZE: usize = 10_000;

    // Stream rows instead of loading all into memory
    let mut stream = sqlx::query(&query).fetch(&mut conn);

    // Get first row to extract schema
    let first_row = stream.next().await;

    let arrow_schema = match &first_row {
        Some(Ok(row)) => schema_from_columns(row.columns()),
        Some(Err(e)) => return Err(DataFetchError::Query(e.to_string())),
        None => {
            // Empty table: query information_schema for schema
            // Need a new connection since stream borrows conn
            let mut schema_conn = connect_with_ssl_retry(&connection_string).await?;
            let schema_rows = sqlx::query(
                r#"
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                "#,
            )
            .bind(schema)
            .bind(table)
            .fetch_all(&mut schema_conn)
            .await?;

            if schema_rows.is_empty() {
                return Err(DataFetchError::Query(format!(
                    "Table {}.{} has no columns",
                    schema, table
                )));
            }

            let fields: Vec<Field> = schema_rows
                .iter()
                .map(|row| {
                    let col_name: String = row.get(0);
                    let data_type: String = row.get(1);
                    let is_nullable: String = row.get(2);
                    Field::new(
                        col_name,
                        mysql_type_to_arrow(&data_type),
                        is_nullable.to_uppercase() == "YES",
                    )
                })
                .collect();

            Schema::new(fields)
        }
    };

    // Initialize writer with schema
    writer.init(&arrow_schema)?;

    // Handle empty table case
    if first_row.is_none() {
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        writer.write_batch(&empty_batch)?;
        return Ok(());
    }

    // Process first row and continue streaming
    let first_row = first_row.unwrap()?;
    let mut batch_rows: Vec<MySqlRow> = Vec::with_capacity(BATCH_SIZE);
    batch_rows.push(first_row);

    // Stream remaining rows
    while let Some(row_result) = stream.next().await {
        let row = row_result?;
        batch_rows.push(row);

        // Write batch when full
        if batch_rows.len() >= BATCH_SIZE {
            let batch = rows_to_batch(&batch_rows, &arrow_schema)?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write any remaining rows
    if !batch_rows.is_empty() {
        let batch = rows_to_batch(&batch_rows, &arrow_schema)?;
        writer.write_batch(&batch)?;
    }

    Ok(())
}

// ============================================================================
// Arrow conversion utilities
// ============================================================================

/// Build Arrow Schema from sqlx column metadata
fn schema_from_columns(columns: &[MySqlColumn]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let name = col.name();
            let data_type = mysql_type_to_arrow(col.type_info().name());
            Field::new(name, data_type, true) // Assume nullable
        })
        .collect();

    Schema::new(fields)
}

/// Convert MySQL type name to Arrow DataType
fn mysql_type_to_arrow(mysql_type: &str) -> DataType {
    use datafusion::arrow::datatypes::TimeUnit;

    // MySQL COLUMN_TYPE includes size info like "int(11)" or "varchar(255)"
    let type_lower = mysql_type.to_lowercase();

    // Special case: TINYINT(1) is conventionally used as boolean in MySQL
    // Other TINYINT sizes (or bare TINYINT) should be treated as Int8
    if type_lower.starts_with("tinyint(1)") || type_lower == "tinyint(1) unsigned" {
        return DataType::Boolean;
    }
    if type_lower.starts_with("tinyint") {
        return DataType::Int8;
    }

    // Extract the base type by taking everything before the first parenthesis
    let base_type = type_lower.split('(').next().unwrap_or(&type_lower).trim();

    // Also handle "unsigned" suffix
    let base_type = base_type.split_whitespace().next().unwrap_or(base_type);

    match base_type {
        "bool" | "boolean" => DataType::Boolean,
        "smallint" => DataType::Int16,
        "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        // Complex numeric types: fallback to Utf8 to preserve precision
        "decimal" | "numeric" | "dec" | "fixed" => DataType::Utf8,
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set" => {
            DataType::Utf8
        }
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            DataType::Binary
        }
        "date" => DataType::Date32,
        "time" => DataType::Utf8, // Time without date stored as string
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "year" => DataType::Int32,
        "json" => DataType::Utf8,
        "bit" => DataType::Binary,
        _ => DataType::Utf8, // Default fallback
    }
}

/// Build a RecordBatch from MySQL rows
fn rows_to_batch(
    rows: &[sqlx::mysql::MySqlRow],
    schema: &Schema,
) -> Result<RecordBatch, DataFetchError> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), rows.len()))
        .collect();

    for row in rows {
        for (i, field) in schema.fields().iter().enumerate() {
            append_value(&mut builders[i], row, i, field.data_type())?;
        }
    }

    let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        builders.iter_mut().map(|b| b.finish()).collect();

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| DataFetchError::Query(e.to_string()))
}

fn make_builder(data_type: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(_, _) => Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)), // Fallback to string
    }
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    row: &sqlx::mysql::MySqlRow,
    idx: usize,
    data_type: &DataType,
) -> Result<(), DataFetchError> {
    match data_type {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            // MySQL TINYINT(1) is used as boolean
            b.append_option(row.try_get::<bool, _>(idx).ok());
        }
        DataType::Int8 => {
            let b = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            b.append_option(row.try_get::<i8, _>(idx).ok());
        }
        DataType::Int16 => {
            let b = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            b.append_option(row.try_get::<i16, _>(idx).ok());
        }
        DataType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            b.append_option(row.try_get::<i32, _>(idx).ok());
        }
        DataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            b.append_option(row.try_get::<i64, _>(idx).ok());
        }
        DataType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            b.append_option(row.try_get::<f32, _>(idx).ok());
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            b.append_option(row.try_get::<f64, _>(idx).ok());
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            b.append_option(row.try_get::<String, _>(idx).ok());
        }
        DataType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            b.append_option(row.try_get::<Vec<u8>, _>(idx).ok());
        }
        DataType::Date32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            // chrono::NaiveDate -> days since epoch
            if let Ok(date) = row.try_get::<chrono::NaiveDate, _>(idx) {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (date - epoch).num_days() as i32;
                b.append_value(days);
            } else {
                b.append_null();
            }
        }
        DataType::Timestamp(_, _) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            // chrono::NaiveDateTime -> microseconds since epoch
            if let Ok(ts) = row.try_get::<chrono::NaiveDateTime, _>(idx) {
                let micros = ts.and_utc().timestamp_micros();
                b.append_value(micros);
            } else {
                b.append_null();
            }
        }
        _ => {
            // Fallback: try to get as string
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            b.append_option(row.try_get::<String, _>(idx).ok());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_type_to_arrow_basic_types() {
        assert!(matches!(mysql_type_to_arrow("bool"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("boolean"), DataType::Boolean));
        // TINYINT(1) is conventionally boolean
        assert!(matches!(
            mysql_type_to_arrow("tinyint(1)"),
            DataType::Boolean
        ));
        assert!(matches!(
            mysql_type_to_arrow("tinyint(1) unsigned"),
            DataType::Boolean
        ));
        // Other TINYINT sizes are Int8
        assert!(matches!(mysql_type_to_arrow("tinyint"), DataType::Int8));
        assert!(matches!(mysql_type_to_arrow("tinyint(4)"), DataType::Int8));
        assert!(matches!(
            mysql_type_to_arrow("tinyint unsigned"),
            DataType::Int8
        ));
        assert!(matches!(mysql_type_to_arrow("smallint"), DataType::Int16));
        assert!(matches!(
            mysql_type_to_arrow("smallint(6)"),
            DataType::Int16
        ));
        assert!(matches!(mysql_type_to_arrow("int"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("int(11)"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("mediumint"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(
            mysql_type_to_arrow("bigint(20)"),
            DataType::Int64
        ));
        assert!(matches!(mysql_type_to_arrow("float"), DataType::Float32));
        assert!(matches!(mysql_type_to_arrow("double"), DataType::Float64));
        assert!(matches!(mysql_type_to_arrow("real"), DataType::Float64));
    }

    #[test]
    fn test_mysql_type_to_arrow_string_types() {
        assert!(matches!(
            mysql_type_to_arrow("varchar(255)"),
            DataType::Utf8
        ));
        assert!(matches!(mysql_type_to_arrow("char(10)"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("text"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("tinytext"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("mediumtext"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("longtext"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("enum"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("set"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("json"), DataType::Utf8));
    }

    #[test]
    fn test_mysql_type_to_arrow_date_time_types() {
        assert!(matches!(mysql_type_to_arrow("date"), DataType::Date32));
        assert!(matches!(mysql_type_to_arrow("time"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("year"), DataType::Int32));

        match mysql_type_to_arrow("datetime") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(
                    unit,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond
                ));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }

        match mysql_type_to_arrow("timestamp") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(
                    unit,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond
                ));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }
    }

    #[test]
    fn test_mysql_type_to_arrow_binary_types() {
        assert!(matches!(
            mysql_type_to_arrow("binary(16)"),
            DataType::Binary
        ));
        assert!(matches!(
            mysql_type_to_arrow("varbinary(255)"),
            DataType::Binary
        ));
        assert!(matches!(mysql_type_to_arrow("blob"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("tinyblob"), DataType::Binary));
        assert!(matches!(
            mysql_type_to_arrow("mediumblob"),
            DataType::Binary
        ));
        assert!(matches!(mysql_type_to_arrow("longblob"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("bit(8)"), DataType::Binary));
    }

    #[test]
    fn test_mysql_type_to_arrow_complex_types_fallback() {
        // Complex numeric types should fall back to Utf8 to preserve precision
        assert!(matches!(
            mysql_type_to_arrow("decimal(10,2)"),
            DataType::Utf8
        ));
        assert!(matches!(
            mysql_type_to_arrow("numeric(10,2)"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_mysql_type_to_arrow_unknown_type_fallback() {
        // Unknown types should default to Utf8
        assert!(matches!(
            mysql_type_to_arrow("unknown_type"),
            DataType::Utf8
        ));
        assert!(matches!(
            mysql_type_to_arrow("custom_type"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_mysql_type_to_arrow_case_insensitive() {
        assert!(matches!(mysql_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("Boolean"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("VarChar(50)"), DataType::Utf8));
    }

    #[test]
    fn test_mysql_type_to_arrow_unsigned() {
        // Unsigned types should still map to their base type
        assert!(matches!(
            mysql_type_to_arrow("int unsigned"),
            DataType::Int32
        ));
        assert!(matches!(
            mysql_type_to_arrow("bigint unsigned"),
            DataType::Int64
        ));
    }
}
