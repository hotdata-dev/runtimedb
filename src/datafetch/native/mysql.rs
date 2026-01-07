//! MySQL native driver implementation using sqlx

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sqlx::mysql::{MySqlColumn, MySqlConnectOptions, MySqlConnection, MySqlRow, MySqlSslMode};
use sqlx::{Column, ConnectOptions, Row, TypeInfo};
use std::sync::Arc;
use tracing::warn;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

use super::StreamingParquetWriter;

/// Build MySQL connection options from source configuration and resolved password.
/// Uses MySqlConnectOptions to avoid embedding credentials in a URL string.
fn build_connect_options(
    host: &str,
    port: u16,
    user: &str,
    database: &str,
    password: &str,
) -> MySqlConnectOptions {
    MySqlConnectOptions::new()
        .host(host)
        .port(port)
        .username(user)
        .password(password)
        .database(database)
}

/// Resolve credentials and build connection options for a MySQL source.
pub async fn resolve_connect_options(
    source: &Source,
    secrets: &SecretManager,
) -> Result<MySqlConnectOptions, DataFetchError> {
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

    Ok(build_connect_options(host, port, user, database, &password))
}

/// Connect to MySQL with automatic SSL retry.
/// If the initial connection fails with an SSL-related error,
/// automatically retries with SSL mode set to Required.
async fn connect_with_ssl_retry(
    options: MySqlConnectOptions,
) -> Result<MySqlConnection, sqlx::Error> {
    match options.clone().connect().await {
        Ok(conn) => Ok(conn),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            // Check if the error indicates SSL is required
            if error_msg.contains("ssl") || error_msg.contains("tls") {
                // Retry with SSL mode required
                options.ssl_mode(MySqlSslMode::Required).connect().await
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
    let options = resolve_connect_options(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(options).await?;

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
            CAST(t.TABLE_CATALOG AS CHAR(64)) AS TABLE_CATALOG,
            CAST(t.TABLE_SCHEMA AS CHAR(64)) AS TABLE_SCHEMA,
            CAST(t.TABLE_NAME AS CHAR(64)) AS TABLE_NAME,
            CAST(t.TABLE_TYPE AS CHAR(64)) AS TABLE_TYPE,
            CAST(c.COLUMN_NAME AS CHAR(64)) AS COLUMN_NAME,
            CAST(c.COLUMN_TYPE AS CHAR(255)) AS COLUMN_TYPE,
            CAST(c.IS_NULLABLE AS CHAR(3)) AS IS_NULLABLE,
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
    let options = resolve_connect_options(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(options.clone()).await?;

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
            let mut schema_conn = connect_with_ssl_retry(options).await?;
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
    let is_unsigned = type_lower.contains("unsigned");

    // Special case: TINYINT(1) is conventionally used as boolean in MySQL
    if type_lower.starts_with("tinyint(1)") {
        return DataType::Boolean;
    }
    // Other TINYINT sizes: unsigned maps to Int16 to avoid overflow
    if type_lower.starts_with("tinyint") {
        return if is_unsigned {
            DataType::Int16
        } else {
            DataType::Int8
        };
    }

    // Extract the base type by taking everything before the first parenthesis
    let base_type = type_lower.split('(').next().unwrap_or(&type_lower).trim();

    // Remove "unsigned" suffix to get clean base type
    let base_type = base_type.split_whitespace().next().unwrap_or(base_type);

    match base_type {
        "bool" | "boolean" => DataType::Boolean,
        // Unsigned integers map to next larger signed type to avoid overflow
        "smallint" => {
            if is_unsigned {
                DataType::Int32
            } else {
                DataType::Int16
            }
        }
        "mediumint" | "int" | "integer" => {
            if is_unsigned {
                DataType::Int64
            } else {
                DataType::Int32
            }
        }
        // BIGINT UNSIGNED maps to Utf8 to preserve full precision (max 18446744073709551615)
        "bigint" => {
            if is_unsigned {
                DataType::Utf8
            } else {
                DataType::Int64
            }
        }
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
        "datetime" => DataType::Timestamp(TimeUnit::Microsecond, None),
        // MySQL TIMESTAMP is stored in UTC, so we annotate with timezone
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
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

/// Helper to try getting a value and log a warning if conversion fails (but value exists)
fn try_get_with_warning<'r, T>(row: &'r MySqlRow, idx: usize, type_name: &str) -> Option<T>
where
    T: sqlx::Decode<'r, sqlx::MySql> + sqlx::Type<sqlx::MySql>,
{
    match row.try_get::<T, _>(idx) {
        Ok(val) => Some(val),
        Err(sqlx::Error::ColumnDecode { source, .. }) => {
            // Only warn if it's a decode error (type mismatch), not a null value
            warn!(
                column_index = idx,
                target_type = type_name,
                error = %source,
                "MySQL column value could not be converted to target type, storing as NULL"
            );
            None
        }
        Err(_) => None, // Null or other error, silently return None
    }
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    row: &MySqlRow,
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
            b.append_option(try_get_with_warning::<bool>(row, idx, "bool"));
        }
        DataType::Int8 => {
            let b = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            b.append_option(try_get_with_warning::<i8>(row, idx, "i8"));
        }
        DataType::Int16 => {
            let b = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            b.append_option(try_get_with_warning::<i16>(row, idx, "i16"));
        }
        DataType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            b.append_option(try_get_with_warning::<i32>(row, idx, "i32"));
        }
        DataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            b.append_option(try_get_with_warning::<i64>(row, idx, "i64"));
        }
        DataType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            b.append_option(try_get_with_warning::<f32>(row, idx, "f32"));
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            b.append_option(try_get_with_warning::<f64>(row, idx, "f64"));
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            b.append_option(try_get_with_warning::<String>(row, idx, "String"));
        }
        DataType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            b.append_option(try_get_with_warning::<Vec<u8>>(row, idx, "Vec<u8>"));
        }
        DataType::Date32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            // chrono::NaiveDate -> days since epoch
            if let Some(date) = try_get_with_warning::<chrono::NaiveDate>(row, idx, "NaiveDate") {
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
            if let Some(ts) =
                try_get_with_warning::<chrono::NaiveDateTime>(row, idx, "NaiveDateTime")
            {
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
            b.append_option(try_get_with_warning::<String>(row, idx, "String"));
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
        // Other TINYINT sizes are Int8 (signed) or Int16 (unsigned)
        assert!(matches!(mysql_type_to_arrow("tinyint"), DataType::Int8));
        assert!(matches!(mysql_type_to_arrow("tinyint(4)"), DataType::Int8));
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
        assert!(matches!(mysql_type_to_arrow("bigint(20)"), DataType::Int64));
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

        // DATETIME has no timezone info
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

        // TIMESTAMP stores in UTC, so we annotate with timezone
        match mysql_type_to_arrow("timestamp") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(
                    unit,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond
                ));
                assert!(tz.is_some());
                assert_eq!(tz.as_ref().unwrap().as_ref(), "UTC");
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
        assert!(matches!(mysql_type_to_arrow("custom_type"), DataType::Utf8));
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
        // Unsigned types map to next larger signed type to avoid overflow
        assert!(matches!(
            mysql_type_to_arrow("tinyint unsigned"),
            DataType::Int16
        ));
        assert!(matches!(
            mysql_type_to_arrow("tinyint(3) unsigned"),
            DataType::Int16
        ));
        assert!(matches!(
            mysql_type_to_arrow("smallint unsigned"),
            DataType::Int32
        ));
        assert!(matches!(
            mysql_type_to_arrow("int unsigned"),
            DataType::Int64
        ));
        assert!(matches!(
            mysql_type_to_arrow("int(10) unsigned"),
            DataType::Int64
        ));
        // BIGINT UNSIGNED maps to Utf8 to preserve full precision
        assert!(matches!(
            mysql_type_to_arrow("bigint unsigned"),
            DataType::Utf8
        ));
        assert!(matches!(
            mysql_type_to_arrow("bigint(20) unsigned"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_mysql_type_to_arrow_timestamp_timezone() {
        use datafusion::arrow::datatypes::TimeUnit;
        // DATETIME has no timezone info
        assert!(matches!(
            mysql_type_to_arrow("datetime"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        // TIMESTAMP stores in UTC
        match mysql_type_to_arrow("timestamp") {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC");
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }
    }
}
