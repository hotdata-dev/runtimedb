//! MySQL native driver implementation using sqlx

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection, MySqlRow, MySqlSslMode};
use sqlx::{ConnectOptions, Connection, Row};
use std::sync::Arc;
use tracing::warn;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

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

/// Check connectivity to a MySQL source
pub async fn check_health(
    source: &Source,
    secrets: &SecretManager,
) -> Result<(), DataFetchError> {
    let options = resolve_connect_options(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(options).await?;
    conn.ping()
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;
    Ok(())
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
    writer: &mut dyn BatchWriter,
) -> Result<(), DataFetchError> {
    let options = resolve_connect_options(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(options).await?;

    // Build query - use backticks for MySQL identifier escaping
    let query = format!(
        "SELECT * FROM `{}`.`{}`",
        schema.replace('`', "``"),
        table.replace('`', "``")
    );

    const BATCH_SIZE: usize = 10_000;

    // Query information_schema for accurate column metadata (especially nullable).
    // This ensures we get correct nullable flags regardless of table data.
    // We query on the same connection before starting the streaming data fetch.
    // Note: CAST is required because MySQL returns some columns as BLOB.
    let schema_rows = sqlx::query(
        r#"
        SELECT
            CAST(COLUMN_NAME AS CHAR(64)) AS COLUMN_NAME,
            CAST(COLUMN_TYPE AS CHAR(255)) AS COLUMN_TYPE,
            CAST(IS_NULLABLE AS CHAR(3)) AS IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_all(&mut conn)
    .await?;

    if schema_rows.is_empty() {
        return Err(DataFetchError::Query(format!(
            "Table {}.{} not found or has no columns",
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

    let arrow_schema = Schema::new(fields);

    // Stream rows instead of loading all into memory
    let mut stream = sqlx::query(&query).fetch(&mut conn);

    // Initialize writer with schema
    writer.init(&arrow_schema)?;

    // Check if table has any rows
    let first_row = match stream.next().await {
        Some(Ok(row)) => row,
        Some(Err(e)) => return Err(DataFetchError::Query(e.to_string())),
        None => {
            // Empty table
            let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
            writer.write_batch(&empty_batch)?;
            return Ok(());
        }
    };
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

/// Convert MySQL type name to Arrow DataType.
///
/// This is the authoritative type mapping implementation. Tests validate against this.
pub fn mysql_type_to_arrow(mysql_type: &str) -> DataType {
    use datafusion::arrow::datatypes::TimeUnit;

    // MySQL COLUMN_TYPE includes size info like "int(11)" or "varchar(255)"
    let type_lower = mysql_type.to_lowercase();
    let is_unsigned = type_lower.contains("unsigned");

    // Special case: TINYINT(1) is conventionally used as boolean in MySQL
    if type_lower.starts_with("tinyint(1)") {
        return DataType::Boolean;
    }
    // Other TINYINT sizes: use proper unsigned type
    if type_lower.starts_with("tinyint") {
        return if is_unsigned {
            DataType::UInt8
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
        // Use proper unsigned Arrow types for unsigned MySQL integers
        "smallint" => {
            if is_unsigned {
                DataType::UInt16
            } else {
                DataType::Int16
            }
        }
        "mediumint" => {
            // MEDIUMINT is 24-bit, unsigned max is 16777215
            // Use UInt32 for unsigned (fits), Int32 for signed
            if is_unsigned {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        }
        "int" | "integer" => {
            if is_unsigned {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        }
        // BIGINT UNSIGNED max (18446744073709551615) fits exactly in UInt64
        "bigint" => {
            if is_unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        // DECIMAL/NUMERIC: parse precision/scale if provided
        "decimal" | "numeric" | "dec" | "fixed" => parse_mysql_decimal_params(&type_lower),
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

/// Parse a decimal string to i128 with the given precision and scale.
/// For example, "123.45" with precision=10, scale=2 becomes 12345.
///
/// Returns an error if:
/// - The value has more fractional digits than the target scale (would truncate)
/// - The value exceeds the declared precision
fn parse_decimal_to_i128(s: &str, precision: u8, scale: i8) -> Result<i128, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty decimal string".to_string());
    }

    let is_negative = s.starts_with('-');
    let s = if is_negative { &s[1..] } else { s };

    // Split on decimal point
    let (integer_part, fractional_part) = if let Some(dot_pos) = s.find('.') {
        (&s[..dot_pos], &s[dot_pos + 1..])
    } else {
        (s, "")
    };

    // Parse integer part
    let integer_val: i128 = if integer_part.is_empty() {
        0
    } else {
        integer_part
            .parse()
            .map_err(|e| format!("Invalid integer part '{}': {}", integer_part, e))?
    };

    // Scale the integer part
    let scale_factor = 10i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("Scale {} too large", scale))?;
    let scaled_integer = integer_val
        .checked_mul(scale_factor)
        .ok_or_else(|| format!("Integer overflow scaling {}", integer_val))?;

    // Parse and scale fractional part
    let fractional_val: i128 = if fractional_part.is_empty() {
        0
    } else {
        let frac_len = fractional_part.len() as i8;
        let frac_val: i128 = fractional_part
            .parse()
            .map_err(|e| format!("Invalid fractional part '{}': {}", fractional_part, e))?;

        if frac_len < scale {
            // Need to pad with zeros
            let padding = 10i128
                .checked_pow((scale - frac_len) as u32)
                .ok_or_else(|| "Scale padding overflow".to_string())?;
            frac_val
                .checked_mul(padding)
                .ok_or_else(|| "Fractional overflow".to_string())?
        } else if frac_len > scale {
            // Reject truncation - don't silently lose precision
            let divisor = 10i128
                .checked_pow((frac_len - scale) as u32)
                .ok_or_else(|| "Scale divisor overflow".to_string())?;
            let remainder = frac_val % divisor;
            if remainder != 0 {
                return Err(format!(
                    "Value '{}' has {} fractional digits but target scale is {} (would truncate)",
                    s, frac_len, scale
                ));
            }
            frac_val / divisor
        } else {
            frac_val
        }
    };

    let result = scaled_integer
        .checked_add(fractional_val)
        .ok_or_else(|| "Overflow adding integer and fractional parts".to_string())?;
    let result = if is_negative { -result } else { result };

    // Validate precision (total number of digits)
    let max_value = 10_i128.checked_pow(precision as u32).unwrap_or(i128::MAX);
    if result.abs() >= max_value {
        return Err(format!(
            "Value {} exceeds precision {} for DECIMAL({}, {})",
            result, precision, precision, scale
        ));
    }

    Ok(result)
}

/// Parse MySQL DECIMAL(precision, scale) parameters.
/// MySQL DECIMAL supports precision 1-65 and scale 0-30.
/// For values within Arrow's Decimal128 limits (precision 1-38), uses Decimal128.
/// For larger precision or invalid params, falls back to Utf8 to preserve precision.
fn parse_mysql_decimal_params(type_str: &str) -> DataType {
    // Check if there are parentheses (parameters provided)
    if type_str.contains('(') {
        // Use shared parsing logic, fall back to Utf8 for out-of-range precision
        // (MySQL DECIMAL supports precision 1-65, but Arrow only supports 1-38)
        super::parse_decimal_params(type_str).unwrap_or(DataType::Utf8)
    } else {
        // MySQL DECIMAL without params defaults to DECIMAL(10,0)
        DataType::Decimal128(10, 0)
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
    use datafusion::arrow::array::{
        Decimal128Builder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    };

    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Decimal128(precision, scale) => Box::new(
            Decimal128Builder::with_capacity(capacity)
                .with_data_type(DataType::Decimal128(*precision, *scale)),
        ),
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
    use datafusion::arrow::array::{
        Decimal128Builder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    };

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
        DataType::UInt8 => {
            let b = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
            b.append_option(try_get_with_warning::<u8>(row, idx, "u8"));
        }
        DataType::UInt16 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap();
            b.append_option(try_get_with_warning::<u16>(row, idx, "u16"));
        }
        DataType::UInt32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap();
            b.append_option(try_get_with_warning::<u32>(row, idx, "u32"));
        }
        DataType::UInt64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            b.append_option(try_get_with_warning::<u64>(row, idx, "u64"));
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
        DataType::Decimal128(precision, scale) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap();
            // MySQL DECIMAL is retrieved as string, parse to i128 scaled value
            if let Some(decimal_str) = try_get_with_warning::<String>(row, idx, "Decimal") {
                match parse_decimal_to_i128(&decimal_str, *precision, *scale) {
                    Ok(val) => b.append_value(val),
                    Err(e) => {
                        return Err(DataFetchError::Query(format!(
                            "MySQL DECIMAL conversion failed for column {}: value '{}' - {}",
                            idx, decimal_str, e
                        )));
                    }
                }
            } else {
                b.append_null();
            }
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
    use datafusion::arrow::datatypes::TimeUnit;

    // =========================================================================
    // Boolean types
    // =========================================================================

    #[test]
    fn test_mysql_boolean_types() {
        assert!(matches!(mysql_type_to_arrow("bool"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("boolean"), DataType::Boolean));
    }

    #[test]
    fn test_mysql_tinyint1_as_boolean() {
        // TINYINT(1) is conventionally used as boolean in MySQL
        assert!(matches!(
            mysql_type_to_arrow("tinyint(1)"),
            DataType::Boolean
        ));
        assert!(matches!(
            mysql_type_to_arrow("tinyint(1) unsigned"),
            DataType::Boolean
        ));
    }

    // =========================================================================
    // Integer types - Signed
    // =========================================================================

    #[test]
    fn test_mysql_tinyint_signed() {
        // TINYINT (non-boolean) is Int8 when signed
        assert!(matches!(mysql_type_to_arrow("tinyint"), DataType::Int8));
        assert!(matches!(mysql_type_to_arrow("tinyint(4)"), DataType::Int8));
        assert!(matches!(mysql_type_to_arrow("tinyint(3)"), DataType::Int8));
    }

    #[test]
    fn test_mysql_smallint_signed() {
        assert!(matches!(mysql_type_to_arrow("smallint"), DataType::Int16));
        assert!(matches!(
            mysql_type_to_arrow("smallint(6)"),
            DataType::Int16
        ));
        assert!(matches!(
            mysql_type_to_arrow("smallint(5)"),
            DataType::Int16
        ));
    }

    #[test]
    fn test_mysql_mediumint_signed() {
        assert!(matches!(mysql_type_to_arrow("mediumint"), DataType::Int32));
        assert!(matches!(
            mysql_type_to_arrow("mediumint(9)"),
            DataType::Int32
        ));
    }

    #[test]
    fn test_mysql_int_signed() {
        assert!(matches!(mysql_type_to_arrow("int"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("int(11)"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(
            mysql_type_to_arrow("integer(11)"),
            DataType::Int32
        ));
    }

    #[test]
    fn test_mysql_bigint_signed() {
        assert!(matches!(mysql_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(mysql_type_to_arrow("bigint(20)"), DataType::Int64));
    }

    // =========================================================================
    // Integer types - Unsigned
    // =========================================================================

    #[test]
    fn test_mysql_tinyint_unsigned() {
        // Unsigned TINYINT (0-255) maps to UInt8
        assert!(matches!(
            mysql_type_to_arrow("tinyint unsigned"),
            DataType::UInt8
        ));
        assert!(matches!(
            mysql_type_to_arrow("tinyint(3) unsigned"),
            DataType::UInt8
        ));
    }

    #[test]
    fn test_mysql_smallint_unsigned() {
        // Unsigned SMALLINT (0-65535) maps to UInt16
        assert!(matches!(
            mysql_type_to_arrow("smallint unsigned"),
            DataType::UInt16
        ));
        assert!(matches!(
            mysql_type_to_arrow("smallint(5) unsigned"),
            DataType::UInt16
        ));
    }

    #[test]
    fn test_mysql_mediumint_unsigned() {
        // Unsigned MEDIUMINT (0-16777215) maps to UInt32
        assert!(matches!(
            mysql_type_to_arrow("mediumint unsigned"),
            DataType::UInt32
        ));
    }

    #[test]
    fn test_mysql_int_unsigned() {
        // Unsigned INT (0-4294967295) maps to UInt32
        assert!(matches!(
            mysql_type_to_arrow("int unsigned"),
            DataType::UInt32
        ));
        assert!(matches!(
            mysql_type_to_arrow("int(10) unsigned"),
            DataType::UInt32
        ));
        assert!(matches!(
            mysql_type_to_arrow("integer unsigned"),
            DataType::UInt32
        ));
    }

    #[test]
    fn test_mysql_bigint_unsigned() {
        // BIGINT UNSIGNED (0-18446744073709551615) fits exactly in UInt64
        assert!(matches!(
            mysql_type_to_arrow("bigint unsigned"),
            DataType::UInt64
        ));
        assert!(matches!(
            mysql_type_to_arrow("bigint(20) unsigned"),
            DataType::UInt64
        ));
    }

    // =========================================================================
    // Float types
    // =========================================================================

    #[test]
    fn test_mysql_float_type() {
        assert!(matches!(mysql_type_to_arrow("float"), DataType::Float32));
        assert!(matches!(
            mysql_type_to_arrow("float(10,2)"),
            DataType::Float32
        ));
    }

    #[test]
    fn test_mysql_double_type() {
        assert!(matches!(mysql_type_to_arrow("double"), DataType::Float64));
        assert!(matches!(
            mysql_type_to_arrow("double(15,5)"),
            DataType::Float64
        ));
        assert!(matches!(mysql_type_to_arrow("real"), DataType::Float64));
    }

    // =========================================================================
    // Decimal types
    // =========================================================================

    // NOTE: Core decimal parsing is tested in mod.rs::tests (parse_decimal_params).
    // These tests verify MySQL-specific fallback behavior and aliases only.

    #[test]
    fn test_mysql_decimal_unconstrained() {
        // Unconstrained DECIMAL defaults to DECIMAL(10,0) per MySQL spec
        match mysql_type_to_arrow("decimal") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 0);
            }
            other => panic!("Expected Decimal128(10,0), got {:?}", other),
        }

        match mysql_type_to_arrow("numeric") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 0);
            }
            other => panic!("Expected Decimal128(10,0), got {:?}", other),
        }
    }

    #[test]
    fn test_mysql_decimal_aliases() {
        // DEC and FIXED are aliases for DECIMAL (MySQL-specific)
        match mysql_type_to_arrow("dec(15,3)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 15);
                assert_eq!(scale, 3);
            }
            other => panic!("Expected Decimal128(15,3), got {:?}", other),
        }

        match mysql_type_to_arrow("fixed(20,5)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 20);
                assert_eq!(scale, 5);
            }
            other => panic!("Expected Decimal128(20,5), got {:?}", other),
        }
    }

    #[test]
    fn test_mysql_decimal_exceeds_arrow_precision() {
        // MySQL supports up to 65 digits, but Arrow only supports 38
        // Values > 38 fall back to Utf8 to preserve precision
        assert!(matches!(
            mysql_type_to_arrow("decimal(50,10)"),
            DataType::Utf8
        ));
        assert!(matches!(
            mysql_type_to_arrow("decimal(65,30)"),
            DataType::Utf8
        ));
    }

    // =========================================================================
    // String types
    // =========================================================================

    #[test]
    fn test_mysql_char_types() {
        assert!(matches!(mysql_type_to_arrow("char"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("char(10)"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("char(255)"), DataType::Utf8));
    }

    #[test]
    fn test_mysql_varchar_types() {
        assert!(matches!(mysql_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(
            mysql_type_to_arrow("varchar(255)"),
            DataType::Utf8
        ));
        assert!(matches!(
            mysql_type_to_arrow("varchar(65535)"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_mysql_text_types() {
        assert!(matches!(mysql_type_to_arrow("text"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("tinytext"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("mediumtext"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("longtext"), DataType::Utf8));
    }

    // =========================================================================
    // Binary types
    // =========================================================================

    #[test]
    fn test_mysql_binary_types() {
        assert!(matches!(mysql_type_to_arrow("binary"), DataType::Binary));
        assert!(matches!(
            mysql_type_to_arrow("binary(16)"),
            DataType::Binary
        ));
        assert!(matches!(mysql_type_to_arrow("varbinary"), DataType::Binary));
        assert!(matches!(
            mysql_type_to_arrow("varbinary(255)"),
            DataType::Binary
        ));
    }

    #[test]
    fn test_mysql_blob_types() {
        assert!(matches!(mysql_type_to_arrow("blob"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("tinyblob"), DataType::Binary));
        assert!(matches!(
            mysql_type_to_arrow("mediumblob"),
            DataType::Binary
        ));
        assert!(matches!(mysql_type_to_arrow("longblob"), DataType::Binary));
    }

    #[test]
    fn test_mysql_bit_type() {
        assert!(matches!(mysql_type_to_arrow("bit"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("bit(1)"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("bit(8)"), DataType::Binary));
        assert!(matches!(mysql_type_to_arrow("bit(64)"), DataType::Binary));
    }

    // =========================================================================
    // Date/Time types
    // =========================================================================

    #[test]
    fn test_mysql_date_type() {
        assert!(matches!(mysql_type_to_arrow("date"), DataType::Date32));
    }

    #[test]
    fn test_mysql_time_type() {
        // Time stored as string
        assert!(matches!(mysql_type_to_arrow("time"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("time(6)"), DataType::Utf8));
    }

    #[test]
    fn test_mysql_datetime_type() {
        // DATETIME has no timezone info
        match mysql_type_to_arrow("datetime") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }

        match mysql_type_to_arrow("datetime(6)") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_mysql_timestamp_type() {
        // TIMESTAMP stores in UTC
        match mysql_type_to_arrow("timestamp") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }

        match mysql_type_to_arrow("timestamp(6)") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }
    }

    #[test]
    fn test_mysql_year_type() {
        assert!(matches!(mysql_type_to_arrow("year"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("year(4)"), DataType::Int32));
    }

    // =========================================================================
    // JSON type
    // =========================================================================

    #[test]
    fn test_mysql_json_type() {
        assert!(matches!(mysql_type_to_arrow("json"), DataType::Utf8));
    }

    // =========================================================================
    // Enum/Set types
    // =========================================================================

    #[test]
    fn test_mysql_enum_type() {
        assert!(matches!(mysql_type_to_arrow("enum"), DataType::Utf8));
        assert!(matches!(
            mysql_type_to_arrow("enum('a','b','c')"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_mysql_set_type() {
        assert!(matches!(mysql_type_to_arrow("set"), DataType::Utf8));
        assert!(matches!(
            mysql_type_to_arrow("set('a','b','c')"),
            DataType::Utf8
        ));
    }

    // =========================================================================
    // Unknown types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_mysql_unknown_type_fallback() {
        assert!(matches!(
            mysql_type_to_arrow("unknown_type"),
            DataType::Utf8
        ));
        assert!(matches!(mysql_type_to_arrow("custom_type"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("geometry"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("point"), DataType::Utf8));
    }

    // =========================================================================
    // Case insensitivity
    // =========================================================================

    #[test]
    fn test_mysql_case_insensitivity() {
        assert!(matches!(mysql_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("Boolean"), DataType::Boolean));
        assert!(matches!(mysql_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("Integer"), DataType::Int32));
        assert!(matches!(mysql_type_to_arrow("VARCHAR"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("VarChar(50)"), DataType::Utf8));
        assert!(matches!(mysql_type_to_arrow("BIGINT"), DataType::Int64));
        assert!(matches!(mysql_type_to_arrow("BigInt"), DataType::Int64));
        assert!(matches!(
            mysql_type_to_arrow("BIGINT UNSIGNED"),
            DataType::UInt64
        ));
    }

    // =========================================================================
    // Parameterized types (with size info)
    // =========================================================================

    #[test]
    fn test_mysql_parameterized_types() {
        // Ensure types with size parameters are handled correctly
        assert!(matches!(mysql_type_to_arrow("int(11)"), DataType::Int32));
        assert!(matches!(
            mysql_type_to_arrow("varchar(255)"),
            DataType::Utf8
        ));
        // DECIMAL(10,2) now correctly parses to Decimal128
        match mysql_type_to_arrow("decimal(10,2)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            other => panic!("Expected Decimal128(10,2), got {:?}", other),
        }
        assert!(matches!(
            mysql_type_to_arrow("binary(16)"),
            DataType::Binary
        ));
        assert!(matches!(mysql_type_to_arrow("bit(8)"), DataType::Binary));
    }

    // =========================================================================
    // Decimal string to i128 conversion
    // =========================================================================

    #[test]
    fn test_parse_decimal_exact_scale() {
        // "123.45" with precision=10, scale=2 -> 12345
        let result = parse_decimal_to_i128("123.45", 10, 2);
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_parse_decimal_scale_up() {
        // "123.4" with precision=10, scale=3 -> 123400 (pad with zero)
        let result = parse_decimal_to_i128("123.4", 10, 3);
        assert_eq!(result.unwrap(), 123400);
    }

    #[test]
    fn test_parse_decimal_scale_down_exact() {
        // "123.4500" with precision=10, scale=2 -> 12345 (trailing zeros OK)
        let result = parse_decimal_to_i128("123.4500", 10, 2);
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_parse_decimal_rejects_truncation() {
        // "123.4567" with precision=10, scale=2 -> error, would truncate
        let result = parse_decimal_to_i128("123.4567", 10, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("would truncate"));
    }

    #[test]
    fn test_parse_decimal_rejects_precision_overflow() {
        // "12345678901" exceeds precision=10 for DECIMAL(10,0)
        let result = parse_decimal_to_i128("12345678901", 10, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds precision"));
    }

    #[test]
    fn test_parse_decimal_negative() {
        // "-99.99" with precision=10, scale=2 -> -9999
        let result = parse_decimal_to_i128("-99.99", 10, 2);
        assert_eq!(result.unwrap(), -9999);
    }

    #[test]
    fn test_parse_decimal_no_fractional() {
        // "12345" with precision=10, scale=2 -> 1234500
        let result = parse_decimal_to_i128("12345", 10, 2);
        assert_eq!(result.unwrap(), 1234500);
    }

    #[test]
    fn test_parse_decimal_leading_dot() {
        // ".45" with precision=10, scale=2 -> 45
        let result = parse_decimal_to_i128(".45", 10, 2);
        assert_eq!(result.unwrap(), 45);
    }
}
