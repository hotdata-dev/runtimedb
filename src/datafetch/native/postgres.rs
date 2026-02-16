//! PostgreSQL native driver implementation using sqlx

use bigdecimal::BigDecimal;
use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{Connection, Row};
use std::sync::Arc;
use tracing::warn;
use urlencoding::encode;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Build a PostgreSQL connection string from source configuration and resolved password.
fn build_connection_string(
    host: &str,
    port: u16,
    user: &str,
    database: &str,
    password: &str,
) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}",
        encode(user),
        encode(password),
        encode(host),
        port,
        encode(database)
    )
}

/// Resolve credentials and build connection string for a Postgres source.
pub async fn resolve_connection_string(
    source: &Source,
    secrets: &SecretManager,
) -> Result<String, DataFetchError> {
    let (host, port, user, database, credential) = match source {
        Source::Postgres {
            host,
            port,
            user,
            database,
            credential,
        } => (host, *port, user, database, credential),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected Postgres source".to_string(),
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

/// Connect to PostgreSQL with automatic SSL retry.
/// If the initial connection fails with an "insecure connection" error,
/// automatically retries with `sslmode=require` appended to the connection string.
async fn connect_with_ssl_retry(connection_string: &str) -> Result<PgConnection, sqlx::Error> {
    match PgConnection::connect(connection_string).await {
        Ok(conn) => Ok(conn),
        Err(e) => {
            let error_msg = e.to_string();
            // Check if the error indicates SSL is required
            if error_msg.contains("connection is insecure") || error_msg.contains("sslmode=require")
            {
                // Append sslmode=require and retry
                let ssl_connection_string = if connection_string.contains('?') {
                    format!("{}&sslmode=require", connection_string)
                } else {
                    format!("{}?sslmode=require", connection_string)
                };
                PgConnection::connect(&ssl_connection_string).await
            } else {
                Err(e)
            }
        }
    }
}

/// Check connectivity to a PostgreSQL source
pub async fn check_health(
    source: &Source,
    secrets: &SecretManager,
) -> Result<(), DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;
    conn.ping()
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;
    Ok(())
}

/// Discover tables and columns from PostgreSQL
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.table_type,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.ordinal_position::int
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        "#,
    )
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
        let ordinal: i32 = row.get(7);

        let column = ColumnMetadata {
            name: col_name,
            data_type: pg_type_to_arrow(&data_type),
            nullable: is_nullable.to_uppercase() == "YES",
            ordinal_position: ordinal,
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
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;

    const BATCH_SIZE: usize = 10_000;

    // Query information_schema for accurate column metadata (especially nullable).
    // This ensures we get correct nullable flags regardless of table data.
    // We query on the same connection before starting the streaming data fetch.
    //
    // For NUMERIC/DECIMAL, we need to construct the full type spec with precision/scale
    // because information_schema.data_type just returns "numeric" without parameters.
    let schema_rows = sqlx::query(
        r#"
        SELECT
            column_name,
            CASE
                WHEN data_type IN ('numeric', 'decimal') AND numeric_precision IS NOT NULL
                THEN data_type || '(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
                ELSE data_type
            END as data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
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

    // Build column list and Arrow schema from metadata.
    // Unconstrained NUMERIC/DECIMAL (without precision/scale) must be cast to TEXT
    // because they can have arbitrary precision that exceeds i128/Decimal128.
    // Constrained NUMERIC(p,s) is decoded via BigDecimal and stored as Decimal128.
    let mut fields: Vec<Field> = Vec::with_capacity(schema_rows.len());
    let mut column_exprs: Vec<String> = Vec::with_capacity(schema_rows.len());

    for row in &schema_rows {
        let col_name: String = row.get(0);
        let data_type: String = row.get(1);
        let is_nullable: String = row.get(2);

        let arrow_type = pg_type_to_arrow(&data_type);
        fields.push(Field::new(
            &col_name,
            arrow_type.clone(),
            is_nullable.to_uppercase() == "YES",
        ));

        // Escape column name for SQL
        let escaped_col = format!("\"{}\"", col_name.replace('"', "\"\""));

        // Only cast unconstrained NUMERIC to TEXT (those mapped to Utf8)
        // Constrained NUMERIC(p,s) is mapped to Decimal128 and decoded via BigDecimal
        if matches!(arrow_type, DataType::Utf8) {
            let type_lower = data_type.to_lowercase();
            let base_type = type_lower.split('(').next().unwrap_or(&type_lower);
            if base_type == "numeric" || base_type == "decimal" {
                column_exprs.push(format!("{}::text AS {}", escaped_col, escaped_col));
            } else {
                column_exprs.push(escaped_col);
            }
        } else {
            column_exprs.push(escaped_col);
        }
    }

    let arrow_schema = Schema::new(fields);

    // Build query with explicit column list and casts
    let query = format!(
        "SELECT {} FROM \"{}\".\"{}\"",
        column_exprs.join(", "),
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );

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
    let mut batch_rows: Vec<PgRow> = Vec::with_capacity(BATCH_SIZE);
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

/// Convert PostgreSQL type name to Arrow DataType.
///
/// This is the authoritative type mapping implementation. Tests validate against this.
pub fn pg_type_to_arrow(pg_type: &str) -> DataType {
    use datafusion::arrow::datatypes::TimeUnit;

    let type_lower = pg_type.to_lowercase();

    // Extract base type (before any parentheses for parameterized types)
    let base_type = type_lower.split('(').next().unwrap_or(&type_lower).trim();

    match base_type {
        "bool" | "boolean" => DataType::Boolean,
        "int2" | "smallint" => DataType::Int16,
        "int4" | "int" | "integer" => DataType::Int32,
        "int8" | "bigint" => DataType::Int64,
        "float4" | "real" => DataType::Float32,
        "float8" | "double precision" => DataType::Float64,
        // NUMERIC/DECIMAL: parse precision/scale if provided, otherwise use Utf8
        // to preserve arbitrary precision for unconstrained NUMERIC
        "numeric" | "decimal" => parse_pg_numeric_params(&type_lower),
        "varchar" | "text" | "char" | "bpchar" | "name" | "character varying" | "character" => {
            DataType::Utf8
        }
        "bytea" => DataType::Binary,
        "date" => DataType::Date32,
        "time" | "time without time zone" => DataType::Utf8,
        "timestamp" | "timestamp without time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamptz" | "timestamp with time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "interval" => DataType::Utf8,
        _ => DataType::Utf8, // Default fallback
    }
}

/// Parse PostgreSQL NUMERIC(precision, scale) parameters.
/// For constrained NUMERIC(p,s), returns Decimal128 with those params.
/// For unconstrained NUMERIC, returns Utf8 to preserve arbitrary precision.
fn parse_pg_numeric_params(type_str: &str) -> DataType {
    // Use shared parsing logic, fall back to Utf8 for unconstrained NUMERIC
    // (PostgreSQL NUMERIC can have arbitrary precision up to 131072 digits)
    super::parse_decimal_params(type_str).unwrap_or(DataType::Utf8)
}

/// Build a RecordBatch from PostgreSQL rows
fn rows_to_batch(
    rows: &[sqlx::postgres::PgRow],
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
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
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

/// Helper to try getting a value and log a warning if conversion fails (but value exists).
/// This helps with debugging type mismatches without silently dropping data.
fn try_get_with_warning<'r, T>(row: &'r PgRow, idx: usize, type_name: &str) -> Option<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    match row.try_get::<T, _>(idx) {
        Ok(val) => Some(val),
        Err(sqlx::Error::ColumnDecode { source, .. }) => {
            // Only warn if it's a decode error (type mismatch), not a null value
            warn!(
                column_index = idx,
                target_type = type_name,
                error = %source,
                "PostgreSQL column value could not be converted to target type, storing as NULL"
            );
            None
        }
        Err(_) => None, // Null or other error, silently return None
    }
}

/// Convert a BigDecimal to i128 for Arrow Decimal128, scaling to the target precision/scale.
///
/// Arrow Decimal128 stores values as i128 with an implicit scale. For example,
/// the value 123.45 with scale=2 is stored as 12345.
///
/// This function:
/// 1. Extracts the unscaled value and exponent from BigDecimal
/// 2. Adjusts the value to match the target Arrow scale
/// 3. Validates the result fits within the target precision
fn bigdecimal_to_i128(bd: &BigDecimal, precision: u8, target_scale: i8) -> Result<i128, String> {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::Zero;
    use bigdecimal::ToPrimitive;

    // BigDecimal stores as (BigInt, scale) where value = BigInt * 10^(-scale)
    // Note: BigDecimal's scale is i64, Arrow's scale is i8
    let (bigint_cow, bd_scale) = bd.as_bigint_and_scale();
    let bigint = bigint_cow.into_owned();

    // Calculate scale adjustment needed
    // If bd_scale=2 and target_scale=4, we need to multiply by 10^2
    // If bd_scale=4 and target_scale=2, we need to divide by 10^2
    let scale_diff = (target_scale as i64) - bd_scale;

    let adjusted = if scale_diff > 0 {
        // Need more decimal places - multiply
        let factor = BigInt::from(10_i64).pow(scale_diff as u32);
        bigint * factor
    } else if scale_diff < 0 {
        // Need fewer decimal places - must divide exactly (no truncation allowed)
        let factor = BigInt::from(10_i64).pow((-scale_diff) as u32);
        let remainder = &bigint % &factor;
        if !remainder.is_zero() {
            return Err(format!(
                "Value {} has more fractional digits than target scale {} allows (would truncate)",
                bd, target_scale
            ));
        }
        bigint / factor
    } else {
        bigint
    };

    // Convert to i128
    let value = adjusted.to_i128().ok_or_else(|| {
        format!(
            "BigDecimal value exceeds i128 range for Decimal128({}, {})",
            precision, target_scale
        )
    })?;

    // Validate precision (the total number of digits)
    // For a value stored as i128 with scale s, precision is ceil(log10(|value|)) + s for the integer part
    // But practically, we check if |value| < 10^precision
    let max_value = 10_i128.checked_pow(precision as u32).unwrap_or(i128::MAX);
    if value.abs() >= max_value {
        return Err(format!(
            "Value {} exceeds precision {} for Decimal128({}, {})",
            value, precision, precision, target_scale
        ));
    }

    Ok(value)
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    row: &sqlx::postgres::PgRow,
    idx: usize,
    data_type: &DataType,
) -> Result<(), DataFetchError> {
    match data_type {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            b.append_option(try_get_with_warning::<bool>(row, idx, "bool"));
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
        DataType::Decimal128(precision, scale) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap();
            if let Some(bd) = try_get_with_warning::<BigDecimal>(row, idx, "BigDecimal") {
                match bigdecimal_to_i128(&bd, *precision, *scale) {
                    Ok(val) => b.append_value(val),
                    Err(e) => {
                        return Err(DataFetchError::Query(format!(
                            "PostgreSQL DECIMAL conversion failed for column {}: {}",
                            idx, e
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
    fn test_pg_boolean_types() {
        assert!(matches!(pg_type_to_arrow("boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("bool"), DataType::Boolean));
    }

    // =========================================================================
    // Integer types
    // =========================================================================

    #[test]
    fn test_pg_smallint_types() {
        assert!(matches!(pg_type_to_arrow("smallint"), DataType::Int16));
        assert!(matches!(pg_type_to_arrow("int2"), DataType::Int16));
    }

    #[test]
    fn test_pg_integer_types() {
        assert!(matches!(pg_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("int"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("int4"), DataType::Int32));
    }

    #[test]
    fn test_pg_bigint_types() {
        assert!(matches!(pg_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow("int8"), DataType::Int64));
    }

    // =========================================================================
    // Float types
    // =========================================================================

    #[test]
    fn test_pg_float32_types() {
        assert!(matches!(pg_type_to_arrow("real"), DataType::Float32));
        assert!(matches!(pg_type_to_arrow("float4"), DataType::Float32));
    }

    #[test]
    fn test_pg_float64_types() {
        assert!(matches!(
            pg_type_to_arrow("double precision"),
            DataType::Float64
        ));
        assert!(matches!(pg_type_to_arrow("float8"), DataType::Float64));
    }

    // =========================================================================
    // Decimal/Numeric types
    // =========================================================================

    // NOTE: Core decimal parsing is tested in mod.rs::tests (parse_decimal_params).
    // These tests verify PostgreSQL-specific fallback behavior only.

    #[test]
    fn test_pg_numeric_unconstrained() {
        // Unconstrained NUMERIC falls back to Utf8 to preserve arbitrary precision
        assert!(matches!(pg_type_to_arrow("numeric"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("decimal"), DataType::Utf8));
    }

    #[test]
    fn test_pg_numeric_exceeds_arrow_precision() {
        // PostgreSQL NUMERIC can exceed Arrow's 38 digit limit - falls back to Utf8
        assert!(matches!(pg_type_to_arrow("numeric(50,10)"), DataType::Utf8));
    }

    #[test]
    fn test_pg_money_type() {
        // Money is not explicitly handled, falls back to Utf8
        assert!(matches!(pg_type_to_arrow("money"), DataType::Utf8));
    }

    // =========================================================================
    // String types
    // =========================================================================

    #[test]
    fn test_pg_char_types() {
        assert!(matches!(pg_type_to_arrow("char"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("bpchar"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("character"), DataType::Utf8));
    }

    #[test]
    fn test_pg_varchar_types() {
        assert!(matches!(pg_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("character varying"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_pg_text_type() {
        assert!(matches!(pg_type_to_arrow("text"), DataType::Utf8));
    }

    #[test]
    fn test_pg_name_type() {
        assert!(matches!(pg_type_to_arrow("name"), DataType::Utf8));
    }

    // =========================================================================
    // Binary types
    // =========================================================================

    #[test]
    fn test_pg_bytea_type() {
        assert!(matches!(pg_type_to_arrow("bytea"), DataType::Binary));
    }

    // =========================================================================
    // Date/Time types
    // =========================================================================

    #[test]
    fn test_pg_date_type() {
        assert!(matches!(pg_type_to_arrow("date"), DataType::Date32));
    }

    #[test]
    fn test_pg_timestamp_without_tz() {
        match pg_type_to_arrow("timestamp") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }

        match pg_type_to_arrow("timestamp without time zone") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_pg_timestamp_with_tz() {
        match pg_type_to_arrow("timestamptz") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with timezone, got {:?}", other),
        }

        match pg_type_to_arrow("timestamp with time zone") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with timezone, got {:?}", other),
        }
    }

    #[test]
    fn test_pg_time_types() {
        // Time stored as string since Arrow time handling is complex
        assert!(matches!(pg_type_to_arrow("time"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("time without time zone"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_pg_interval_type() {
        // Interval falls back to Utf8
        assert!(matches!(pg_type_to_arrow("interval"), DataType::Utf8));
    }

    // =========================================================================
    // JSON types
    // =========================================================================

    #[test]
    fn test_pg_json_types() {
        assert!(matches!(pg_type_to_arrow("json"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("jsonb"), DataType::Utf8));
    }

    // =========================================================================
    // UUID type
    // =========================================================================

    #[test]
    fn test_pg_uuid_type() {
        assert!(matches!(pg_type_to_arrow("uuid"), DataType::Utf8));
    }

    // =========================================================================
    // Network types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_pg_network_types() {
        // Network types are not explicitly handled, fall back to Utf8
        assert!(matches!(pg_type_to_arrow("inet"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("cidr"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("macaddr"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("macaddr8"), DataType::Utf8));
    }

    // =========================================================================
    // Geometric types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_pg_geometric_types() {
        // Geometric types are not explicitly handled, fall back to Utf8
        assert!(matches!(pg_type_to_arrow("point"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("line"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("lseg"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("box"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("path"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("polygon"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("circle"), DataType::Utf8));
    }

    // =========================================================================
    // Array types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_pg_array_types() {
        // Array types are not explicitly handled, fall back to Utf8
        assert!(matches!(pg_type_to_arrow("integer[]"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("text[]"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("_int4"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("_text"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("varchar[]"), DataType::Utf8));
    }

    // =========================================================================
    // Range types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_pg_range_types() {
        // Range types are not explicitly handled, fall back to Utf8
        assert!(matches!(pg_type_to_arrow("int4range"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("int8range"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("numrange"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("tsrange"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("tstzrange"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("daterange"), DataType::Utf8));
    }

    // =========================================================================
    // Unknown types (fall back to Utf8)
    // =========================================================================

    #[test]
    fn test_pg_unknown_type_fallback() {
        assert!(matches!(pg_type_to_arrow("unknown_type"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("custom_type"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("my_domain"), DataType::Utf8));
    }

    // =========================================================================
    // Case insensitivity
    // =========================================================================

    #[test]
    fn test_pg_case_insensitivity() {
        // All type matching should be case-insensitive
        assert!(matches!(pg_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("Boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("Integer"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("VARCHAR"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("VarChar"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("TIMESTAMPTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ));
        assert!(matches!(
            pg_type_to_arrow("TimestampTz"),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ));
    }

    // =========================================================================
    // BigDecimal to i128 conversion
    // =========================================================================

    #[test]
    fn test_bigdecimal_to_i128_exact_scale() {
        use std::str::FromStr;
        // 123.45 with scale=2 -> should convert to 12345 for Decimal128(10, 2)
        let bd = BigDecimal::from_str("123.45").unwrap();
        let result = bigdecimal_to_i128(&bd, 10, 2);
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_bigdecimal_to_i128_scale_up() {
        use std::str::FromStr;
        // 123.45 with scale=2, target scale=4 -> should multiply by 100 to get 1234500
        let bd = BigDecimal::from_str("123.45").unwrap();
        let result = bigdecimal_to_i128(&bd, 10, 4);
        assert_eq!(result.unwrap(), 1234500);
    }

    #[test]
    fn test_bigdecimal_to_i128_scale_down_exact() {
        use std::str::FromStr;
        // 123.4500 with scale=4, target scale=2 -> should divide exactly to 12345
        let bd = BigDecimal::from_str("123.4500").unwrap();
        let result = bigdecimal_to_i128(&bd, 10, 2);
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_bigdecimal_to_i128_rejects_truncation() {
        use std::str::FromStr;
        // 123.4567 with scale=4, target scale=2 -> should error, not truncate
        let bd = BigDecimal::from_str("123.4567").unwrap();
        let result = bigdecimal_to_i128(&bd, 10, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("more fractional digits than target scale"));
    }

    #[test]
    fn test_bigdecimal_to_i128_rejects_precision_overflow() {
        use std::str::FromStr;
        // 12345678901 exceeds precision 10 for Decimal128(10, 0)
        let bd = BigDecimal::from_str("12345678901").unwrap();
        let result = bigdecimal_to_i128(&bd, 10, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds precision"));
    }
}
