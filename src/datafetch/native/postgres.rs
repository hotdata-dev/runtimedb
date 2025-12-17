//! PostgreSQL native driver implementation using sqlx

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sqlx::postgres::{PgColumn, PgConnection, PgRow};
use sqlx::{Column, Connection, Row, TypeInfo};
use std::sync::Arc;
use urlencoding::encode;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

use super::StreamingParquetWriter;

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
    secrets: Option<&dyn SecretManager>,
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

    Ok(build_connection_string(host, port, user, database, &password))
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

/// Discover tables and columns from PostgreSQL
pub async fn discover_tables(
    source: &Source,
    secrets: Option<&dyn SecretManager>,
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
    secrets: Option<&dyn SecretManager>,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    let connection_string = resolve_connection_string(source, secrets).await?;
    let mut conn = connect_with_ssl_retry(&connection_string).await?;

    // Build query - properly escape identifiers
    let query = format!(
        "SELECT * FROM \"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
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
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
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
                        pg_type_to_arrow(&data_type),
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

/// Build Arrow Schema from sqlx column metadata
fn schema_from_columns(columns: &[PgColumn]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let name = col.name();
            let data_type = pg_type_to_arrow(col.type_info().name());
            Field::new(name, data_type, true) // Assume nullable
        })
        .collect();

    Schema::new(fields)
}

/// Convert PostgreSQL type name to Arrow DataType
fn pg_type_to_arrow(pg_type: &str) -> DataType {
    use datafusion::arrow::datatypes::TimeUnit;

    let type_lower = pg_type.to_lowercase();

    match type_lower.as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int2" | "smallint" => DataType::Int16,
        "int4" | "int" | "integer" => DataType::Int32,
        "int8" | "bigint" => DataType::Int64,
        "float4" | "real" => DataType::Float32,
        "float8" | "double precision" => DataType::Float64,
        // Complex types: fallback to Utf8 to avoid runtime panics
        "numeric" | "decimal" => DataType::Utf8,
        "varchar" | "text" | "char" | "bpchar" | "name" => DataType::Utf8,
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
        "character varying" | "character" => DataType::Utf8,
        _ => DataType::Utf8, // Default fallback
    }
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
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(_, _) => Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)), // Fallback to string
    }
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
            b.append_option(row.try_get::<bool, _>(idx).ok());
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
    fn test_pg_type_to_arrow_basic_types() {
        assert!(matches!(pg_type_to_arrow("bool"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("int2"), DataType::Int16));
        assert!(matches!(pg_type_to_arrow("smallint"), DataType::Int16));
        assert!(matches!(pg_type_to_arrow("int4"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("int8"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow("float4"), DataType::Float32));
        assert!(matches!(pg_type_to_arrow("real"), DataType::Float32));
        assert!(matches!(pg_type_to_arrow("float8"), DataType::Float64));
        assert!(matches!(
            pg_type_to_arrow("double precision"),
            DataType::Float64
        ));
    }

    #[test]
    fn test_pg_type_to_arrow_string_types() {
        assert!(matches!(pg_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("text"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("char"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("bpchar"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("character varying"),
            DataType::Utf8
        ));
        assert!(matches!(pg_type_to_arrow("uuid"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("json"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("jsonb"), DataType::Utf8));
    }

    #[test]
    fn test_pg_type_to_arrow_date_time_types() {
        assert!(matches!(pg_type_to_arrow("date"), DataType::Date32));
        assert!(matches!(pg_type_to_arrow("time"), DataType::Utf8));

        match pg_type_to_arrow("timestamp") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(
                    unit,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond
                ));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }

        match pg_type_to_arrow("timestamptz") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(
                    unit,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond
                ));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            _ => panic!("Expected Timestamp type with timezone"),
        }
    }

    #[test]
    fn test_pg_type_to_arrow_binary_types() {
        assert!(matches!(pg_type_to_arrow("bytea"), DataType::Binary));
    }

    #[test]
    fn test_pg_type_to_arrow_complex_types_fallback() {
        // Complex types should fall back to Utf8 to avoid runtime panics
        assert!(matches!(pg_type_to_arrow("numeric"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("decimal"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("interval"), DataType::Utf8));
    }

    #[test]
    fn test_pg_type_to_arrow_unknown_type_fallback() {
        // Unknown types should default to Utf8
        assert!(matches!(pg_type_to_arrow("unknown_type"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow("custom_type"), DataType::Utf8));
    }

    #[test]
    fn test_pg_type_to_arrow_case_insensitive() {
        assert!(matches!(pg_type_to_arrow("BOOLEAN"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("Boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("INTEGER"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("VarChar"), DataType::Utf8));
    }
}
