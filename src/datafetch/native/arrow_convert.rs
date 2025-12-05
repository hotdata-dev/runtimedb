//! Convert sqlx PostgreSQL rows to Arrow RecordBatches

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use sqlx::postgres::{PgColumn, PgRow};
use sqlx::{Column, Row, TypeInfo};
use std::sync::Arc;

use crate::datafetch::DataFetchError;

/// Build Arrow Schema from sqlx column metadata
pub fn schema_from_columns(columns: &[PgColumn]) -> Schema {
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
pub fn pg_type_to_arrow(pg_type: &str) -> DataType {
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
        "timestamp" | "timestamp without time zone" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" | "timestamp with time zone" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "interval" => DataType::Utf8,
        "character varying" | "character" => DataType::Utf8,
        _ => DataType::Utf8, // Default fallback
    }
}

/// Build a RecordBatch from PostgreSQL rows
pub fn rows_to_batch(rows: &[PgRow], schema: &Schema) -> Result<RecordBatch, DataFetchError> {
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

    let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> = builders
        .iter_mut()
        .map(|b| b.finish())
        .collect();

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
        DataType::Timestamp(_, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(capacity))
        }
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)), // Fallback to string
    }
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    row: &PgRow,
    idx: usize,
    data_type: &DataType,
) -> Result<(), DataFetchError> {
    match data_type {
        DataType::Boolean => {
            let b = builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
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
            let b = builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap();
            b.append_option(row.try_get::<f32, _>(idx).ok());
        }
        DataType::Float64 => {
            let b = builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
            b.append_option(row.try_get::<f64, _>(idx).ok());
        }
        DataType::Utf8 => {
            let b = builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            b.append_option(row.try_get::<String, _>(idx).ok());
        }
        DataType::Binary => {
            let b = builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
            b.append_option(row.try_get::<Vec<u8>, _>(idx).ok());
        }
        DataType::Date32 => {
            let b = builder.as_any_mut().downcast_mut::<Date32Builder>().unwrap();
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
            let b = builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            b.append_option(row.try_get::<String, _>(idx).ok());
        }
    }
    Ok(())
}