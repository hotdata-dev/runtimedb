//! BigQuery native driver implementation using gcp-bigquery-client

use datafusion::arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_cell::TableCell;
use gcp_bigquery_client::model::table_row::TableRow;
use gcp_bigquery_client::Client;
use std::sync::Arc;
use tracing::warn;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Build a BigQuery client from source credentials.
/// The credential is expected to be a GCP service account key JSON string.
async fn build_client(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Client, DataFetchError> {
    let credential = match source {
        Source::Bigquery { credential, .. } => credential,
        _ => {
            return Err(DataFetchError::Connection(
                "Expected BigQuery source".to_string(),
            ))
        }
    };

    let cred_json = credential
        .resolve(secrets)
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    let sa_key = serde_json::from_str(&cred_json).map_err(|e| {
        DataFetchError::Connection(format!("Invalid service account key JSON: {}", e))
    })?;

    Client::from_service_account_key(sa_key, false)
        .await
        .map_err(|e| DataFetchError::Connection(format!("Failed to create BigQuery client: {}", e)))
}

/// Discover tables and columns from BigQuery
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let client = build_client(source, secrets).await?;

    let (project_id, dataset_filter, region) = match source {
        Source::Bigquery {
            project_id,
            dataset,
            region,
            ..
        } => (project_id.as_str(), dataset.as_deref(), region.as_str()),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected BigQuery source".to_string(),
            ))
        }
    };

    // Build discovery query with optional dataset filter
    let query_sql = if let Some(dataset) = dataset_filter {
        format!(
            r#"
            SELECT
                c.table_catalog,
                c.table_schema,
                c.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position
            FROM `{project_id}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS c
            JOIN `{project_id}`.`{dataset}`.INFORMATION_SCHEMA.TABLES t
                ON c.table_catalog = t.table_catalog
                AND c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            "#,
            project_id = project_id.replace('`', "\\`"),
            dataset = dataset.replace('`', "\\`"),
        )
    } else {
        // Without a dataset filter, query all datasets via region-level INFORMATION_SCHEMA
        format!(
            r#"
            SELECT
                c.table_catalog,
                c.table_schema,
                c.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position
            FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.COLUMNS c
            JOIN `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.TABLES t
                ON c.table_catalog = t.table_catalog
                AND c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            "#,
            project_id = project_id.replace('`', "\\`"),
            region = region.replace('`', "\\`"),
        )
    };

    let query_request = QueryRequest::new(&query_sql);

    let response = client
        .job()
        .query(project_id, query_request)
        .await
        .map_err(|e| DataFetchError::Query(format!("Discovery query failed: {}", e)))?;

    let rows = match &response.rows {
        Some(rows) => rows,
        None => return Ok(Vec::new()),
    };

    let mut tables: Vec<TableMetadata> = Vec::new();

    for row in rows {
        let cells = match &row.columns {
            Some(cells) => cells,
            None => continue,
        };
        if cells.len() < 8 {
            continue;
        }

        let catalog = cell_to_string(&cells[0]);
        let schema_name = cell_to_string(&cells[1]).unwrap_or_default();
        let table_name = cell_to_string(&cells[2]).unwrap_or_default();
        let table_type = cell_to_string(&cells[3]).unwrap_or_else(|| "BASE TABLE".to_string());
        let col_name = cell_to_string(&cells[4]).unwrap_or_default();
        let data_type_str = cell_to_string(&cells[5]).unwrap_or_else(|| "STRING".to_string());
        let is_nullable = cell_to_string(&cells[6])
            .map(|s| s.to_uppercase() == "YES")
            .unwrap_or(true);
        let ordinal = cell_to_string(&cells[7])
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        let column = ColumnMetadata {
            name: col_name,
            data_type: bigquery_type_to_arrow(&data_type_str),
            nullable: is_nullable,
            ordinal_position: ordinal,
        };

        if let Some(existing) = tables.iter_mut().find(|t| {
            t.catalog_name.as_deref() == catalog.as_deref()
                && t.schema_name == schema_name
                && t.table_name == table_name
        }) {
            existing.columns.push(column);
        } else {
            tables.push(TableMetadata {
                catalog_name: catalog,
                schema_name,
                table_name,
                table_type,
                columns: vec![column],
            });
        }
    }

    Ok(tables)
}

const BATCH_SIZE: usize = 10_000;

/// Fetch table data and write to Parquet
pub async fn fetch_table(
    source: &Source,
    secrets: &SecretManager,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut dyn BatchWriter,
) -> Result<(), DataFetchError> {
    let client = build_client(source, secrets).await?;

    let project_id = match source {
        Source::Bigquery { project_id, .. } => project_id.as_str(),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected BigQuery source".to_string(),
            ))
        }
    };

    // First, get the schema from INFORMATION_SCHEMA
    let schema_sql = format!(
        r#"
        SELECT column_name, data_type, is_nullable
        FROM `{project_id}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
        "#,
        project_id = project_id.replace('`', "\\`"),
        dataset = schema.replace('`', "\\`"),
        table = table.replace('\'', "\\'"),
    );

    let schema_request = QueryRequest::new(&schema_sql);

    let schema_response = client
        .job()
        .query(project_id, schema_request)
        .await
        .map_err(|e| DataFetchError::Query(format!("Schema query failed: {}", e)))?;

    let fields: Vec<Field> = match &schema_response.rows {
        Some(rows) if !rows.is_empty() => rows
            .iter()
            .filter_map(|row| {
                let cells = row.columns.as_ref()?;
                if cells.len() < 3 {
                    return None;
                }
                let col_name = cell_to_string(&cells[0])?;
                let data_type_str = cell_to_string(&cells[1]).unwrap_or_else(|| "STRING".to_string());
                let is_nullable = cell_to_string(&cells[2])
                    .map(|s| s.to_uppercase() == "YES")
                    .unwrap_or(true);
                Some(Field::new(
                    col_name,
                    bigquery_type_to_arrow(&data_type_str),
                    is_nullable,
                ))
            })
            .collect(),
        _ => {
            return Err(DataFetchError::Query(format!(
                "Table {}.{} not found or has no columns",
                schema, table
            )))
        }
    };

    let arrow_schema = Schema::new(fields);
    writer.init(&arrow_schema)?;

    // Now fetch the actual data
    let data_sql = format!(
        "SELECT * FROM `{}`.`{}`.`{}`",
        project_id.replace('`', "\\`"),
        schema.replace('`', "\\`"),
        table.replace('`', "\\`"),
    );

    let data_request = QueryRequest::new(&data_sql);

    let data_response = client
        .job()
        .query(project_id, data_request)
        .await
        .map_err(|e| DataFetchError::Query(format!("Fetch query failed: {}", e)))?;

    let job_id = data_response
        .job_reference
        .as_ref()
        .and_then(|jr| jr.job_id.clone())
        .ok_or_else(|| DataFetchError::Query("No job_id in query response".to_string()))?;

    // Process first page + paginate through remaining results
    let mut row_buffer: Vec<TableRow> = Vec::with_capacity(BATCH_SIZE);
    let mut page_token = data_response.page_token.clone();

    // Buffer rows from the first page
    if let Some(rows) = data_response.rows {
        for row in rows {
            row_buffer.push(row);
            if row_buffer.len() >= BATCH_SIZE {
                let batch = rows_to_batch(&row_buffer, &arrow_schema)?;
                writer.write_batch(&batch)?;
                row_buffer.clear();
            }
        }
    }

    // Fetch subsequent pages
    while let Some(token) = page_token {
        let result = client
            .job()
            .get_query_results(
                project_id,
                &job_id,
                GetQueryResultsParameters {
                    page_token: Some(token),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| DataFetchError::Query(format!("Pagination query failed: {}", e)))?;

        if let Some(rows) = result.rows {
            for row in rows {
                row_buffer.push(row);
                if row_buffer.len() >= BATCH_SIZE {
                    let batch = rows_to_batch(&row_buffer, &arrow_schema)?;
                    writer.write_batch(&batch)?;
                    row_buffer.clear();
                }
            }
        }

        page_token = result.page_token;
    }

    // Flush remaining rows
    if row_buffer.is_empty() {
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        writer.write_batch(&empty_batch)?;
    } else {
        let batch = rows_to_batch(&row_buffer, &arrow_schema)?;
        writer.write_batch(&batch)?;
    }

    Ok(())
}

/// Extract a string value from a BigQuery TableCell
fn cell_to_string(cell: &TableCell) -> Option<String> {
    match &cell.value {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
        Some(serde_json::Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

/// Convert BigQuery INFORMATION_SCHEMA data_type string to Arrow DataType
pub fn bigquery_type_to_arrow(bq_type: &str) -> DataType {
    let type_upper = bq_type.to_uppercase();
    let base_type = type_upper.split('<').next().unwrap_or(&type_upper).trim();
    let base_type = base_type.split('(').next().unwrap_or(base_type).trim();

    match base_type {
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "INT64" | "INT" | "SMALLINT" | "INTEGER" | "BIGINT" | "TINYINT" | "BYTEINT" => {
            DataType::Int64
        }
        "FLOAT" | "FLOAT64" => DataType::Float64,
        "NUMERIC" | "DECIMAL" => DataType::Utf8,
        "BIGNUMERIC" | "BIGDECIMAL" => DataType::Utf8,
        "STRING" => DataType::Utf8,
        "BYTES" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Utf8,
        "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "GEOGRAPHY" => DataType::Utf8,
        "JSON" => DataType::Utf8,
        "INTERVAL" => DataType::Utf8,
        "STRUCT" | "RECORD" => DataType::Utf8,
        "ARRAY" => DataType::Utf8,
        "RANGE" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

/// Build a RecordBatch from BigQuery rows
fn rows_to_batch(
    rows: &[gcp_bigquery_client::model::table_row::TableRow],
    schema: &Schema,
) -> Result<RecordBatch, DataFetchError> {
    let num_cols = schema.fields().len();
    let num_rows = rows.len();

    let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), num_rows))
        .collect();

    for row in rows {
        let cells = row.columns.as_ref().map(|c| c.as_slice()).unwrap_or(&[]);
        for col_idx in 0..num_cols {
            let cell = cells.get(col_idx);
            let data_type = schema.field(col_idx).data_type();
            append_cell(&mut builders[col_idx], cell, data_type);
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
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(_, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(capacity);
            if let Some(tz) = tz {
                b = b.with_timezone(tz.as_ref());
            }
            Box::new(b)
        }
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

/// Append a BigQuery cell value to the appropriate Arrow builder.
/// BigQuery REST API returns all values as JSON strings, so we parse them.
fn append_cell(
    builder: &mut Box<dyn ArrayBuilder>,
    cell: Option<&TableCell>,
    data_type: &DataType,
) {
    let value_str = cell
        .and_then(|c| c.value.as_ref())
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.as_str()),
            serde_json::Value::Null => None,
            _ => None,
        });

    match data_type {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            match value_str {
                Some(s) => b.append_value(s.eq_ignore_ascii_case("true")),
                None => b.append_null(),
            }
        }
        DataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match value_str.and_then(|s| s.parse::<i64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            match value_str.and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        DataType::Date32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            match value_str.and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()) {
                Some(date) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days = (date - epoch).num_days() as i32;
                    b.append_value(days);
                }
                None => b.append_null(),
            }
        }
        DataType::Timestamp(_, tz) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match value_str {
                Some(s) => {
                    // BigQuery TIMESTAMP returns epoch seconds as float string
                    // BigQuery DATETIME returns ISO format string
                    if tz.is_some() {
                        // TIMESTAMP: value is epoch seconds (e.g. "1.609459200E9" or "1609459200.0")
                        if let Ok(epoch_secs) = s.parse::<f64>() {
                            let micros = (epoch_secs * 1_000_000.0) as i64;
                            b.append_value(micros);
                        } else {
                            warn!(value = s, "Failed to parse BigQuery TIMESTAMP value");
                            b.append_null();
                        }
                    } else {
                        // DATETIME: value is ISO-like "2021-01-01 12:00:00" or "2021-01-01T12:00:00"
                        let parsed = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"));
                        match parsed {
                            Ok(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                            Err(_) => {
                                warn!(value = s, "Failed to parse BigQuery DATETIME value");
                                b.append_null();
                            }
                        }
                    }
                }
                None => b.append_null(),
            }
        }
        DataType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            match value_str {
                // BigQuery BYTES are base64 encoded
                Some(s) => match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s) {
                    Ok(bytes) => b.append_value(&bytes),
                    Err(_) => b.append_value(s.as_bytes()),
                },
                None => b.append_null(),
            }
        }
        _ => {
            // Default: store as string (Utf8)
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match value_str {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcp_bigquery_client::model::field_type::FieldType;
    use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;

    /// Convert BigQuery FieldType enum to Arrow DataType
    fn field_type_to_arrow(ft: &FieldType) -> DataType {
        match ft {
            FieldType::Bool | FieldType::Boolean => DataType::Boolean,
            FieldType::Int64 | FieldType::Integer => DataType::Int64,
            FieldType::Float64 | FieldType::Float => DataType::Float64,
            FieldType::Numeric => DataType::Utf8,
            FieldType::Bignumeric => DataType::Utf8,
            FieldType::String => DataType::Utf8,
            FieldType::Bytes => DataType::Binary,
            FieldType::Date => DataType::Date32,
            FieldType::Time => DataType::Utf8,
            FieldType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            FieldType::Geography => DataType::Utf8,
            FieldType::Json => DataType::Utf8,
            FieldType::Interval => DataType::Utf8,
            FieldType::Record | FieldType::Struct => DataType::Utf8,
        }
    }

    /// Build Arrow schema from BigQuery TableFieldSchema
    fn schema_from_fields(fields: &[TableFieldSchema]) -> Schema {
        let arrow_fields: Vec<Field> = fields
            .iter()
            .map(|f| {
                let nullable = f
                    .mode
                    .as_ref()
                    .map(|m| m.to_uppercase() != "REQUIRED")
                    .unwrap_or(true);
                Field::new(&f.name, field_type_to_arrow(&f.r#type), nullable)
            })
            .collect();
        Schema::new(arrow_fields)
    }

    // =========================================================================
    // Type mapping tests
    // =========================================================================

    #[test]
    fn test_bigquery_type_boolean() {
        assert!(matches!(bigquery_type_to_arrow("BOOL"), DataType::Boolean));
        assert!(matches!(
            bigquery_type_to_arrow("BOOLEAN"),
            DataType::Boolean
        ));
        assert!(matches!(bigquery_type_to_arrow("bool"), DataType::Boolean));
    }

    #[test]
    fn test_bigquery_type_integer() {
        assert!(matches!(bigquery_type_to_arrow("INT64"), DataType::Int64));
        assert!(matches!(bigquery_type_to_arrow("INT"), DataType::Int64));
        assert!(matches!(bigquery_type_to_arrow("INTEGER"), DataType::Int64));
        assert!(matches!(bigquery_type_to_arrow("BIGINT"), DataType::Int64));
        assert!(matches!(
            bigquery_type_to_arrow("SMALLINT"),
            DataType::Int64
        ));
        assert!(matches!(
            bigquery_type_to_arrow("TINYINT"),
            DataType::Int64
        ));
        assert!(matches!(
            bigquery_type_to_arrow("BYTEINT"),
            DataType::Int64
        ));
    }

    #[test]
    fn test_bigquery_type_float() {
        assert!(matches!(
            bigquery_type_to_arrow("FLOAT64"),
            DataType::Float64
        ));
        assert!(matches!(bigquery_type_to_arrow("FLOAT"), DataType::Float64));
    }

    #[test]
    fn test_bigquery_type_numeric() {
        assert!(matches!(bigquery_type_to_arrow("NUMERIC"), DataType::Utf8));
        assert!(matches!(bigquery_type_to_arrow("DECIMAL"), DataType::Utf8));
        assert!(matches!(
            bigquery_type_to_arrow("BIGNUMERIC"),
            DataType::Utf8
        ));
        assert!(matches!(
            bigquery_type_to_arrow("BIGDECIMAL"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_bigquery_type_string() {
        assert!(matches!(bigquery_type_to_arrow("STRING"), DataType::Utf8));
        assert!(matches!(bigquery_type_to_arrow("string"), DataType::Utf8));
    }

    #[test]
    fn test_bigquery_type_bytes() {
        assert!(matches!(bigquery_type_to_arrow("BYTES"), DataType::Binary));
    }

    #[test]
    fn test_bigquery_type_date() {
        assert!(matches!(bigquery_type_to_arrow("DATE"), DataType::Date32));
    }

    #[test]
    fn test_bigquery_type_time() {
        assert!(matches!(bigquery_type_to_arrow("TIME"), DataType::Utf8));
    }

    #[test]
    fn test_bigquery_type_datetime() {
        match bigquery_type_to_arrow("DATETIME") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp without tz, got {:?}", other),
        }
    }

    #[test]
    fn test_bigquery_type_timestamp() {
        match bigquery_type_to_arrow("TIMESTAMP") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }
    }

    #[test]
    fn test_bigquery_type_geography() {
        assert!(matches!(
            bigquery_type_to_arrow("GEOGRAPHY"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_bigquery_type_json() {
        assert!(matches!(bigquery_type_to_arrow("JSON"), DataType::Utf8));
    }

    #[test]
    fn test_bigquery_type_struct_and_record() {
        assert!(matches!(bigquery_type_to_arrow("STRUCT"), DataType::Utf8));
        assert!(matches!(bigquery_type_to_arrow("RECORD"), DataType::Utf8));
    }

    #[test]
    fn test_bigquery_type_array() {
        assert!(matches!(bigquery_type_to_arrow("ARRAY"), DataType::Utf8));
        // Parameterized ARRAY<STRING>
        assert!(matches!(
            bigquery_type_to_arrow("ARRAY<STRING>"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_bigquery_type_parameterized() {
        // NUMERIC(10,2) should still map to Utf8 (base type extraction)
        assert!(matches!(
            bigquery_type_to_arrow("NUMERIC(10,2)"),
            DataType::Utf8
        ));
        assert!(matches!(
            bigquery_type_to_arrow("STRING(100)"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_bigquery_type_case_insensitive() {
        assert!(matches!(bigquery_type_to_arrow("int64"), DataType::Int64));
        assert!(matches!(
            bigquery_type_to_arrow("Float64"),
            DataType::Float64
        ));
        assert!(matches!(
            bigquery_type_to_arrow("Timestamp"),
            DataType::Timestamp(_, _)
        ));
    }

    #[test]
    fn test_bigquery_type_unknown_fallback() {
        assert!(matches!(
            bigquery_type_to_arrow("UNKNOWN_TYPE"),
            DataType::Utf8
        ));
    }

    // =========================================================================
    // FieldType enum mapping tests
    // =========================================================================

    #[test]
    fn test_field_type_to_arrow_boolean() {
        assert!(matches!(
            field_type_to_arrow(&FieldType::Bool),
            DataType::Boolean
        ));
        assert!(matches!(
            field_type_to_arrow(&FieldType::Boolean),
            DataType::Boolean
        ));
    }

    #[test]
    fn test_field_type_to_arrow_integer() {
        assert!(matches!(
            field_type_to_arrow(&FieldType::Int64),
            DataType::Int64
        ));
        assert!(matches!(
            field_type_to_arrow(&FieldType::Integer),
            DataType::Int64
        ));
    }

    #[test]
    fn test_field_type_to_arrow_float() {
        assert!(matches!(
            field_type_to_arrow(&FieldType::Float64),
            DataType::Float64
        ));
        assert!(matches!(
            field_type_to_arrow(&FieldType::Float),
            DataType::Float64
        ));
    }

    #[test]
    fn test_field_type_to_arrow_string() {
        assert!(matches!(
            field_type_to_arrow(&FieldType::String),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_field_type_to_arrow_timestamp() {
        match field_type_to_arrow(&FieldType::Timestamp) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("Expected Timestamp with UTC, got {:?}", other),
        }
    }

    #[test]
    fn test_field_type_to_arrow_datetime() {
        match field_type_to_arrow(&FieldType::Datetime) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            other => panic!("Expected Timestamp without tz, got {:?}", other),
        }
    }

    // =========================================================================
    // Cell extraction tests
    // =========================================================================

    #[test]
    fn test_cell_to_string_values() {
        let string_cell = TableCell {
            value: Some(serde_json::Value::String("hello".to_string())),
        };
        assert_eq!(cell_to_string(&string_cell), Some("hello".to_string()));

        let number_cell = TableCell {
            value: Some(serde_json::Value::Number(serde_json::Number::from(42))),
        };
        assert_eq!(cell_to_string(&number_cell), Some("42".to_string()));

        let bool_cell = TableCell {
            value: Some(serde_json::Value::Bool(true)),
        };
        assert_eq!(cell_to_string(&bool_cell), Some("true".to_string()));

        let null_cell = TableCell {
            value: Some(serde_json::Value::Null),
        };
        assert_eq!(cell_to_string(&null_cell), None);

        let none_cell = TableCell { value: None };
        assert_eq!(cell_to_string(&none_cell), None);
    }

    // =========================================================================
    // Schema building tests
    // =========================================================================

    #[test]
    fn test_schema_from_fields() {
        let fields = vec![
            TableFieldSchema::string("name"),
            TableFieldSchema::integer("age"),
            TableFieldSchema::bool("active"),
        ];

        let schema = schema_from_fields(&fields);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(*schema.field(1).data_type(), DataType::Int64);
        assert_eq!(schema.field(2).name(), "active");
        assert_eq!(*schema.field(2).data_type(), DataType::Boolean);
    }

    // =========================================================================
    // Row conversion tests
    // =========================================================================

    #[test]
    fn test_rows_to_batch_basic() {

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]);

        let rows = vec![
            TableRow {
                columns: Some(vec![
                    TableCell {
                        value: Some(serde_json::Value::String("Alice".to_string())),
                    },
                    TableCell {
                        value: Some(serde_json::Value::String("30".to_string())),
                    },
                ]),
            },
            TableRow {
                columns: Some(vec![
                    TableCell {
                        value: Some(serde_json::Value::String("Bob".to_string())),
                    },
                    TableCell {
                        value: Some(serde_json::Value::String("25".to_string())),
                    },
                ]),
            },
        ];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let age_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(age_col.value(0), 30);
        assert_eq!(age_col.value(1), 25);
    }

    #[test]
    fn test_rows_to_batch_with_nulls() {

        let schema = Schema::new(vec![
            Field::new("value", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]);

        let rows = vec![TableRow {
            columns: Some(vec![
                TableCell {
                    value: Some(serde_json::Value::Null),
                },
                TableCell { value: None },
            ]),
        }];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_rows_to_batch_boolean() {

        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, true)]);

        let rows = vec![
            TableRow {
                columns: Some(vec![TableCell {
                    value: Some(serde_json::Value::String("true".to_string())),
                }]),
            },
            TableRow {
                columns: Some(vec![TableCell {
                    value: Some(serde_json::Value::String("false".to_string())),
                }]),
            },
        ];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
    }

    #[test]
    fn test_rows_to_batch_date() {

        let schema = Schema::new(vec![Field::new("d", DataType::Date32, true)]);

        let rows = vec![TableRow {
            columns: Some(vec![TableCell {
                value: Some(serde_json::Value::String("2021-06-15".to_string())),
            }]),
        }];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Date32Array>()
            .unwrap();
        // 2021-06-15 is 18793 days since epoch
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2021, 6, 15).unwrap();
        let expected_days = (expected - epoch).num_days() as i32;
        assert_eq!(col.value(0), expected_days);
    }

    #[test]
    fn test_rows_to_batch_timestamp() {

        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);

        // BigQuery TIMESTAMP returns epoch seconds as string
        let rows = vec![TableRow {
            columns: Some(vec![TableCell {
                value: Some(serde_json::Value::String("1609459200.0".to_string())),
            }]),
        }];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        // 2021-01-01 00:00:00 UTC = 1609459200 seconds = 1609459200000000 microseconds
        assert_eq!(col.value(0), 1609459200000000);
    }

    #[test]
    fn test_rows_to_batch_float() {

        let schema = Schema::new(vec![Field::new("f", DataType::Float64, true)]);

        let rows = vec![TableRow {
            columns: Some(vec![TableCell {
                value: Some(serde_json::Value::String("3.14".to_string())),
            }]),
        }];

        let batch = rows_to_batch(&rows, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.14).abs() < f64::EPSILON);
    }
}
