//! Snowflake native driver implementation using snowflake-api
//!
//! Note: Due to arrow version mismatch between snowflake-api (arrow 54) and datafusion (arrow 56),
//! we use IPC serialization to bridge the versions.

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use snowflake_api::{QueryResult, SnowflakeApi};
use std::sync::Arc;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::types::GeometryColumnInfo;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

// Re-exports of arrow 54 for snowflake-api compatibility
use arrow_array_54 as arrow54_array;
use arrow_ipc_54 as arrow54_ipc;

/// Credential format for password authentication
#[derive(Deserialize)]
struct PasswordCredential {
    password: String,
}

/// Credential format for key-pair authentication
#[derive(Deserialize)]
struct KeyPairCredential {
    private_key: String,
}

/// Parse credential JSON and build Snowflake client
async fn build_client(
    source: &Source,
    secrets: &SecretManager,
) -> Result<SnowflakeApi, DataFetchError> {
    let (account, user, warehouse, database, schema, role, credential) = match source {
        Source::Snowflake {
            account,
            user,
            warehouse,
            database,
            schema,
            role,
            credential,
        } => (account, user, warehouse, database, schema, role, credential),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected Snowflake source".to_string(),
            ))
        }
    };

    let cred_json = credential
        .resolve(secrets)
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    // Try to parse as password credential first, then key-pair
    // API signature: with_password_auth(account, warehouse, database, schema, username, role, password)
    let client = if let Ok(pwd_cred) = serde_json::from_str::<PasswordCredential>(&cred_json) {
        SnowflakeApi::with_password_auth(
            account,
            Some(warehouse.as_str()),
            Some(database.as_str()),
            schema.as_deref(),
            user,
            role.as_deref(),
            &pwd_cred.password,
        )
        .map_err(|e| {
            DataFetchError::Connection(format!("Failed to create Snowflake client: {}", e))
        })?
    } else if let Ok(key_cred) = serde_json::from_str::<KeyPairCredential>(&cred_json) {
        SnowflakeApi::with_certificate_auth(
            account,
            Some(warehouse.as_str()),
            Some(database.as_str()),
            schema.as_deref(),
            user,
            role.as_deref(),
            &key_cred.private_key,
        )
        .map_err(|e| {
            DataFetchError::Connection(format!(
                "Failed to create Snowflake client with key-pair: {}",
                e
            ))
        })?
    } else {
        return Err(DataFetchError::Connection(
            "Invalid credential format: expected JSON with 'password' or 'private_key' field"
                .to_string(),
        ));
    };

    Ok(client)
}

/// Check connectivity to a Snowflake source
pub async fn check_health(source: &Source, secrets: &SecretManager) -> Result<(), DataFetchError> {
    let client = build_client(source, secrets).await?;
    client
        .exec("SELECT 1")
        .await
        .map_err(|e| DataFetchError::Query(format!("Health check failed: {}", e)))?;
    Ok(())
}

/// Discover tables and columns from Snowflake
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let client = build_client(source, secrets).await?;

    // Extract database and optional schema filter
    let (database, schema_filter) = match source {
        Source::Snowflake {
            database, schema, ..
        } => (database.as_str(), schema.as_deref()),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected Snowflake source".to_string(),
            ))
        }
    };

    // Build discovery query with optional schema filter
    let query = if let Some(schema) = schema_filter {
        format!(
            r#"
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_type,
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM "{database}".information_schema.columns
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
              AND table_schema = '{schema}'
            ORDER BY table_schema, table_name, ordinal_position
            "#,
            database = database.replace('"', "\"\""),
            schema = schema.replace('\'', "''")
        )
    } else {
        format!(
            r#"
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_type,
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM "{database}".information_schema.columns
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY table_schema, table_name, ordinal_position
            "#,
            database = database.replace('"', "\"\"")
        )
    };

    let result = client
        .exec(&query)
        .await
        .map_err(|e| DataFetchError::Query(format!("Discovery query failed: {}", e)))?;

    let batches = match result {
        QueryResult::Arrow(batches) => batches,
        QueryResult::Json(_) => {
            return Err(DataFetchError::Query(
                "Unexpected JSON response from Snowflake".to_string(),
            ))
        }
        QueryResult::Empty => return Ok(Vec::new()),
    };

    let mut tables: Vec<TableMetadata> = Vec::new();

    for batch in batches {
        // Convert arrow 54 batch to arrow 56 for processing
        let batch = convert_arrow_batch(&batch)?;
        let num_rows = batch.num_rows();

        // Get columns by index (0-7 based on SELECT order)
        let catalog_col = batch.column(0);
        let schema_col = batch.column(1);
        let table_col = batch.column(2);
        let table_type_col = batch.column(3);
        let col_name_col = batch.column(4);
        let data_type_col = batch.column(5);
        let nullable_col = batch.column(6);
        let ordinal_col = batch.column(7);

        for row in 0..num_rows {
            let catalog = get_string_value(catalog_col.as_ref(), row);
            let schema_name = get_string_value(schema_col.as_ref(), row).unwrap_or_default();
            let table_name = get_string_value(table_col.as_ref(), row).unwrap_or_default();
            let table_type = get_string_value(table_type_col.as_ref(), row)
                .unwrap_or_else(|| "BASE TABLE".to_string());
            let col_name = get_string_value(col_name_col.as_ref(), row).unwrap_or_default();
            let data_type_str = get_string_value(data_type_col.as_ref(), row)
                .unwrap_or_else(|| "VARCHAR".to_string());
            let is_nullable = get_string_value(nullable_col.as_ref(), row)
                .map(|s| s.to_uppercase() == "YES")
                .unwrap_or(true);
            let ordinal = get_int_value(ordinal_col.as_ref(), row).unwrap_or(0) as i32;

            // Check if this is a spatial column
            let is_spatial = is_spatial_type(&data_type_str);

            let column = ColumnMetadata {
                name: col_name.clone(),
                data_type: snowflake_type_to_arrow(&data_type_str),
                nullable: is_nullable,
                ordinal_position: ordinal,
            };

            // Find or create table entry
            if let Some(existing) = tables.iter_mut().find(|t| {
                t.catalog_name == catalog
                    && t.schema_name == schema_name
                    && t.table_name == table_name
            }) {
                existing.columns.push(column);
                // Add geometry column info if spatial
                if is_spatial {
                    let geo_info = parse_snowflake_geometry_info(&data_type_str);
                    existing.geometry_columns.insert(col_name, geo_info);
                }
            } else {
                let mut table_meta = TableMetadata {
                    catalog_name: catalog,
                    schema_name,
                    table_name,
                    table_type,
                    columns: vec![column],
                    geometry_columns: std::collections::HashMap::new(),
                };
                // Add geometry column info if spatial
                if is_spatial {
                    let geo_info = parse_snowflake_geometry_info(&data_type_str);
                    table_meta.geometry_columns.insert(col_name, geo_info);
                }
                tables.push(table_meta);
            }
        }
    }

    Ok(tables)
}

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

    // Get database from source
    let database = match source {
        Source::Snowflake { database, .. } => database.as_str(),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected Snowflake source".to_string(),
            ))
        }
    };

    // Query column info to detect spatial columns
    // Try exact case first to avoid mixing columns from case-variant tables (e.g., "Foo" vs "FOO")
    // Only fall back to case-insensitive if exact match returns no rows
    let exact_schema_query = format!(
        r#"
        SELECT column_name, data_type
        FROM "{database}".information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        "#,
        database = database.replace('"', "\"\""),
        schema = schema.replace('\'', "''"),
        table = table.replace('\'', "''")
    );

    let case_insensitive_schema_query = format!(
        r#"
        SELECT column_name, data_type
        FROM "{database}".information_schema.columns
        WHERE UPPER(table_schema) = UPPER('{schema}') AND UPPER(table_name) = UPPER('{table}')
        ORDER BY ordinal_position
        "#,
        database = database.replace('"', "\"\""),
        schema = schema.replace('\'', "''"),
        table = table.replace('\'', "''")
    );

    // Helper to parse column expressions from query result
    fn parse_column_exprs(
        batches: &[arrow54_array::RecordBatch],
    ) -> Result<Vec<String>, DataFetchError> {
        let mut exprs = Vec::new();
        for batch in batches {
            let converted = convert_arrow_batch(batch)?;
            for row in 0..converted.num_rows() {
                if let (Some(col_name), Some(data_type)) = (
                    get_string_value(converted.column(0).as_ref(), row),
                    get_string_value(converted.column(1).as_ref(), row),
                ) {
                    let escaped_col = format!("\"{}\"", col_name.replace('"', "\"\""));
                    if is_spatial_type(&data_type) {
                        exprs.push(format!("ST_AsBinary({}) AS {}", escaped_col, escaped_col));
                    } else {
                        exprs.push(escaped_col);
                    }
                }
            }
        }
        Ok(exprs)
    }

    // Try exact case first
    let column_exprs: Vec<String> = {
        let exact_result = client
            .exec(&exact_schema_query)
            .await
            .map_err(|e| DataFetchError::Query(format!("Schema query failed: {}", e)))?;

        match exact_result {
            QueryResult::Arrow(batches) if !batches.is_empty() => {
                let exprs = parse_column_exprs(&batches)?;
                if !exprs.is_empty() {
                    exprs
                } else {
                    // Exact query returned batches but no columns, try case-insensitive
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    };

    // If exact case returned nothing, try case-insensitive fallback
    let column_exprs = if column_exprs.is_empty() {
        let fallback_result = client
            .exec(&case_insensitive_schema_query)
            .await
            .map_err(|e| DataFetchError::Query(format!("Schema query failed: {}", e)))?;

        match fallback_result {
            QueryResult::Arrow(batches) => parse_column_exprs(&batches)?,
            _ => Vec::new(),
        }
    } else {
        column_exprs
    };

    // Build SELECT query with column expressions
    // Guard against empty column list which could silently skip ST_AsBinary wrapping
    let select_clause = if column_exprs.is_empty() {
        // If schema query returned nothing, check if table has spatial columns
        // This handles edge cases like permissions issues where info_schema doesn't return columns
        let spatial_check_query = format!(
            r#"
            SELECT COUNT(*) as cnt
            FROM "{database}".information_schema.columns
            WHERE (table_schema = '{schema}' AND table_name = '{table}')
               OR (UPPER(table_schema) = UPPER('{schema}') AND UPPER(table_name) = UPPER('{table}'))
            AND data_type IN ('GEOGRAPHY', 'GEOMETRY')
            "#,
            database = database.replace('"', "\"\""),
            schema = schema.replace('\'', "''"),
            table = table.replace('\'', "''")
        );

        let has_spatial = match client.exec(&spatial_check_query).await {
            Ok(QueryResult::Arrow(batches)) => batches.first().is_some_and(|b| {
                if let Ok(converted) = convert_arrow_batch(b) {
                    get_int_value(converted.column(0).as_ref(), 0).unwrap_or(0) > 0
                } else {
                    false
                }
            }),
            _ => false,
        };

        if has_spatial {
            return Err(DataFetchError::Query(
                "Table contains spatial columns but column schema query returned no results. \
                 Cannot fetch geometry data without ST_AsBinary() wrapping."
                    .to_string(),
            ));
        }
        "*".to_string()
    } else {
        column_exprs.join(", ")
    };
    let query = format!(
        r#"SELECT {} FROM "{}"."{}"."{}"#,
        select_clause,
        database.replace('"', "\"\""),
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );

    let result = client
        .exec(&query)
        .await
        .map_err(|e| DataFetchError::Query(format!("Fetch query failed: {}", e)))?;

    let batches = match result {
        QueryResult::Arrow(batches) => batches,
        QueryResult::Json(_) => {
            return Err(DataFetchError::Query(
                "Unexpected JSON response from Snowflake".to_string(),
            ))
        }
        QueryResult::Empty => {
            // Empty table - need to get schema from information_schema
            let schema_query = format!(
                r#"
                SELECT column_name, data_type, is_nullable
                FROM "{database}".information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
                "#,
                database = database.replace('"', "\"\""),
                schema = schema.replace('\'', "''"),
                table = table.replace('\'', "''")
            );

            let schema_result = client
                .exec(&schema_query)
                .await
                .map_err(|e| DataFetchError::Query(format!("Schema query failed: {}", e)))?;

            let arrow_schema = match schema_result {
                QueryResult::Arrow(batches) => schema_from_info_schema(&batches)?,
                _ => {
                    return Err(DataFetchError::Query(format!(
                        "Table {}.{} has no columns or does not exist",
                        schema, table
                    )))
                }
            };

            writer.init(&arrow_schema)?;
            let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
            writer.write_batch(&empty_batch)?;
            return Ok(());
        }
    };

    // Process batches
    let mut initialized = false;
    for batch in batches {
        // Convert from snowflake-api's arrow 54 to datafusion's arrow 56
        let converted_batch = convert_arrow_batch(&batch)?;

        if !initialized {
            writer.init(converted_batch.schema().as_ref())?;
            initialized = true;
        }
        writer.write_batch(&converted_batch)?;
    }

    Ok(())
}

/// Convert an arrow 54 RecordBatch (from snowflake-api) to datafusion arrow RecordBatch (arrow 56)
/// using IPC serialization as a bridge between arrow versions.
fn convert_arrow_batch(
    snowflake_batch: &arrow54_array::RecordBatch,
) -> Result<RecordBatch, DataFetchError> {
    use arrow54_ipc::writer::StreamWriter as Arrow54StreamWriter;
    use datafusion::arrow::ipc::reader::StreamReader as DatafusionStreamReader;

    // Serialize with arrow 54
    let mut buffer = Vec::new();
    {
        let mut stream_writer =
            Arrow54StreamWriter::try_new(&mut buffer, snowflake_batch.schema().as_ref())
                .map_err(|e| DataFetchError::Query(format!("IPC write error: {}", e)))?;
        stream_writer
            .write(snowflake_batch)
            .map_err(|e| DataFetchError::Query(format!("IPC write error: {}", e)))?;
        stream_writer
            .finish()
            .map_err(|e| DataFetchError::Query(format!("IPC finish error: {}", e)))?;
    }

    // Deserialize with datafusion's arrow (56)
    let cursor = std::io::Cursor::new(buffer);
    let mut stream_reader = DatafusionStreamReader::try_new(cursor, None)
        .map_err(|e| DataFetchError::Query(format!("IPC read error: {}", e)))?;

    stream_reader
        .next()
        .ok_or_else(|| DataFetchError::Query("Empty IPC stream".to_string()))?
        .map_err(|e| DataFetchError::Query(format!("IPC read error: {}", e)))
}

/// Convert Snowflake type string to Arrow DataType
fn snowflake_type_to_arrow(sf_type: &str) -> DataType {
    let type_upper = sf_type.to_uppercase();

    // Handle parameterized types like NUMBER(38,0), VARCHAR(255)
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "BOOLEAN" => DataType::Boolean,

        // Integer types
        "TINYINT" | "BYTEINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,

        // Numeric types - use Utf8 for precision preservation like Postgres
        "NUMBER" | "DECIMAL" | "NUMERIC" => DataType::Utf8,

        // Float types
        "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => DataType::Float64,

        // String types
        "VARCHAR" | "CHAR" | "CHARACTER" | "STRING" | "TEXT" => DataType::Utf8,

        // Binary
        "BINARY" | "VARBINARY" => DataType::Binary,

        // Date/Time types
        "DATE" => DataType::Date32,
        "TIME" => DataType::Utf8, // Time without timezone as string
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "DATETIME" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "TIMESTAMP_LTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TIMESTAMP_TZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),

        // Semi-structured types - store as JSON strings
        "VARIANT" | "OBJECT" | "ARRAY" => DataType::LargeUtf8,

        // Geography/Geometry - stored as Binary (WKB format)
        "GEOGRAPHY" | "GEOMETRY" => DataType::Binary,

        _ => DataType::Utf8, // Default fallback
    }
}

/// Check if a Snowflake type is a spatial type
pub fn is_spatial_type(sf_type: &str) -> bool {
    let type_upper = sf_type.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();
    matches!(base_type, "GEOGRAPHY" | "GEOMETRY")
}

/// Parse Snowflake geometry type info for GeoParquet metadata.
/// Snowflake only has GEOGRAPHY (WGS84, SRID 4326) and GEOMETRY (planar, SRID 0).
fn parse_snowflake_geometry_info(sf_type: &str) -> GeometryColumnInfo {
    let type_upper = sf_type.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    // GEOGRAPHY is always WGS84 (SRID 4326), GEOMETRY is planar (SRID 0)
    let srid = if base_type == "GEOGRAPHY" { 4326 } else { 0 };

    // Snowflake doesn't expose the specific geometry subtype (Point, Polygon, etc.)
    // in information_schema, so we use the generic type
    let geometry_type = Some("Geometry".to_string());

    GeometryColumnInfo {
        srid,
        geometry_type,
    }
}

/// Extract string value from Arrow array (supports both Utf8 and LargeUtf8)
fn get_string_value(array: &dyn datafusion::arrow::array::Array, row: usize) -> Option<String> {
    use datafusion::arrow::array::{LargeStringArray, StringArray};

    if array.is_null(row) {
        return None;
    }

    // Try StringArray (Utf8) first, then LargeStringArray (LargeUtf8)
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(row).to_string());
    }

    None
}

/// Extract integer value from Arrow array
fn get_int_value(array: &dyn datafusion::arrow::array::Array, row: usize) -> Option<i64> {
    use datafusion::arrow::array::{Int16Array, Int32Array, Int64Array};

    if array.is_null(row) {
        return None;
    }

    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row) as i64);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int16Array>() {
        return Some(a.value(row) as i64);
    }

    None
}

/// Build Arrow schema from information_schema query result
fn schema_from_info_schema(
    batches: &[arrow54_array::RecordBatch],
) -> Result<Schema, DataFetchError> {
    let mut fields = Vec::new();

    for batch in batches {
        // Convert to arrow 56 for processing
        let batch = convert_arrow_batch(batch)?;
        let num_rows = batch.num_rows();
        let col_name_col = batch.column(0);
        let data_type_col = batch.column(1);
        let nullable_col = batch.column(2);

        for row in 0..num_rows {
            let col_name = get_string_value(col_name_col.as_ref(), row).unwrap_or_default();
            let data_type_str = get_string_value(data_type_col.as_ref(), row)
                .unwrap_or_else(|| "VARCHAR".to_string());
            let is_nullable = get_string_value(nullable_col.as_ref(), row)
                .map(|s| s.to_uppercase() == "YES")
                .unwrap_or(true);

            fields.push(Field::new(
                col_name,
                snowflake_type_to_arrow(&data_type_str),
                is_nullable,
            ));
        }
    }

    if fields.is_empty() {
        return Err(DataFetchError::Query("Table has no columns".to_string()));
    }

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_type_to_arrow_boolean() {
        assert!(matches!(
            snowflake_type_to_arrow("BOOLEAN"),
            DataType::Boolean
        ));
    }

    #[test]
    fn test_snowflake_type_to_arrow_integers() {
        assert!(matches!(snowflake_type_to_arrow("TINYINT"), DataType::Int8));
        assert!(matches!(
            snowflake_type_to_arrow("SMALLINT"),
            DataType::Int16
        ));
        assert!(matches!(snowflake_type_to_arrow("INT"), DataType::Int32));
        assert!(matches!(
            snowflake_type_to_arrow("INTEGER"),
            DataType::Int32
        ));
        assert!(matches!(snowflake_type_to_arrow("BIGINT"), DataType::Int64));
    }

    #[test]
    fn test_snowflake_type_to_arrow_floats() {
        assert!(matches!(
            snowflake_type_to_arrow("FLOAT"),
            DataType::Float64
        ));
        assert!(matches!(
            snowflake_type_to_arrow("DOUBLE"),
            DataType::Float64
        ));
        assert!(matches!(snowflake_type_to_arrow("REAL"), DataType::Float64));
    }

    #[test]
    fn test_snowflake_type_to_arrow_strings() {
        assert!(matches!(snowflake_type_to_arrow("VARCHAR"), DataType::Utf8));
        assert!(matches!(
            snowflake_type_to_arrow("VARCHAR(255)"),
            DataType::Utf8
        ));
        assert!(matches!(snowflake_type_to_arrow("STRING"), DataType::Utf8));
        assert!(matches!(snowflake_type_to_arrow("TEXT"), DataType::Utf8));
    }

    #[test]
    fn test_snowflake_type_to_arrow_numeric() {
        // Numeric types fall back to Utf8 for precision preservation
        assert!(matches!(snowflake_type_to_arrow("NUMBER"), DataType::Utf8));
        assert!(matches!(
            snowflake_type_to_arrow("NUMBER(38,0)"),
            DataType::Utf8
        ));
        assert!(matches!(snowflake_type_to_arrow("DECIMAL"), DataType::Utf8));
    }

    #[test]
    fn test_snowflake_type_to_arrow_datetime() {
        assert!(matches!(snowflake_type_to_arrow("DATE"), DataType::Date32));

        match snowflake_type_to_arrow("TIMESTAMP_NTZ") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }

        match snowflake_type_to_arrow("TIMESTAMP_LTZ") {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            _ => panic!("Expected Timestamp type with timezone"),
        }
    }

    #[test]
    fn test_snowflake_type_to_arrow_semi_structured() {
        assert!(matches!(
            snowflake_type_to_arrow("VARIANT"),
            DataType::LargeUtf8
        ));
        assert!(matches!(
            snowflake_type_to_arrow("OBJECT"),
            DataType::LargeUtf8
        ));
        assert!(matches!(
            snowflake_type_to_arrow("ARRAY"),
            DataType::LargeUtf8
        ));
    }

    #[test]
    fn test_snowflake_type_to_arrow_binary() {
        assert!(matches!(
            snowflake_type_to_arrow("BINARY"),
            DataType::Binary
        ));
        assert!(matches!(
            snowflake_type_to_arrow("VARBINARY"),
            DataType::Binary
        ));
    }

    #[test]
    fn test_snowflake_type_to_arrow_case_insensitive() {
        assert!(matches!(
            snowflake_type_to_arrow("boolean"),
            DataType::Boolean
        ));
        assert!(matches!(
            snowflake_type_to_arrow("Boolean"),
            DataType::Boolean
        ));
        assert!(matches!(snowflake_type_to_arrow("varchar"), DataType::Utf8));
    }

    #[test]
    fn test_snowflake_type_to_arrow_unknown_fallback() {
        assert!(matches!(
            snowflake_type_to_arrow("UNKNOWN_TYPE"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_password_credential_parsing() {
        let json = r#"{"password": "secret123"}"#;
        let cred: PasswordCredential = serde_json::from_str(json).unwrap();
        assert_eq!(cred.password, "secret123");
    }

    #[test]
    fn test_keypair_credential_parsing() {
        let json =
            r#"{"private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"}"#;
        let cred: KeyPairCredential = serde_json::from_str(json).unwrap();
        assert!(cred.private_key.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn test_get_string_value_utf8() {
        use datafusion::arrow::array::StringArray;
        use std::sync::Arc;

        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(
            get_string_value(array_ref.as_ref(), 0),
            Some("hello".to_string())
        );
        assert_eq!(get_string_value(array_ref.as_ref(), 1), None);
        assert_eq!(
            get_string_value(array_ref.as_ref(), 2),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_get_string_value_large_utf8() {
        use datafusion::arrow::array::LargeStringArray;
        use std::sync::Arc;

        let array = LargeStringArray::from(vec![Some("large"), None, Some("string")]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(
            get_string_value(array_ref.as_ref(), 0),
            Some("large".to_string())
        );
        assert_eq!(get_string_value(array_ref.as_ref(), 1), None);
        assert_eq!(
            get_string_value(array_ref.as_ref(), 2),
            Some("string".to_string())
        );
    }

    #[test]
    fn test_get_int_value_int64() {
        use datafusion::arrow::array::Int64Array;
        use std::sync::Arc;

        let array = Int64Array::from(vec![Some(42i64), None, Some(-100i64)]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(get_int_value(array_ref.as_ref(), 0), Some(42));
        assert_eq!(get_int_value(array_ref.as_ref(), 1), None);
        assert_eq!(get_int_value(array_ref.as_ref(), 2), Some(-100));
    }

    #[test]
    fn test_get_int_value_int32() {
        use datafusion::arrow::array::Int32Array;
        use std::sync::Arc;

        let array = Int32Array::from(vec![Some(123i32), None, Some(456i32)]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(get_int_value(array_ref.as_ref(), 0), Some(123));
        assert_eq!(get_int_value(array_ref.as_ref(), 1), None);
        assert_eq!(get_int_value(array_ref.as_ref(), 2), Some(456));
    }

    #[test]
    fn test_get_int_value_int16() {
        use datafusion::arrow::array::Int16Array;
        use std::sync::Arc;

        let array = Int16Array::from(vec![Some(10i16), None, Some(20i16)]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(get_int_value(array_ref.as_ref(), 0), Some(10));
        assert_eq!(get_int_value(array_ref.as_ref(), 1), None);
        assert_eq!(get_int_value(array_ref.as_ref(), 2), Some(20));
    }

    #[test]
    fn test_get_int_value_unsupported_type() {
        use datafusion::arrow::array::Float64Array;
        use std::sync::Arc;

        // Float64 is not handled by get_int_value
        let array = Float64Array::from(vec![Some(1.5)]);
        let array_ref: Arc<dyn datafusion::arrow::array::Array> = Arc::new(array);

        assert_eq!(get_int_value(array_ref.as_ref(), 0), None);
    }
}
