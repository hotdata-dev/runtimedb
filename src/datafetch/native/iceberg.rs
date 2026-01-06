//! Apache Iceberg native driver implementation

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::TryStreamExt;
use iceberg::spec::{PrimitiveType, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::{AwsCredentials, Credential, IcebergCatalogType, Source};

use super::StreamingParquetWriter;

// Re-exports of arrow 55 for iceberg compatibility
use arrow_array_55 as arrow55_array;
use arrow_ipc_55 as arrow55_ipc;

// Property constants for REST catalog
const REST_URI: &str = "uri";
const REST_WAREHOUSE: &str = "warehouse";
const REST_TOKEN: &str = "token";

// Property constants for Glue catalog
const GLUE_WAREHOUSE: &str = "warehouse";
const AWS_REGION: &str = "region_name";
const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const AWS_SESSION_TOKEN: &str = "aws_session_token";

/// Build an Iceberg catalog from source configuration.
async fn build_catalog(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Arc<dyn Catalog>, DataFetchError> {
    let (catalog_type, warehouse) = match source {
        Source::Iceberg {
            catalog_type,
            warehouse,
            ..
        } => (catalog_type, warehouse),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected Iceberg source".to_string(),
            ))
        }
    };

    match catalog_type {
        IcebergCatalogType::Rest { uri, credential } => {
            let mut props = HashMap::new();
            props.insert(REST_URI.to_string(), uri.clone());
            props.insert(REST_WAREHOUSE.to_string(), warehouse.clone());

            // Add auth token if credential provided
            if let Credential::SecretRef { .. } = credential {
                let token = credential
                    .resolve(secrets)
                    .await
                    .map_err(|e| DataFetchError::Connection(e.to_string()))?;
                props.insert(REST_TOKEN.to_string(), token);
            }

            let catalog = RestCatalogBuilder::default()
                .load("rest", props)
                .await
                .map_err(|e| DataFetchError::Connection(e.to_string()))?;

            Ok(Arc::new(catalog))
        }

        IcebergCatalogType::Glue { region, credential } => {
            let mut props = HashMap::new();
            props.insert(GLUE_WAREHOUSE.to_string(), warehouse.clone());
            props.insert(AWS_REGION.to_string(), region.clone());

            // If credential provided, use explicit keys; otherwise use IAM role
            if let Credential::SecretRef { .. } = credential {
                let creds_json = credential
                    .resolve(secrets)
                    .await
                    .map_err(|e| DataFetchError::Connection(e.to_string()))?;
                let aws_creds: AwsCredentials = serde_json::from_str(&creds_json).map_err(|e| {
                    DataFetchError::Connection(format!("Invalid AWS credentials JSON: {}", e))
                })?;
                props.insert(AWS_ACCESS_KEY_ID.to_string(), aws_creds.access_key_id);
                props.insert(
                    AWS_SECRET_ACCESS_KEY.to_string(),
                    aws_creds.secret_access_key,
                );
                if let Some(session_token) = aws_creds.session_token {
                    props.insert(AWS_SESSION_TOKEN.to_string(), session_token);
                }
            }

            let catalog = GlueCatalogBuilder::default()
                .load("glue", props)
                .await
                .map_err(|e| DataFetchError::Connection(e.to_string()))?;

            Ok(Arc::new(catalog))
        }
    }
}

/// Discover tables and columns from an Iceberg catalog.
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let catalog = build_catalog(source, secrets).await?;

    // Get namespace filter or list all
    let namespaces = match source {
        Source::Iceberg {
            namespace: Some(ns),
            ..
        } => {
            let parts: Vec<&str> = ns.split('.').collect();
            vec![NamespaceIdent::from_strs(&parts)
                .map_err(|e| DataFetchError::Query(e.to_string()))?]
        }
        Source::Iceberg {
            namespace: None, ..
        } => catalog
            .list_namespaces(None)
            .await
            .map_err(|e| DataFetchError::Query(e.to_string()))?,
        _ => unreachable!(),
    };

    let mut tables = Vec::new();

    for ns in namespaces {
        let table_idents = catalog
            .list_tables(&ns)
            .await
            .map_err(|e| DataFetchError::Query(e.to_string()))?;

        for table_ident in table_idents {
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| DataFetchError::Query(e.to_string()))?;

            let schema = table.metadata().current_schema();

            let columns = schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| ColumnMetadata {
                    name: field.name.clone(),
                    data_type: iceberg_type_to_arrow(&field.field_type),
                    nullable: !field.required,
                    ordinal_position: i as i32,
                })
                .collect();

            tables.push(TableMetadata {
                catalog_name: None,
                schema_name: ns.to_url_string(),
                table_name: table_ident.name().to_string(),
                table_type: "BASE TABLE".to_string(),
                columns,
            });
        }
    }

    Ok(tables)
}

/// Fetch table data and write to Parquet using streaming.
///
/// Note: Due to arrow version mismatch between iceberg (arrow 55) and datafusion (arrow 56),
/// we write directly using iceberg's parquet writer instead of StreamingParquetWriter.
pub async fn fetch_table(
    source: &Source,
    secrets: &SecretManager,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    let catalog = build_catalog(source, secrets).await?;

    let parts: Vec<&str> = schema.split('.').collect();
    let namespace =
        NamespaceIdent::from_strs(&parts).map_err(|e| DataFetchError::Query(e.to_string()))?;
    let table_ident = TableIdent::new(namespace, table.to_string());

    let iceberg_table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    // Build a scan for the current snapshot
    let scan = iceberg_table
        .scan()
        .build()
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    // Get Arrow schema from Iceberg schema and initialize writer
    let iceberg_schema = iceberg_table.metadata().current_schema();
    let arrow_schema = iceberg_schema_to_datafusion_arrow(iceberg_schema)?;
    writer.init(&arrow_schema)?;

    // Stream record batches from the scan
    // Note: iceberg returns arrow 55 RecordBatches, we convert to datafusion's arrow 56
    let batch_stream = scan
        .to_arrow()
        .await
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    let batches: Vec<_> = batch_stream
        .try_collect()
        .await
        .map_err(|e| DataFetchError::Query(e.to_string()))?;

    // Convert each batch from iceberg's arrow (55) to datafusion's arrow (56) via IPC
    for iceberg_batch in batches {
        let datafusion_batch = convert_arrow_batch(&iceberg_batch)?;
        writer.write_batch(&datafusion_batch)?;
    }

    Ok(())
}

/// Convert an iceberg arrow RecordBatch (arrow 55) to datafusion arrow RecordBatch (arrow 56)
/// using IPC serialization as a bridge between arrow versions.
fn convert_arrow_batch(
    iceberg_batch: &arrow55_array::RecordBatch,
) -> Result<datafusion::arrow::record_batch::RecordBatch, DataFetchError> {
    use arrow55_ipc::writer::StreamWriter as Arrow55StreamWriter;
    use datafusion::arrow::ipc::reader::StreamReader as DatafusionStreamReader;

    // Serialize with arrow 55
    let mut buffer = Vec::new();
    {
        let mut stream_writer =
            Arrow55StreamWriter::try_new(&mut buffer, iceberg_batch.schema().as_ref())
                .map_err(|e| DataFetchError::Query(format!("IPC write error: {}", e)))?;
        stream_writer
            .write(iceberg_batch)
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

/// Convert Iceberg schema to DataFusion Arrow schema.
fn iceberg_schema_to_datafusion_arrow(
    iceberg_schema: &iceberg::spec::Schema,
) -> Result<Schema, DataFetchError> {
    let fields: Vec<Field> = iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            Field::new(
                &field.name,
                iceberg_type_to_arrow(&field.field_type),
                !field.required,
            )
        })
        .collect();

    Ok(Schema::new(fields))
}

/// Convert Iceberg type to Arrow DataType.
fn iceberg_type_to_arrow(iceberg_type: &Type) -> DataType {
    match iceberg_type {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
            PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(len) => DataType::FixedSizeBinary(*len as i32),
            PrimitiveType::Binary => DataType::Binary,
        },
        // Nested types: fallback to string representation for discovery metadata
        Type::Struct(_) => DataType::Utf8,
        Type::List(_) => DataType::Utf8,
        Type::Map(_) => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_type_to_arrow_primitives() {
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Boolean)),
            DataType::Boolean
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Int)),
            DataType::Int32
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Long)),
            DataType::Int64
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Float)),
            DataType::Float32
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Double)),
            DataType::Float64
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::String)),
            DataType::Utf8
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Binary)),
            DataType::Binary
        ));
        assert!(matches!(
            iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Date)),
            DataType::Date32
        ));
    }

    #[test]
    fn test_iceberg_type_to_arrow_timestamp() {
        match iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamp)) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }

        match iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamptz)) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Microsecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            _ => panic!("Expected Timestamp type with timezone"),
        }

        match iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestampNs)) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Nanosecond));
                assert!(tz.is_none());
            }
            _ => panic!("Expected Timestamp type"),
        }

        match iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestamptzNs)) {
            DataType::Timestamp(unit, tz) => {
                assert!(matches!(unit, TimeUnit::Nanosecond));
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            _ => panic!("Expected Timestamp type with timezone"),
        }
    }

    #[test]
    fn test_iceberg_type_to_arrow_decimal() {
        match iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        })) {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal128 type"),
        }
    }
}
