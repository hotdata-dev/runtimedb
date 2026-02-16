//! DuckLake native driver implementation using datafusion-ducklake

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::prelude::SessionContext;
use datafusion_ducklake::{DuckLakeCatalog, PostgresMetadataProvider};
use object_store::aws::AmazonS3Builder;
use serde::Deserialize;
use url::Url;

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Credentials extracted from the credentials_json secret.
#[derive(Deserialize)]
struct DucklakeCredentials {
    catalog_db_password: String,
    s3_access_key_id: Option<String>,
    s3_secret_access_key: Option<String>,
}

/// Build the full catalog URL by injecting the password into the connection string.
fn build_catalog_url_with_password(
    catalog_url: &str,
    password: &str,
) -> Result<String, DataFetchError> {
    let mut parsed = Url::parse(catalog_url)
        .map_err(|e| DataFetchError::Connection(format!("Invalid catalog_url: {}", e)))?;
    parsed.set_password(Some(password)).map_err(|_| {
        DataFetchError::Connection("Cannot set password on catalog_url".to_string())
    })?;
    Ok(parsed.to_string())
}

/// Build a DuckLakeCatalog from source configuration.
async fn build_catalog(
    source: &Source,
    secrets: &SecretManager,
) -> Result<(Arc<DuckLakeCatalog>, DucklakeCredentials), DataFetchError> {
    let (catalog_url, credential) = match source {
        Source::Ducklake {
            catalog_url,
            credential,
            ..
        } => (catalog_url.as_str(), credential),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected DuckLake source".to_string(),
            ))
        }
    };

    let creds_json = credential
        .resolve(secrets)
        .await
        .map_err(|e| DataFetchError::Connection(e.to_string()))?;

    let creds: DucklakeCredentials = serde_json::from_str(&creds_json).map_err(|e| {
        DataFetchError::Connection(format!("Invalid DuckLake credentials JSON: {}", e))
    })?;

    let full_url = build_catalog_url_with_password(catalog_url, &creds.catalog_db_password)?;

    let provider = PostgresMetadataProvider::new(&full_url)
        .await
        .map_err(|e| {
            DataFetchError::Connection(format!("Failed to connect to catalog DB: {}", e))
        })?;

    let catalog = DuckLakeCatalog::new(provider).map_err(|e| {
        DataFetchError::Connection(format!("Failed to create DuckLake catalog: {}", e))
    })?;

    Ok((Arc::new(catalog), creds))
}

/// Discover tables and columns from a DuckLake catalog.
pub async fn discover_tables(
    source: &Source,
    secrets: &SecretManager,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let (catalog, _creds) = build_catalog(source, secrets).await?;

    let schema_names = catalog.schema_names();

    let mut tables = Vec::new();

    for schema_name in schema_names {
        // Skip system schemas
        if schema_name == "information_schema" {
            continue;
        }

        let schema = match catalog.schema(&schema_name) {
            Some(s) => s,
            None => continue,
        };

        let table_names = schema.table_names();

        for table_name in table_names {
            let table = match schema.table(&table_name).await {
                Ok(Some(t)) => t,
                _ => continue,
            };

            let arrow_schema = table.schema();
            let columns = arrow_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| ColumnMetadata {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    ordinal_position: i as i32,
                })
                .collect();

            tables.push(TableMetadata {
                catalog_name: None,
                schema_name: schema_name.clone(),
                table_name,
                table_type: "BASE TABLE".to_string(),
                columns,
            });
        }
    }

    Ok(tables)
}

/// Build a SessionContext with the DuckLake catalog and S3 object store registered.
async fn build_session_context(
    source: &Source,
    secrets: &SecretManager,
) -> Result<SessionContext, DataFetchError> {
    let (s3_endpoint, s3_region) = match source {
        Source::Ducklake {
            s3_endpoint,
            s3_region,
            ..
        } => (s3_endpoint.clone(), s3_region.clone()),
        _ => {
            return Err(DataFetchError::Connection(
                "Expected DuckLake source".to_string(),
            ))
        }
    };

    let (catalog, creds) = build_catalog(source, secrets).await?;

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", catalog.clone());

    // Get the data path from the catalog to determine the S3 bucket
    let data_path = catalog
        .provider()
        .get_data_path()
        .map_err(|e| DataFetchError::Connection(format!("Failed to get data path: {}", e)))?;

    // If the data path is an S3 URL, register an object store for the bucket
    if data_path.starts_with("s3://") {
        let parsed_data_url = Url::parse(&data_path)
            .map_err(|e| DataFetchError::Connection(format!("Invalid data path URL: {}", e)))?;
        let bucket = parsed_data_url.host_str().ok_or_else(|| {
            DataFetchError::Connection(format!("No bucket name in data path: {}", data_path))
        })?;

        let mut s3_builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_allow_http(true);

        if let Some(access_key) = &creds.s3_access_key_id {
            s3_builder = s3_builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &creds.s3_secret_access_key {
            s3_builder = s3_builder.with_secret_access_key(secret_key);
        }
        if let Some(endpoint) = &s3_endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
        }
        if let Some(region) = &s3_region {
            s3_builder = s3_builder.with_region(region);
        }

        let s3_store = s3_builder
            .build()
            .map_err(|e| DataFetchError::Connection(format!("Failed to build S3 store: {}", e)))?;

        let s3_url = Url::parse(&format!("s3://{}/", bucket))
            .map_err(|e| DataFetchError::Connection(e.to_string()))?;
        ctx.register_object_store(&s3_url, Arc::new(s3_store));
    }

    Ok(ctx)
}

/// Check connectivity to the DuckLake catalog database and object store.
pub async fn check_health(source: &Source, secrets: &SecretManager) -> Result<(), DataFetchError> {
    let ctx = build_session_context(source, secrets).await?;
    ctx.sql("SELECT 1")
        .await
        .map_err(|e| DataFetchError::Query(format!("DuckLake health check failed: {}", e)))?
        .collect()
        .await
        .map_err(|e| DataFetchError::Query(format!("DuckLake health check failed: {}", e)))?;
    Ok(())
}

/// Fetch table data from DuckLake and write to the batch writer.
pub async fn fetch_table(
    source: &Source,
    secrets: &SecretManager,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut dyn BatchWriter,
) -> Result<(), DataFetchError> {
    let ctx = build_session_context(source, secrets).await?;

    // Execute SELECT * FROM ducklake.{schema}.{table}
    let sql = format!(
        "SELECT * FROM ducklake.{}.{}",
        sanitize_identifier(schema),
        sanitize_identifier(table),
    );

    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| DataFetchError::Query(format!("DuckLake query failed: {}", e)))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| DataFetchError::Query(format!("DuckLake collect failed: {}", e)))?;

    if batches.is_empty() {
        // Get schema from the table provider directly for empty tables
        let catalog_provider = ctx
            .catalog("ducklake")
            .ok_or_else(|| DataFetchError::Query("DuckLake catalog not found".to_string()))?;
        let schema_provider = catalog_provider
            .schema(schema)
            .ok_or_else(|| DataFetchError::Query(format!("Schema '{}' not found", schema)))?;
        let table_provider = schema_provider
            .table(table)
            .await
            .map_err(|e| DataFetchError::Query(e.to_string()))?
            .ok_or_else(|| {
                DataFetchError::Query(format!("Table '{}.{}' not found", schema, table))
            })?;
        writer.init(table_provider.schema().as_ref())?;
    } else {
        writer.init(batches[0].schema().as_ref())?;
        for batch in &batches {
            writer.write_batch(batch)?;
        }
    }

    Ok(())
}

/// Sanitize a SQL identifier by quoting it with double quotes.
fn sanitize_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_catalog_url_with_password() {
        let url = build_catalog_url_with_password(
            "postgresql://user@localhost:5432/ducklake",
            "secret123",
        )
        .unwrap();
        assert_eq!(url, "postgresql://user:secret123@localhost:5432/ducklake");
    }

    #[test]
    fn test_build_catalog_url_with_special_chars_password() {
        let url = build_catalog_url_with_password(
            "postgresql://user@localhost:5432/ducklake",
            "p@ss w0rd!&=",
        )
        .unwrap();
        // Verify the password round-trips correctly through URL parsing
        let parsed = Url::parse(&url).unwrap();
        assert_eq!(parsed.username(), "user");
        // The url crate percent-encodes some special chars but not all
        // Verify the password is present and decodes correctly
        let decoded = urlencoding::decode(parsed.password().unwrap()).unwrap();
        assert_eq!(decoded, "p@ss w0rd!&=");
    }

    #[test]
    fn test_build_catalog_url_replaces_existing_password() {
        let url = build_catalog_url_with_password(
            "postgresql://user:oldpass@localhost:5432/ducklake",
            "newpass",
        )
        .unwrap();
        assert_eq!(url, "postgresql://user:newpass@localhost:5432/ducklake");
    }

    #[test]
    fn test_build_catalog_url_invalid_url() {
        let result = build_catalog_url_with_password("not-a-url", "pass");
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("my_table"), "\"my_table\"");
        assert_eq!(sanitize_identifier("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn test_ducklake_credentials_deserialization() {
        let json = r#"{"catalog_db_password": "secret", "s3_access_key_id": "AKIA...", "s3_secret_access_key": "wJal..."}"#;
        let creds: DucklakeCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.catalog_db_password, "secret");
        assert_eq!(creds.s3_access_key_id.as_deref(), Some("AKIA..."));
        assert_eq!(creds.s3_secret_access_key.as_deref(), Some("wJal..."));
    }

    #[test]
    fn test_ducklake_credentials_without_s3() {
        let json = r#"{"catalog_db_password": "secret"}"#;
        let creds: DucklakeCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.catalog_db_password, "secret");
        assert!(creds.s3_access_key_id.is_none());
        assert!(creds.s3_secret_access_key.is_none());
    }
}
