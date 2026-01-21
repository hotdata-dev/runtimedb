//! Integration tests for Iceberg native datafetch source.
//!
//! Tests verify table discovery and data fetching from an Iceberg REST catalog (Lakekeeper)
//! backed by MinIO for S3-compatible storage.
//!
//! Architecture:
//! - PostgreSQL: Lakekeeper metadata storage
//! - MinIO: S3-compatible object storage for Iceberg data files
//! - Lakekeeper: Iceberg REST Catalog server
//!
//! Run these tests with: cargo test --test iceberg_tests

use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
use runtimedb::datafetch::{BatchWriter, DataFetcher, NativeFetcher, StreamingParquetWriter};
use runtimedb::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use runtimedb::source::{Credential, IcebergCatalogType, Source};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use testcontainers_modules::{minio::MinIO, postgres::Postgres};

/// MinIO default credentials
const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";
const MINIO_BUCKET: &str = "warehouse";

/// Lakekeeper configuration
const LAKEKEEPER_IMAGE: &str = "quay.io/lakekeeper/catalog";
const LAKEKEEPER_TAG: &str = "latest";
const LAKEKEEPER_PORT: u16 = 8181;

/// Create a test SecretManager with temporary storage.
async fn test_secret_manager(dir: &TempDir) -> SecretManager {
    let db_path = dir.path().join("test_catalog.db");
    let catalog = Arc::new(
        SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap(),
    );
    catalog.run_migrations().await.unwrap();

    let key = [0x42u8; 32];
    let backend = Arc::new(EncryptedCatalogBackend::new(key, catalog.clone()));

    SecretManager::new(backend, catalog, ENCRYPTED_PROVIDER_TYPE)
}

/// Container infrastructure for Iceberg tests
struct IcebergTestInfra {
    #[allow(dead_code)]
    postgres: ContainerAsync<Postgres>,
    #[allow(dead_code)]
    minio: ContainerAsync<MinIO>,
    #[allow(dead_code)]
    lakekeeper: ContainerAsync<GenericImage>,
    catalog_uri: String,
}

impl IcebergTestInfra {
    async fn start() -> Self {
        // Start PostgreSQL for Lakekeeper metadata
        let postgres = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .expect("Failed to start PostgreSQL");

        // Get PostgreSQL's bridge IP for inter-container communication (cross-platform)
        let pg_bridge_ip = postgres.get_bridge_ip_address().await.unwrap();

        // Start MinIO for S3-compatible storage
        let minio = MinIO::default()
            .start()
            .await
            .expect("Failed to start MinIO");

        // Get MinIO's bridge IP for inter-container communication
        let minio_bridge_ip = minio.get_bridge_ip_address().await.unwrap();
        let minio_host_port = minio.get_host_port_ipv4(9000).await.unwrap();
        let minio_host = minio.get_host().await.unwrap();

        // Create the warehouse bucket in MinIO (from host)
        let minio_endpoint = format!("http://{}:{}", minio_host, minio_host_port);
        create_minio_bucket(&minio_endpoint, MINIO_BUCKET).await;

        // Build Lakekeeper container with environment variables
        // Use bridge IPs for inter-container communication (works on Linux, macOS, Windows)
        let pg_encryption_key = "0123456789abcdef0123456789abcdef";
        let pg_connection_url = format!(
            "postgres://postgres:postgres@{}:5432/postgres",
            pg_bridge_ip
        );
        let minio_internal_endpoint = format!("http://{}:9000", minio_bridge_ip);

        // Step 1: Run Lakekeeper migrate command
        // The Lakekeeper image is a minimal distroless image, so we can't use shell commands.
        // We need to run migrate first, then start serve in a separate container.
        let migrate = GenericImage::new(LAKEKEEPER_IMAGE, LAKEKEEPER_TAG)
            .with_wait_for(WaitFor::Nothing)
            .with_cmd(["migrate"])
            .with_startup_timeout(Duration::from_secs(60))
            .with_env_var("LAKEKEEPER__PG_ENCRYPTION_KEY", pg_encryption_key)
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_READ", &pg_connection_url)
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_WRITE", &pg_connection_url)
            .with_env_var("RUST_LOG", "info,lakekeeper=debug")
            .start()
            .await
            .expect("Failed to start Lakekeeper migrate");

        // Wait for migration to complete by reading stdout until container exits
        use tokio::io::AsyncReadExt;
        let mut stdout = migrate.stdout(true);
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf).await;

        // Step 2: Run Lakekeeper serve
        // Lakekeeper logs to stdout in JSON format. The ready message is "Lakekeeper is now running"
        let lakekeeper = GenericImage::new(LAKEKEEPER_IMAGE, LAKEKEEPER_TAG)
            .with_exposed_port(LAKEKEEPER_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Lakekeeper is now running"))
            .with_cmd(["serve"])
            .with_startup_timeout(Duration::from_secs(120))
            .with_env_var("LAKEKEEPER__PG_ENCRYPTION_KEY", pg_encryption_key)
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_READ", &pg_connection_url)
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_WRITE", &pg_connection_url)
            .with_env_var("RUST_LOG", "info,lakekeeper=debug")
            // S3 credentials for MinIO
            .with_env_var("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER)
            .with_env_var("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD)
            .with_env_var("AWS_REGION", "us-east-1")
            .with_env_var("AWS_ENDPOINT_URL", &minio_internal_endpoint)
            .with_env_var("AWS_ALLOW_HTTP", "true")
            .start()
            .await
            .expect("Failed to start Lakekeeper serve");

        let lakekeeper_port = lakekeeper
            .get_host_port_ipv4(LAKEKEEPER_PORT)
            .await
            .unwrap();
        let lakekeeper_host = lakekeeper.get_host().await.unwrap();

        let catalog_uri = format!("http://{}:{}", lakekeeper_host, lakekeeper_port);

        // Wait for Lakekeeper to be ready by polling the health endpoint
        let client = reqwest::Client::new();
        let health_url = format!("{}/health", catalog_uri);
        let mut ready = false;
        for _ in 0..30 {
            if let Ok(resp) = client.get(&health_url).send().await {
                if resp.status().is_success() {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        if !ready {
            panic!("Lakekeeper failed to become ready after 60 seconds");
        }

        // Bootstrap the catalog with a warehouse
        bootstrap_lakekeeper(&catalog_uri, &minio_internal_endpoint).await;

        Self {
            postgres,
            minio,
            lakekeeper,
            catalog_uri,
        }
    }
}

/// Create a bucket in MinIO using the mc (MinIO Client) via Docker
async fn create_minio_bucket(endpoint: &str, bucket: &str) {
    use std::process::Command;

    // Use the mc (MinIO Client) Docker image to create the bucket
    // MC_HOST format: http(s)://<ACCESS_KEY>:<SECRET_KEY>@<HOST>:<PORT>
    let mc_host = format!(
        "http://{}:{}@{}",
        MINIO_ROOT_USER,
        MINIO_ROOT_PASSWORD,
        endpoint.trim_start_matches("http://")
    );

    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network=host",
            "-e",
            &format!("MC_HOST_minio={}", mc_host),
            "minio/mc",
            "mb",
            "--ignore-existing",
            &format!("minio/{}", bucket),
        ])
        .output()
        .expect("Failed to run docker mc command - is Docker running?");

    if !output.status.success() {
        eprintln!(
            "Warning: mc mb command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Give MinIO time to process
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Bootstrap Lakekeeper with a warehouse and test namespace/table
async fn bootstrap_lakekeeper(catalog_uri: &str, minio_endpoint: &str) {
    let client = reqwest::Client::new();

    // Bootstrap Lakekeeper (creates initial admin and default project)
    let bootstrap_payload = serde_json::json!({
        "accept-terms-of-use": true,
        "is-operator": true
    });

    let url = format!("{}/management/v1/bootstrap", catalog_uri);
    // Bootstrap may fail if already done - this is expected
    match client.post(&url).json(&bootstrap_payload).send().await {
        Ok(resp) if !resp.status().is_success() => {
            eprintln!(
                "Bootstrap returned {}: may already be bootstrapped",
                resp.status()
            );
        }
        Err(e) => {
            eprintln!("Bootstrap request failed: {} - continuing anyway", e);
        }
        Ok(_) => {}
    }

    // Get the default project created by bootstrap
    let url = format!("{}/management/v1/project", catalog_uri);
    let project_id = match client.get(&url).send().await {
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|json| {
                    json.get("project-id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| "00000000-0000-0000-0000-000000000000".to_string())
        }
        Err(_) => "00000000-0000-0000-0000-000000000000".to_string(),
    };

    // Create a warehouse via Lakekeeper Management API
    let warehouse_payload = serde_json::json!({
        "warehouse-name": "test_warehouse",
        "project-id": project_id,
        "storage-profile": {
            "type": "s3",
            "bucket": MINIO_BUCKET,
            "region": "us-east-1",
            "path-style-access": true,
            "endpoint": minio_endpoint,
            "sts-enabled": false,
            "flavor": "minio"
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": MINIO_ROOT_USER,
            "aws-secret-access-key": MINIO_ROOT_PASSWORD
        }
    });

    let url = format!("{}/management/v1/warehouse", catalog_uri);
    let resp = client
        .post(&url)
        .json(&warehouse_payload)
        .send()
        .await
        .expect("Failed to create warehouse");

    let body = resp.text().await.unwrap_or_default();
    let warehouse_id = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("warehouse-id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "test_warehouse".to_string());

    // Wait for warehouse creation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Query GET /catalog/v1/config to get the prefix for this warehouse
    // Lakekeeper returns prefix in defaults.prefix (the warehouse UUID)
    fn parse_prefix_from_config(body: &str) -> Option<String> {
        serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|json| {
                json.get("defaults")
                    .and_then(|d| d.get("prefix"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
    }

    // Try with warehouse name (Lakekeeper requires this)
    let config_url = format!("{}/catalog/v1/config?warehouse=test_warehouse", catalog_uri);
    let prefix = match client.get(&config_url).send().await {
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            parse_prefix_from_config(&body).unwrap_or(warehouse_id)
        }
        Err(_) => warehouse_id,
    };

    // Create a namespace via Iceberg REST API using the prefix
    let namespace_payload = serde_json::json!({
        "namespace": ["test_namespace"],
        "properties": {}
    });

    let url = format!("{}/catalog/v1/{}/namespaces", catalog_uri, prefix);
    client
        .post(&url)
        .json(&namespace_payload)
        .send()
        .await
        .expect("Failed to create namespace");

    // Create a test table via Iceberg REST API
    let table_payload = serde_json::json!({
        "name": "test_table",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "type": "long", "required": true},
                {"id": 2, "name": "name", "type": "string", "required": false},
                {"id": 3, "name": "amount", "type": "double", "required": false}
            ]
        }
    });

    let url = format!(
        "{}/catalog/v1/{}/namespaces/test_namespace/tables",
        catalog_uri, prefix
    );
    client
        .post(&url)
        .json(&table_payload)
        .send()
        .await
        .expect("Failed to create table");

    // Wait for table creation
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn test_iceberg_rest_catalog_discovery() {
    // Start the test infrastructure
    let infra = IcebergTestInfra::start().await;

    // Create test context
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    // Create Iceberg source pointing to our test catalog
    // URI is the catalog base URL (Lakekeeper serves at /catalog)
    // The REST client will add /v1/config?warehouse=... etc
    let source = Source::Iceberg {
        catalog_type: IcebergCatalogType::Rest {
            uri: format!("{}/catalog", infra.catalog_uri),
            credential: Credential::None,
        },
        warehouse: "test_warehouse".to_string(),
        namespace: None, // Discover all namespaces
    };

    // Test table discovery
    let fetcher = NativeFetcher::new();
    let result = fetcher.discover_tables(&source, &secrets).await;

    match result {
        Ok(tables) => {
            // Verify we found the test table
            assert!(!tables.is_empty(), "Should discover at least one table");

            let test_table = tables
                .iter()
                .find(|t| t.table_name == "test_table" && t.schema_name == "test_namespace");

            assert!(
                test_table.is_some(),
                "Should find test_table in test_namespace. Found tables: {:?}",
                tables
                    .iter()
                    .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                    .collect::<Vec<_>>()
            );

            let table = test_table.unwrap();
            assert_eq!(table.columns.len(), 3, "Table should have 3 columns");

            // Verify column metadata
            let id_col = table.columns.iter().find(|c| c.name == "id");
            assert!(id_col.is_some(), "Should have id column");

            let name_col = table.columns.iter().find(|c| c.name == "name");
            assert!(name_col.is_some(), "Should have name column");

            let amount_col = table.columns.iter().find(|c| c.name == "amount");
            assert!(amount_col.is_some(), "Should have amount column");
        }
        Err(e) => {
            panic!("Table discovery failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_iceberg_rest_catalog_fetch_empty_table() {
    // Start the test infrastructure
    let infra = IcebergTestInfra::start().await;

    // Create test context
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    // Create Iceberg source
    // URI is the catalog base URL (Lakekeeper serves at /catalog)
    let source = Source::Iceberg {
        catalog_type: IcebergCatalogType::Rest {
            uri: format!("{}/catalog", infra.catalog_uri),
            credential: Credential::None,
        },
        warehouse: "test_warehouse".to_string(),
        namespace: Some("test_namespace".to_string()),
    };

    // Test fetching an empty table (no data files)
    let fetcher = NativeFetcher::new();
    let output_path = temp_dir.path().join("output.parquet");
    let mut writer = StreamingParquetWriter::new(output_path.clone());

    let result = fetcher
        .fetch_table(
            &source,
            &secrets,
            None,
            "test_namespace",
            "test_table",
            &mut writer,
        )
        .await;

    // Empty tables should still succeed (schema written, no data)
    assert!(
        result.is_ok(),
        "Fetching empty table should succeed: {:?}",
        result.err()
    );

    Box::new(writer).close().unwrap();

    // Verify the parquet file was created with correct schema
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(&output_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut total_rows = 0;
    let mut schema = None;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
        if schema.is_none() {
            schema = Some(batch.schema());
        }
    }

    // Empty table should have 0 rows
    assert_eq!(total_rows, 0, "Empty table should have 0 rows");
}

#[tokio::test]
async fn test_iceberg_rest_catalog_with_namespace_filter() {
    // Start the test infrastructure
    let infra = IcebergTestInfra::start().await;

    // Create test context
    let temp_dir = TempDir::new().unwrap();
    let secrets = test_secret_manager(&temp_dir).await;

    // Create Iceberg source with namespace filter
    // URI is the catalog base URL (Lakekeeper serves at /catalog)
    let source = Source::Iceberg {
        catalog_type: IcebergCatalogType::Rest {
            uri: format!("{}/catalog", infra.catalog_uri),
            credential: Credential::None,
        },
        warehouse: "test_warehouse".to_string(),
        namespace: Some("test_namespace".to_string()),
    };

    // Test table discovery with namespace filter
    let fetcher = NativeFetcher::new();
    let result = fetcher.discover_tables(&source, &secrets).await;

    match result {
        Ok(tables) => {
            // All discovered tables should be in the filtered namespace
            for table in &tables {
                assert_eq!(
                    table.schema_name, "test_namespace",
                    "All tables should be in filtered namespace"
                );
            }
        }
        Err(e) => {
            panic!("Table discovery with namespace filter failed: {}", e);
        }
    }
}
