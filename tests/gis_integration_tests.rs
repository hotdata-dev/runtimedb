//! GIS/PostGIS integration tests for RivetDB
//!
//! These tests verify end-to-end GIS functionality:
//! - Geometry column detection during schema discovery
//! - Fetching geometry data as WKB
//! - GeoParquet metadata in output files
//! - Spatial SQL functions via geodatafusion

use datafusion::arrow::datatypes::DataType;
use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
use runtimedb::datafetch::{BatchWriter, DataFetcher, NativeFetcher, StreamingParquetWriter};
use runtimedb::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use runtimedb::source::{Credential, Source};
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};

const TEST_PASSWORD: &str = "test_password";

/// Create a test SecretManager with temporary storage
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

/// Create a test SecretManager with a password stored
async fn create_test_secret_manager_with_password(
    dir: &TempDir,
    secret_name: &str,
    password: &str,
) -> (SecretManager, String) {
    let secrets = test_secret_manager(dir).await;
    let secret_id = secrets
        .create(secret_name, password.as_bytes())
        .await
        .unwrap();
    (secrets, secret_id)
}

/// Start a PostGIS-enabled PostgreSQL container
/// Uses kartoza/postgis which has multi-arch support (amd64 + arm64)
async fn start_postgis_container() -> ContainerAsync<GenericImage> {
    GenericImage::new("kartoza/postgis", "16-3.4")
        .with_exposed_port(5432.into())
        // Wait for the final startup message after the init sequence completes
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
            "restarting in foreground",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", TEST_PASSWORD)
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await
        .expect("Failed to start postgis container")
}

/// Wait for the database to be ready for connections
async fn wait_for_db(conn_str: &str) -> sqlx::PgPool {
    for attempt in 1..=30 {
        if let Ok(pool) = sqlx::PgPool::connect(conn_str).await {
            // Test connection is actually working
            if sqlx::query("SELECT 1").execute(&pool).await.is_ok() {
                return pool;
            }
            pool.close().await;
        }
        if attempt < 30 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
    panic!("Failed to connect to PostgreSQL after 30 attempts");
}

/// Test that geometry columns are detected during schema discovery
#[tokio::test(flavor = "multi_thread")]
async fn test_postgis_geometry_column_discovery() {
    let temp_dir = TempDir::new().unwrap();
    let (secrets, secret_id) =
        create_test_secret_manager_with_password(&temp_dir, "pg-pass", TEST_PASSWORD).await;

    let container = start_postgis_container().await;
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    // Wait for database to be ready
    let conn_str = format!(
        "postgres://postgres:{}@localhost:{}/postgres",
        TEST_PASSWORD, port
    );
    let pool = wait_for_db(&conn_str).await;

    // Enable PostGIS extension
    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(&pool)
        .await
        .unwrap();

    // Create test schema and table with geometry column
    sqlx::query("CREATE SCHEMA locations")
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE locations.addresses (
            id SERIAL PRIMARY KEY,
            house_num VARCHAR(20),
            street_name VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(2),
            zipcode VARCHAR(10),
            point GEOMETRY(Point, 4326)
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    pool.close().await;

    // Discover tables
    let fetcher = NativeFetcher::new();
    let source = Source::Postgres {
        host: "localhost".to_string(),
        port,
        user: "postgres".to_string(),
        database: "postgres".to_string(),
        credential: Credential::secret_ref(&secret_id),
    };

    let tables = fetcher
        .discover_tables(&source, &secrets)
        .await
        .expect("Discovery should succeed");

    // Find the addresses table
    let addresses = tables
        .iter()
        .find(|t| t.table_name == "addresses")
        .expect("Should find addresses table");

    // Verify geometry column was detected
    let point_col = addresses
        .columns
        .iter()
        .find(|c| c.name == "point")
        .expect("Should find point column");

    assert!(
        matches!(point_col.data_type, DataType::Binary),
        "Geometry column should map to Binary, got {:?}",
        point_col.data_type
    );

    // Verify geometry metadata was captured
    assert!(
        addresses.geometry_columns.contains_key("point"),
        "Should have geometry metadata for 'point' column"
    );

    let geom_info = addresses.geometry_columns.get("point").unwrap();
    assert_eq!(geom_info.srid, 4326, "SRID should be 4326");
    assert_eq!(
        geom_info.geometry_type.as_deref(),
        Some("POINT"),
        "Should detect Point geometry type"
    );
}

/// Test fetching geometry data as WKB and writing to GeoParquet
#[tokio::test(flavor = "multi_thread")]
async fn test_postgis_fetch_geometry_to_geoparquet() {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let temp_dir = TempDir::new().unwrap();
    let (secrets, secret_id) =
        create_test_secret_manager_with_password(&temp_dir, "pg-pass", TEST_PASSWORD).await;

    let container = start_postgis_container().await;
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let conn_str = format!(
        "postgres://postgres:{}@localhost:{}/postgres",
        TEST_PASSWORD, port
    );
    let pool = wait_for_db(&conn_str).await;

    // Enable PostGIS
    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(&pool)
        .await
        .unwrap();

    // Create and populate test table
    sqlx::query("CREATE SCHEMA locations")
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE locations.parcels (
            id SERIAL PRIMARY KEY,
            parcel_id VARCHAR(50),
            the_geom GEOMETRY(Polygon, 4326)
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test polygons of different sizes
    sqlx::query(
        "INSERT INTO locations.parcels (parcel_id, the_geom) VALUES
            ('PARCEL-001', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 4326)),
            ('PARCEL-002', ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))', 4326)),
            ('PARCEL-003', ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))', 4326))",
    )
    .execute(&pool)
    .await
    .unwrap();

    pool.close().await;

    // Fetch to parquet
    let fetcher = NativeFetcher::new();
    let source = Source::Postgres {
        host: "localhost".to_string(),
        port,
        user: "postgres".to_string(),
        database: "postgres".to_string(),
        credential: Credential::secret_ref(&secret_id),
    };

    let output_path = temp_dir.path().join("parcels.parquet");
    let mut writer = StreamingParquetWriter::new(output_path.clone());

    // Set geometry columns for GeoParquet metadata
    let mut geom_cols = std::collections::HashMap::new();
    geom_cols.insert(
        "the_geom".to_string(),
        runtimedb::datafetch::GeometryColumnInfo {
            srid: 4326,
            geometry_type: Some("Polygon".to_string()),
        },
    );
    writer.set_geometry_columns(geom_cols);

    let result = fetcher
        .fetch_table(&source, &secrets, None, "locations", "parcels", &mut writer)
        .await;
    assert!(result.is_ok(), "Fetch should succeed: {:?}", result.err());

    Box::new(writer).close().unwrap();

    // Verify GeoParquet metadata
    let file = File::open(&output_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();

    let geo_metadata = file_metadata
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|item| item.key == "geo"));

    assert!(
        geo_metadata.is_some(),
        "GeoParquet 'geo' metadata should be present"
    );

    let geo_value = geo_metadata.unwrap().value.as_ref().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(geo_value).unwrap();

    assert_eq!(parsed["version"], "1.1.0");
    assert_eq!(parsed["columns"]["the_geom"]["encoding"], "WKB");
    assert_eq!(parsed["columns"]["the_geom"]["crs"]["id"]["code"], 4326);

    // Verify data was fetched correctly
    assert_eq!(
        reader.metadata().file_metadata().num_rows(),
        3,
        "Should have 3 parcels"
    );
}

/// Test spatial SQL functions work via geodatafusion integration
#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_sql_functions() {
    use runtimedb::RuntimeEngine;

    let temp_dir = TempDir::new().unwrap();
    let engine = RuntimeEngine::defaults(temp_dir.path())
        .await
        .expect("Engine should initialize");

    // Test st_area function is registered
    let result = engine
        .execute_query("SELECT st_area(st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))")
        .await;

    assert!(
        result.is_ok(),
        "st_area query should succeed: {:?}",
        result.err()
    );

    let response = result.unwrap();
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].num_rows(), 1);

    // Test st_distance function
    let result = engine
        .execute_query(
            "SELECT st_distance(
                st_geomfromtext('POINT(0 0)'),
                st_geomfromtext('POINT(3 4)')
            )",
        )
        .await;

    assert!(
        result.is_ok(),
        "st_distance query should succeed: {:?}",
        result.err()
    );

    let response = result.unwrap();
    assert_eq!(response.results.len(), 1);

    // Test st_centroid function
    let result = engine
        .execute_query("SELECT st_centroid(st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'))")
        .await;

    assert!(
        result.is_ok(),
        "st_centroid query should succeed: {:?}",
        result.err()
    );
}

/// Generate a random base64-encoded 32-byte key for test secret manager.
fn generate_test_secret_key() -> String {
    use base64::Engine;
    let key_bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(key_bytes)
}

/// Example queries from the issue - demonstrating GIS query capabilities
/// These require a full end-to-end setup with data loaded
#[tokio::test(flavor = "multi_thread")]
async fn test_example_gis_queries() {
    use runtimedb::RuntimeEngine;

    let temp_dir = TempDir::new().unwrap();
    let container = start_postgis_container().await;
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let conn_str = format!(
        "postgres://postgres:{}@localhost:{}/postgres",
        TEST_PASSWORD, port
    );
    let pool = wait_for_db(&conn_str).await;

    // Enable PostGIS
    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(&pool)
        .await
        .unwrap();

    // Create schema
    sqlx::query("CREATE SCHEMA locations")
        .execute(&pool)
        .await
        .unwrap();

    // Create addresses table
    sqlx::query(
        "CREATE TABLE locations.addresses (
            id SERIAL PRIMARY KEY,
            house_num VARCHAR(20),
            street_name VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(2),
            zipcode VARCHAR(10),
            point GEOMETRY(Point, 4326)
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Create addresses_boundaries join table
    sqlx::query(
        "CREATE TABLE locations.addresses_boundaries (
            address_id INTEGER REFERENCES locations.addresses(id),
            boundary_type VARCHAR(50)
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Create parcels table
    sqlx::query(
        "CREATE TABLE locations.parcels (
            id SERIAL PRIMARY KEY,
            the_geom GEOMETRY(Polygon, 4326)
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test addresses
    sqlx::query(
        "INSERT INTO locations.addresses (house_num, street_name, city, state, zipcode, point) VALUES
            ('123', 'Main St', 'Springfield', 'IL', '62701', ST_GeomFromText('POINT(-89.65 39.80)', 4326)),
            ('456', 'Oak Ave', 'Springfield', 'IL', '62702', ST_GeomFromText('POINT(-89.64 39.81)', 4326)),
            ('789', 'Pine Rd', 'Springfield', 'IL', '62703', ST_GeomFromText('POINT(-89.63 39.82)', 4326)),
            ('101', 'Elm St', 'Springfield', 'IL', '62704', ST_GeomFromText('POINT(-89.62 39.83)', 4326)),
            ('202', 'Maple Ln', 'Springfield', 'IL', '62705', ST_GeomFromText('POINT(-89.61 39.84)', 4326))",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert address boundaries
    sqlx::query(
        "INSERT INTO locations.addresses_boundaries (address_id, boundary_type) VALUES
            (1, 'residential'), (2, 'commercial'), (3, 'residential'),
            (4, 'industrial'), (5, 'residential')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert parcels of varying sizes
    sqlx::query(
        "INSERT INTO locations.parcels (the_geom) VALUES
            (ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 4326)),
            (ST_GeomFromText('POLYGON((2 2, 2 5, 5 5, 5 2, 2 2))', 4326)),
            (ST_GeomFromText('POLYGON((10 10, 10 12, 12 12, 12 10, 10 10))', 4326))",
    )
    .execute(&pool)
    .await
    .unwrap();

    pool.close().await;

    // Create engine with secret key enabled
    let secret_key = generate_test_secret_key();
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(secret_key)
        .build()
        .await
        .expect("Engine should initialize");

    // Store password in engine's secret manager
    let secret_id = engine
        .secret_manager()
        .create("pg-pass", TEST_PASSWORD.as_bytes())
        .await
        .expect("Failed to store secret");

    // Register connection with secret reference
    let source = Source::Postgres {
        host: "localhost".to_string(),
        port,
        user: "postgres".to_string(),
        database: "postgres".to_string(),
        credential: Credential::secret_ref(&secret_id),
    };

    engine
        .connect("test_conn", source)
        .await
        .expect("Should connect");

    // Example Query 1: The addresses and location of 5 locations
    let result = engine
        .execute_query(
            "SELECT a.id, a.house_num, a.street_name, a.city, a.state, a.zipcode, a.point
             FROM test_conn.locations.addresses a
             JOIN test_conn.locations.addresses_boundaries ab ON a.id = ab.address_id
             LIMIT 5",
        )
        .await;

    assert!(
        result.is_ok(),
        "Query 1 (addresses with boundaries) should succeed: {:?}",
        result.err()
    );
    let response = result.unwrap();
    assert!(
        response.results[0].num_rows() <= 5,
        "Should return at most 5 rows"
    );

    // Example Query 2: The largest parcel (using st_area)
    let result = engine
        .execute_query(
            "SELECT id, the_geom
             FROM test_conn.locations.parcels
             ORDER BY st_area(the_geom) DESC
             LIMIT 1",
        )
        .await;

    assert!(
        result.is_ok(),
        "Query 2 (largest parcel) should succeed: {:?}",
        result.err()
    );
    let response = result.unwrap();
    assert_eq!(
        response.results[0].num_rows(),
        1,
        "Should return 1 largest parcel"
    );

    // Example Query 3: The distance between two largest parcels
    let result = engine
        .execute_query(
            "WITH largest_parcels AS (
                SELECT id, the_geom
                FROM test_conn.locations.parcels
                ORDER BY st_area(the_geom) DESC
                LIMIT 2
            )
            SELECT st_distance(
                st_centroid((SELECT the_geom FROM largest_parcels LIMIT 1)),
                st_centroid((SELECT the_geom FROM largest_parcels OFFSET 1 LIMIT 1))
            ) AS distance_between_largest_parcels",
        )
        .await;

    assert!(
        result.is_ok(),
        "Query 3 (distance between parcels) should succeed: {:?}",
        result.err()
    );

    // Example Query 4: Convex hull around the largest parcel
    let result = engine
        .execute_query(
            "SELECT id, st_convexhull(the_geom) AS hull
             FROM test_conn.locations.parcels
             ORDER BY st_area(the_geom) DESC
             LIMIT 1",
        )
        .await;

    assert!(
        result.is_ok(),
        "Query 4 (convex hull) should succeed: {:?}",
        result.err()
    );
}
